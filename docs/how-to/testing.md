# Testing a system built on this

The point of an in-process event log is that a system wired together with one can be tested in a single
process, with real files and real durability, at the speed of a unit test. Two things make that pleasant, and
one of them is not obvious.

## A directory per test

Everything a run touches lives under `ChimeraOptions.RootDirectory`, so a fresh directory per test is a fresh
world. Delete it at the end.

```csharp
public sealed class ChimeraTestRoot : IDisposable
{
    public ChimeraTestRoot()
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "chimera", Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(Path);
        Options = new ChimeraOptions { RootDirectory = Path };
    }

    public string Path { get; }
    public ChimeraOptions Options { get; }

    public void Dispose()
    {
        try { Directory.Delete(Path, recursive: true); }
        catch (IOException) { /* a stray handle should not fail a test that already passed */ }
    }
}
```

Four things about that cleanup are worth knowing rather than discovering.

Close the logs and subscriptions before deleting. On Linux a delete under an open handle unlinks the file and
looks like it worked; on Windows it throws. Swallowing `IOException` covers the difference without hiding a
real failure, since the test has already made its assertions by then.

Delete the directory rather than the files. Each database has `-wal` and `-shm` sidecars. They go on a clean
close, but not if the process was killed.

Nothing here pools connections, so a closed log or subscription has really let go of its file. If your test
opens its own connection to look inside one, set `Pooling=false` on it or call
`SqliteConnection.ClearAllPools()` before deleting, or cleanup fails for reasons that look like nothing.

An in-memory database is not an option, and not only because durability is the point. A shared-cache
in-memory database has no write-ahead log, so a writer blocks readers instead of running alongside them; its
locking reports `SQLITE_LOCKED`, which `busy_timeout` does not cover; and a connection cannot be both
in-memory and read-only, so the guarantee that a subscriber physically cannot write a topic file disappears.
If you want the speed, put the root on a memory-backed filesystem and keep the real semantics.

## Knowing when the system has come to rest

A test over one subscription can count messages. A test over several components wired together cannot: how
many messages flow between them is their business, and asserting on it makes the test fail whenever somebody
adds a hop. What such a test can assert is that the system came to rest, and then that the right things
happened.

`SoEx.Messaging.Chimera.Testing` reads that from the files, without disturbing anything:

```csharp
ChimeraOverview overview = ChimeraInspector.Overview(options);

if (overview.Settled && overview.DeadLettered == 0)
    // every subscriber has caught up, nothing is being handled, nothing is waiting to go out again
```

`Settled` means nothing will move on its own. Dead-lettered and deferred messages do not count against it,
deliberately: both were parked on purpose, so a system holding them is at rest. It is not necessarily
healthy, which is a different question — assert `DeadLettered == 0` separately, and a topology that gives up
on everything fails with a count and a reason instead of waiting for a timeout.

It is a snapshot taken while everything carries on running, not a barrier. A handler that has settled its
message and is about to publish what comes next looks quiet for that instant, so wait for quiet twice in a
row with a gap between rather than once. That window closes entirely if your binding publishes before it
settles, which is the at-least-once shape and worth having anyway.

One thing to arrange in the harness: a subscription records its position when it is created, but only if the
topic file already exists. Open your logs before your subscriptions and every subscriber is visible from the
start. Otherwise a subscriber that has not yet reached its first read is not in the answer at all, and a root
with nothing else outstanding reads as settled.

When it does not settle, `SubscriberActivity` says who is stuck on what — how far behind, how much scheduled
work is due, what is in flight right now, what is waiting to be retried or replayed. And for one message:

```csharp
MessageStatus status = ChimeraInspector.StatusOf(options, "orders", "billing", messageId);
// Pending, NotYetDue, InFlight, AwaitingRetry, Completed, Deferred or DeadLettered, with the reason
```

A deferred or dead-lettered message that has been replayed and is being handled right now reads as
`InFlight`, not as parked. It agrees with the overview, which keeps such a subscriber unsettled: a test that
waits for quiet and a test that asks about one message should never disagree about whether work is running.

Ask by message id, not by the sequence on an envelope: sequences come from two different tables and only look
orderable. One thing it cannot tell you is how many attempts a message that succeeded took. The row carrying
that count is deleted when the position absorbs it, which is what keeps a subscriber's file from growing with
the log. If a test needs that, watch the deliveries — every envelope carries its own attempt number.

## Controlling time

`ChimeraOptions.Clock` is a `TimeProvider`, and a fake one has to be a real fake: use
`Microsoft.Extensions.TimeProvider.Testing`'s `FakeTimeProvider`, never a hand-rolled provider that only
answers `GetUtcNow`. A reader waits on this clock when it has nothing to deliver, so a provider that does not
drive timers leaves that wait on the real clock and the wake-at-the-due-time path never runs. A test written
against one of those passes because the poll interval happened to fire, which is not what it says it checked.

That same fact decides how to use it, and the answer differs by what you are testing.

**If the test is about your components rather than about time**, leave this alone. Use the real clock and a
short `PollInterval`, and settle the question of when to assert with the overview above. Faking time here
buys nothing and costs the problem below.

**If the test is about time** — a message scheduled for later, retention, deferred messages ageing out — then
a fake clock is right, and one rule comes with it. Advancing only fires timers that have already been
registered, so advance while nothing is reading, between one read and the next. In a test where several
reader loops run continuously for its whole duration there is no such moment, and a reader that misses the
tick waits until the next one. Pass a real-clock cancellation to every read loop regardless: it is the only
thing that ends a wait on a fake clock that is not going to advance again.

Domain timing is a different matter and does not belong here at all. This library owns no scheduler on
purpose. If a component says "do this in an hour", that is a durable workflow's timer, and the fake clock
belongs to the workflow rather than to the transport underneath it.
