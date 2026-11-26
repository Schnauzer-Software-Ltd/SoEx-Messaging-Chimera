# Testing a system built on Chimera

Chimera runs in process. A test can run a system of several connected components in one process. The test
uses real files and real durability, at the speed of a unit test. This page shows how to isolate each test, how to find
when the system is at rest, and how to control time.

## Use a directory per test

All the files of a run are in `ChimeraOptions.RootDirectory`. A new directory for each test gives each test a
clean state.

1. Create a new root directory for each test.
2. Delete the directory at the end of the test.

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

### Clean up a test root

1. Close the logs and subscriptions before you delete the directory.

   On Linux, a delete under an open handle unlinks the file and appears to succeed. On Windows, the delete
   throws. The class above catches `IOException` for this difference. The test has made its assertions before
   the cleanup, so the catch hides no real failure.

2. Delete the full directory, with its files.

   Each database has `-wal` and `-shm` sidecar files. A clean close removes them. After a process kill, they
   stay on disk.

3. Set `Pooling=false` on each connection that your test opens to a Chimera file.

   As an alternative, call `SqliteConnection.ClearAllPools()` before the delete. Chimera opens its connections
   without pooling, so a closed log or subscription releases its file. A pooled connection from your test keeps
   the file open. The cleanup then fails with no clear cause.

### Use a memory-backed filesystem for speed

1. For speed, put the root directory on a memory-backed filesystem.

   The test keeps the real durability and locking behaviour.

2. Do not use an in-memory SQLite database.

   The tests need durability. A shared-cache in-memory database also has these differences:

   - It has no write-ahead log. A writer blocks the readers.
   - Its locking reports `SQLITE_LOCKED`. `busy_timeout` does not cover that error.
   - A connection cannot be both in-memory and read-only. A subscriber then has write access to a topic file.

## Find when the system is at rest

A test of one subscription can count messages. A test of several connected components must not count the
messages between them. That number belongs to the components. If somebody adds a hop, a count assertion
fails. Assert that the system is at rest. Then assert the results.

`SoEx.Messaging.Chimera.Testing` reads the state of the system from the files. It changes nothing:

```csharp
ChimeraOverview overview = ChimeraInspector.Overview(options);

if (overview.Settled && overview.DeadLettered == 0)
    // every subscriber has caught up, nothing is being handled, nothing is waiting to go out again
```

1. Open the topic logs before the subscriptions.

   A subscription records its position at creation if the topic file exists at that time. If the file does
   not exist yet, the subscriber is absent from the overview until its first read. A root with no other
   outstanding work then reads as settled. With the logs open first, the overview shows each subscriber from
   the start.

2. Read `ChimeraInspector.Overview` until `Settled` is true twice in a row, with a gap between the two reads.

   The overview is a snapshot. The system continues to run while the inspector reads it. A handler that has
   settled its message and is about to publish the next one looks quiet for that instant. If your binding
   publishes before it settles, this window does not occur. Publish before you settle. This order is the
   at-least-once shape, and we recommend it.

3. Assert `DeadLettered == 0` as a separate check.

   `Settled` means that nothing moves on its own. Dead-lettered and deferred messages are parked by intent, so
   they do not prevent `Settled`. A system at rest can still have failures. With this check, a topology that
   gives up on all its messages fails with a count and a reason, before the timeout. `DeadLettered` counts the
   dead letters held now. The next section gives the result of this.

### If the system does not settle

`SubscriberActivity` shows each stuck subscriber and its work:

- its lag
- the scheduled work that is due
- the deliveries in flight now
- the messages that wait for a retry or a replay

For one message, use `ChimeraInspector.StatusOf`:

```csharp
MessageStatus status = ChimeraInspector.StatusOf(options, "orders", "billing", messageId);
// Pending, NotYetDue, InFlight, AwaitingRetry, Completed, Deferred or DeadLettered, with the reason
```

`StatusOf` reports `InFlight` for a replayed deferred or dead-lettered message during its handling. The
overview keeps that subscriber unsettled at the same time. A test that waits for rest and a test that asks
about one message always agree on whether work runs.

1. Query by message id.

   Do not use the sequence on an envelope. Sequences come from two different tables, and they cannot be
   compared across the tables.

2. To count the attempts of a successful message, watch the deliveries.

   Each envelope carries its attempt number. `StatusOf` does not report the attempt count of a completed
   message. The row that holds that count is deleted when the position absorbs it. This deletion keeps the
   subscriber file from growing with the log.

## What these cannot tell you: that nothing was lost

`ChimeraInspector.Overview` and `ChimeraInspector.StatusOf` read the files as they are now. When Chimera
destroys a message, it also destroys the records that show the message existed. Thus `Completed` and
`DeadLettered == 0` do not prove that nothing was lost. Each one fails in a different way:

- `Completed` means settled successfully **or destroyed by the dead-letter bound**. A dropped dead letter
  loses the same records as a completed message. After the drop, the two cases look the same.
- `Unknown` means that the topic holds no record with that id. This covers three cases: a message that was
  never published, a message that log retention removed unread, and a message that this subscriber completed
  long ago. `StatusOf` starts from the topic file and stops when the record is gone. Thus a successful message
  does not always stay `Completed`.
- `Overview.DeadLettered` counts the dead letters held now. A dead letter that the bound destroyed has left the
  set, so it has also left the count.

### Assert on the set-aside report

Chimera records each loss. The subscriber file counts each drop and each deferral that ages out, at the time it
occurs. `ApplyRetentionAsync` returns the two counts:

```csharp
SetAsideReport report = await subscription.ApplyRetentionAsync(ct);
// report.DeadDropped     — dead letters destroyed to stay inside the bound, by this pass and by any settle
// report.DeferredAgedOut — deferrals that became dead letters because they sat too long
```

1. Run the retention passes from the test harness.

   The report is read once and then cleared. The first pass that asks for it gets it. If a maintenance loop
   also runs passes, the loop can get the report before the test.

2. Assert on the report that nothing was lost.

If the test does not need the dead-letter bound, do not set `DeadLetterOverflow.DropOldest`. This setting is
opt-in. By default, a full dead-letter set stops delivery and destroys nothing.

## Control time

`ChimeraOptions.Clock` is a `TimeProvider`.

1. To fake time, use `FakeTimeProvider` from `Microsoft.Extensions.TimeProvider.Testing`.

   Do not use a hand-rolled provider that only answers `GetUtcNow`. A reader waits on this clock when it has
   nothing to deliver. A provider that does not drive timers leaves that wait on the real clock. The path that
   wakes the reader at the due time then never runs. A test on such a provider passes only because the poll
   interval fired. That test does not check what it claims to check.

The correct use of the clock depends on what the test is about.

### If the test is about your components

1. Use the real clock.
2. Set a short `PollInterval`.
3. Use the overview to find when to assert.

A fake clock gives no advantage here, and it brings the problem in the next section.

### If the test is about time

Examples are a message scheduled for later, retention, and deferred messages that age out. Use a fake clock,
and obey these rules:

1. Advance the clock only while nothing reads, between one read and the next.

   An advance fires only the timers that are already registered. Some tests run several reader loops
   continuously for their full duration. Such a test has no quiet moment. A reader that misses the tick waits
   until the next tick.

2. Pass a real-clock cancellation to every read loop.

   This cancellation is the only thing that ends a wait on a fake clock that does not advance again.

### Domain timing

Keep domain timing out of these tests. Chimera schedules messages for a due time (`ScheduleAsync`). If a
component must "do this in an hour", use a durable workflow timer. The fake clock then belongs to the workflow, above the transport.
