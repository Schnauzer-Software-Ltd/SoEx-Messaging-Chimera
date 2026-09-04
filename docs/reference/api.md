# API

## Publishing — `SqliteTopicLog`

`Open(topic, options, topicOptions)` opens the one writer for a topic. Open it once per topic per process and
share it: a second writer on the same file is the one thing the design does not allow, and the symptom is
lock contention rather than an error.

`AppendAsync(payload)` returns once the record is durable. Payloads are capped, 256 KB by default.

`ScheduleAsync(payload, dueAt)` stores a message to become deliverable later. It is never moved into the log;
subscribers read the scheduled table and pick it up themselves, which is why there is no timer per message.
Delivery is no earlier than the due time and no promise is made about how much later. A due time that has
already gone means due now, and is stored as now, which is how Service Bus reads one too.

`CancelScheduledAsync(id)` removes the row. It returns true when there was a row to remove, which is not the
same as having caught the message in time: a delivered scheduled message keeps its row until retention takes
it, so cancelling one that has already been handled removes the row and returns true. Whether it was handled
is each subscriber's own business, recorded in each subscriber's own file, and there is nothing here that
could tell you.

`ApplyRetentionAsync()` trims the log and reports what went, including which subscribers were overtaken.
Call it from wherever you run maintenance.

`InspectSubscribersAsync()` reads every subscriber's file to see where they have got to. It is a read and
only a read: truncation is never gated on it.

## Consuming — `SqliteSubscription`

`Open(subscriber, topic, options, subscriptionOptions)`. The subscriber name is a durable identity: change it
and you get a new position, not the old one. It is the same role as a subscription name on a Service Bus
topic.

Opening records the position, if this subscriber has never read this topic before. That is also where a
position belonging to a different incarnation of the log is refused, so `LogIdentityException` comes out of
`Open` rather than out of the first read. A topic with no file yet is not an error: the subscription waits,
and binds when the publisher creates it.

`ReadAsync()` yields messages until cancelled. Order is not guaranteed. Every message must be settled:

| To say | Call | What happens |
|---|---|---|
| handled | `CompleteAsync` | the position absorbs it once everything before it is settled |
| it threw | `FailAsync` | straight back for another attempt, or a dead letter once attempts run out |
| not now | `DeferAsync` | out of the flow into your own store, until you ask for it |
| never | `DeadLetterAsync` | filed, with its payload, so it stays replayable |

Settle each delivery once. A second call for the same message throws rather than being ignored: each settle
frees one of the in-flight slots, so settling twice would quietly raise the limit you set. A settle that
throws has settled nothing and leaves the delivery yours to settle again, so a retry is the right response to
one. `FailAsync` returns whether the message is coming back — false means it is now a dead letter, or that
retention removed the record while you were holding it, which is the one case no caller can check for itself.

One subscriber reading several topics is one `SqliteSubscription` per topic, all sharing that subscriber's
file and its set-aside store. Open them freely: they do not disturb each other's deliveries. Do not open two
for the same subscriber and topic, though nothing stops you — that is two writers on one file, and the rule
that there is only ever one is what removes the need for a lease or a visibility timeout anywhere here. A
root written two ways is one root: paths are resolved before they are compared, so `/var/events` and
`/var/./events` are the same files to every part of this. Symbolic links are not resolved.

Disposing a subscription that is still holding deliveries hands them back rather than stranding them. They go
out again on the next read, counted as further attempts, which is what the process stopping would have done
to them — so an overlapping redeploy costs a redelivery rather than a message nobody can reach.

`DeferredAsync` and `DeadLettersAsync` list what is set aside. `ReplayAsync(asideId)` makes one deliverable
again from its own stored copy, so it works even after the log has dropped the original. `DiscardAsync`
destroys it, which is the only deliberate loss in this library. Both leave a message that is being handled
right now alone and answer false: a row cannot be delivered to two handlers at once, and destroying one under
a handler would make everything it is told afterwards untrue. Wait for the delivery to settle and ask again.

`LagAsync()` reports how far behind this subscriber is, as a count and as a duration.

`ApplyRetentionAsync()` ages out deferred messages and enforces the dead-letter bounds. Its report also
carries any dead letters destroyed since the last pass, wherever they were destroyed — a settle that fills
the set under `DropOldest` causes real loss too, and it would be misleading to put a number next to one path
and a silence next to the other.

## Settings worth knowing

`ChimeraOptions.Durability` — `ProcessCrash` by default, which survives a crash but not a power cut.
`PowerLoss` costs an fsync per commit.

`ChimeraOptions.BusyTimeout` — how long a connection waits for a lock before giving up, five seconds by
default. It has to be greater than zero, and anything less is refused when a file is opened: SQLite reads
zero as "do not wait", which turns ordinary contention into failures.

`SubscriptionOptions.MaxInFlight` — deliveries outstanding at once, across every source rather than each
separately. Reading stops here until something is settled.

`SubscriptionOptions.MaxAttempts` — deliveries of one message before `FailAsync` dead-letters it. There is no
delay between attempts.

`SubscriptionOptions.Start` — where a subscriber that has never read this topic before begins. `Beginning`,
the default, delivers what the topic still holds, which is the retained window and not the whole history.
`Next` delivers only what arrives from then on, which is what a Service Bus subscription does. Either way a
message scheduled before the subscriber existed but due after it started still arrives when it falls due. The
setting applies once, when the position is first recorded; after that the stored position is what matters and
changing this does not move it.

`SetAsidePolicy` — bounds on what one subscriber may set aside. Set-aside rows carry payload copies, so
without these a consumer that defers freely fills the disk. There are four: `MaxDeferredCount` and
`MaxDeferredBytes` on what may be deferred, `MaxDeadCount` and `MaxDeadBytes` on what may be dead-lettered.
Breaching a deferred bound files the message as a dead letter rather than dropping it, and says so in the
row's reason.

The bytes are counted per set rather than across both, because the two sets can answer a bound differently. A
deferral that does not fit has somewhere to go; dead is where passing a message along stops, so the only
answers there are to stop the subscriber or to destroy something, which is what `DeadOverflow` chooses
between. A single number spanning both could only mean the first, and the dead set would grow past it one
payload at a time while the setting said it was bounded.

When a dead-letter bound is reached the subscriber stops, by default, rather than destroying anything: it
keeps its position, blocks nobody else, and starts again when the dead letters are dealt with. That is a
property of the stored set and not of the object, so closing the subscription and opening it again does not
buy another crop of deliveries — replaying or discarding the dead letters is what clears it. Under
`DropOldest` the oldest are destroyed instead, and a dead letter being handled right now is never one of
them; it still counts towards the bound, so the set can sit briefly above it until that delivery settles.

Every one of these values is checked when the subscription is opened, as retention's are when the log is
opened. A negative or zero bound fails silently and destructively otherwise — a retention age below zero puts
the cutoff in the future and removes the whole log — so `null` is how you say a bound is not wanted. Zero is
allowed for `MaxDeferredAge` alone, where it means "age this out the next time a pass looks" and files the
message dead rather than destroying it.
