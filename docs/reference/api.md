# API

## `SqliteTopicLog`

`SqliteTopicLog` is the topic log: the writer for one topic.

### `Open(topic, options, topicOptions)`

Opens the writer for a topic. Share one writer per topic per process.

Do not open a second writer on the same topic file. The design does not allow it. A second topic writer
causes lock contention. No error occurs.

### `AppendAsync(payload)`

Appends the payload as a record. Returns when the record is durable. The default payload limit is 256 KB.

### `ScheduleAsync(payload, dueAt)`

Stores a scheduled message that becomes deliverable at the due time. The message stays in the scheduled table.
It never moves into the log. Each subscriber reads the scheduled table and picks up the message itself. A
scheduled message needs no timer.

Delivery occurs at the due time or later. Chimera sets no maximum delay after the due time. A due time in the
past means "due now". Chimera stores it as the current time. Service Bus reads a past due time in the same way.

The retention policy sets a maximum number of scheduled messages that are not yet due. If the topic holds that
number, `ScheduleAsync` throws `ScheduleRefusedException` and stores nothing.

Messages past their due time do not count toward this maximum. They are due for delivery, and `MaxAge` removes
them. A message scheduled for now never counts, because Chimera stores its due time as now. The count
bounds future-dated messages, and `MaxAge` bounds the rest. [Limits](../explanation/limits.md) gives the sizing
arithmetic.

### `CancelScheduledAsync(id)`

Removes the row of a scheduled message. Returns true if a row was there to remove.

A true result does not mean that the cancel came in time. A delivered scheduled message keeps its row until
retention removes it. A cancel after delivery removes the row and returns true. Each subscriber records in
its subscriber file whether it handled the message. The topic log cannot read that record.

### `ApplyRetentionAsync()`

Removes old records from the log. Returns a report of what it removed and which subscribers it overtook. Call it
from your maintenance task.

### `InspectSubscribersAsync()`

Reads every subscriber file and reports the position of each subscriber. It only reads. Truncation never
depends on it.

## `SqliteSubscription`

`SqliteSubscription` is one subscriber that reads one topic.

### `Open(subscriber, topic, options, subscriptionOptions)`

Opens a subscription. The subscriber name is a durable identity. A new name gives a new position. A
subscription name on a Service Bus topic has the same role.

If this subscriber has never read this topic, `Open` records its position. `Open` also refuses a stored
position that belongs to a different incarnation of the log. `LogIdentityException` comes from `Open`,
before the first read.

If the topic has no file yet, `Open` succeeds and the subscription waits. When the publisher creates the file,
the subscription binds to it.

### One writer per subscriber file

One subscriber can read several topics. Open one `SqliteSubscription` per topic. These subscriptions share the
subscriber file and its set-aside store. You can open as many as you need. One does not disturb the deliveries
of another.

Do not open two subscriptions for the same subscriber and topic. The library does not prevent it. Two
subscriptions are two writers on one file. Because each file has one writer, Chimera needs no lease and no
visibility timeout.

The rule applies across processes and in one process. A publisher in one process with its subscribers in other
processes is a supported layout. Tests use this layout. Each of these files has one writer.

If two processes write the same subscriber file, some messages are handled twice. The file is not damaged. In a
measurement, 200 messages gave about 210 deliveries, and none were lost.

You must enforce the one-writer rule. [Files](files.md#writers-and-readers) explains the rule.

Chimera resolves root paths before comparison. `/var/events` and `/var/./events` are the same root,
with the same files. Chimera does not resolve symbolic links.

### `ReadAsync()`

Yields messages until cancellation. Delivery order can differ from append order. Settle every message:

| Result | Call | Effect |
|---|---|---|
| Handled | `CompleteAsync` | The position moves past the message when all earlier messages are settled. |
| The handler threw | `FailAsync` | The message comes back immediately for another attempt. After the last attempt, it becomes a dead letter. |
| Not now | `DeferAsync` | The message leaves the flow and goes into the set-aside store. It stays there until you ask for it. |
| Never | `DeadLetterAsync` | Chimera files the message as a dead letter with its payload. You can replay it. |

### Settle rules

Settle each delivery once. A second settle of the same delivery throws `InvalidOperationException`. Each settle
frees one in-flight slot. A second settle would silently raise the configured in-flight limit.

A settle finishes a delivery. If a failed message goes out again, a settle of the old delivery still throws.
The new delivery is yours to settle. Service Bus uses its lock token in the same way.

A settle that throws has settled nothing. The delivery stays yours to settle again. Retry the settle.

`FailAsync` returns true if the message comes back for another attempt. It returns false in two cases:

- The message is now a dead letter.
- Retention removed the record while you held the delivery. The caller cannot check for this case.

In the second case, a later read reports the message as lost. The loss is reported even if you do not
read the return value.

Every settle is one durable write. Nothing that can fail runs after the write. A settle that throws has
settled nothing.

A queued write cannot be recalled. If you cancel a settle before Chimera queues its write, the cancel takes
effect. After that point, the cancel has no effect.

The same rule applies if you close the subscription during a settle. The close drains the queued writes. A
settle in progress when the close starts commits, and returns to report the commit.

### Dead letters larger than the bound

`DeadLetterAsync` and `FailAsync` throw `DeadLetterTooLargeException` in one case. The subscriber uses
`DropOldest`, and the message is larger than the dead-letter byte bound.

The trim would destroy such a message, whatever room the set has. Chimera refuses to store a message only to
lose it. The rule applies to every source of the message. This includes a message with no attempts left, and a replayed deferred message.

A settle that throws has settled nothing, so the delivery is still yours. Do one of these:

- Complete the message to lose it knowingly.
- Raise the bound.

A message that is already a dead letter is the exception. Chimera files it again and does not refuse it. The
set already holds it, so it adds no cost to the set.

A bound lowered across a reopen works in the same way. The policy comes with each open, and does not stay
with the row. The first retention pass that sees oversized rows removes all of them and counts them in
`DeadDropped`. Dead letters that still fit stay in the set.

The row that the current settle files is the one exception. The trim spares the new row. An
oversized row survives its own pass. The next pass removes it.

### Close a subscription

If you dispose a subscription that holds deliveries, it hands them back. The next read delivers them again.
Each counts as one more attempt. A process stop has the same effect. An overlapping redeploy causes a
redelivery, and every message stays reachable.

The hand-back covers all deliveries that the subscription holds at the close. This includes a delivery in the
middle of its claim, and a delivery whose settle fails during the close. The close has three steps:

1. It seals the subscription against new work.
2. It waits for all operations in progress to stop.
3. It hands back every outstanding delivery.

You can close a subscription while its read loop runs. You do not have to cancel the loop first. The loop ends
with the same closed-store answer as any other call. The loop gets no delivery as it ends. The close hands
back a message claimed as the close started. No handler that cannot settle gets the message.

A read that waits for an in-flight slot, or pauses between polls, wakes up and gets the same answer. `ReadAsync` also ends when its token is never cancelled.

After a close, each call fails with an error about the close. Chimera does not throw a disposal error:

- A read reports that the store is closed.
- A settle reports that the delivery is not this caller's to settle. The message names the hand-back as one of
  three possible causes.

This error does not mean that the message is settled. Chimera wrote nothing. The next subscription gets the
message.

### `DeferredAsync`, `DeadLettersAsync`, `ReplayAsync`, `DiscardAsync`

`DeferredAsync` and `DeadLettersAsync` list the set-aside messages.

`ReplayAsync(asideId)` makes one set-aside message deliverable again from its stored copy. Replay works after
the log has removed the original.

`DiscardAsync` destroys a set-aside message. It is the only deliberate loss in this library.

If a handler holds the message now, `ReplayAsync` and `DiscardAsync` leave it and return false. A row cannot go
to two handlers at the same time. A row destroyed under a handler would make every later result to that handler untrue. Wait for the delivery to settle. Then call again.

If `ReplayAsync` returns true, the row is marked for delivery. The delivery is not certain. No subscriber has
read the row yet. Under `DropOldest`, the dead-letter bound can remove the row before a read.
`AppendAsync` has the same limit: retention can remove a record before a read.

Only a message in the hands of a handler is safe from that bound. Under `StopDelivering`, the default, Chimera
destroys nothing.

### `LagAsync()`

Reports the lag of this subscriber as three values:

- The count of log records that the subscriber has not reached.
- The age of the oldest of those records.
- The count of due scheduled messages that the subscriber has not finished.

The scheduled count is separate because different bounds control the two tables. `MaxAge` removes from both
tables, so retention can overtake a subscriber in either table.

`LagAsync` is for use while the subscription reads. It is safe to call at that time.

### `ApplyRetentionAsync()` on a subscription

Ages out deferred messages and enforces the dead-letter bounds.

Ageing out a deferred message files it as a dead letter. Nothing else changes. Ageing out does not cancel
a replay. The row stays on its way to you, and the next read delivers it as a dead letter. A replay of a dead
letter always works in this way.

Under `DropOldest`, the same pass then enforces the bound. It can destroy a row aged out in this pass, or an earlier replayed row. The report counts each destroyed row.

The pass skips in-flight messages, which are not set aside. A later pass measures the age of such a message again, from where its settle files it.

The report also counts the dead letters destroyed since the last pass, from any cause. A settle that fills the
set under `DropOldest` also causes real loss. The report counts the losses on both paths.

Chimera keeps this count in the subscriber file. The count stays if the process closes or stops before a
pass.

## Settings

### `ChimeraOptions`

`Durability` sets the durability level. The default, `ProcessCrash`, survives a process crash. It does not
survive a power cut. `PowerLoss` adds an fsync per commit.

`MaxAppendBatch` is the maximum number of queued operations that one transaction commits together. The default
is 128. Concurrent appends share one commit. This gives most of the throughput. The value must be one or more.
Chimera refuses a lower value and does not round it up.

`BusyTimeout` is the maximum wait of a connection for a lock. The default is five seconds. The
value must be more than zero. When a file opens, Chimera refuses a lower value. SQLite reads zero as "do not
wait". With zero, normal contention causes failures.

### `SubscriptionOptions`

`MaxInFlight` is the maximum number of deliveries outstanding at one time. The default is 16. The limit applies
to all sources together. When the limit is reached, reading stops until a delivery is settled.

`MaxAttempts` is the number of deliveries of one message before `FailAsync` dead-letters it. The default is 3.
The attempts follow each other with no delay.

`Start` sets where a subscriber that has never read this topic starts:

- `Beginning`, the default, delivers what the topic still holds. This is the retained window, which can be
  shorter than the whole history.
- `Next` delivers only the messages that arrive after the open. A Service Bus subscription works in this way.

With either value, a message scheduled before the subscriber existed arrives at its due time, if that due time is after the subscriber's start.

The setting applies once, when Chimera first records the position. After that, the stored position controls
the start. A change to `Start` does not move it.

### `SetAsidePolicy`

`SetAsidePolicy` sets the bounds on what one subscriber can set aside. Set-aside rows hold copies of payloads.
The bounds stop a subscriber that defers freely from filling the disk. There are four bounds:

| Bound | Limits |
|---|---|
| `MaxDeferredCount` | The number of deferred messages. |
| `MaxDeferredBytes` | The bytes of deferred messages. |
| `MaxDeadCount` | The number of dead letters. |
| `MaxDeadBytes` | The bytes of dead letters. |

If a message exceeds a deferred bound, Chimera files it as a dead letter. Chimera does not drop it. The reason
field of the row records the cause.

Chimera counts the bytes of each set separately, because the two sets respond to a bound in different ways. A
deferral that does not fit goes to the dead set. The dead set is the end of the path. At a dead-letter bound,
the choices are to stop the subscriber or to destroy messages. `DeadOverflow` selects one of the two.

### When a dead-letter bound is reached

By default, the subscriber stops when it reaches a dead-letter bound. It destroys nothing. It keeps its
position and blocks no other subscriber. It starts again when you replay or discard the dead letters.

The stop is a property of the stored set. Each poll reads the size of the set from the file. A close and
reopen of the subscription gives no more deliveries. A subscription that was already open when the set filled
also stops.

The bound counts the dead letters in the set:

- A replayed dead letter leaves the count. A stopped subscriber can work through the dead letters that
  stopped it.
- If a replay fails back into the set, the message counts again.
- If you defer a replayed dead letter, Chimera files it as deferred. This also removes it from the dead set.

Under `DropOldest`, Chimera destroys the oldest dead letters. It never destroys a dead letter that a handler
holds. That message still counts toward the bound. The set can briefly exceed the bound until that
delivery settles.

The bound also never destroys the message that the current settle files. The message that Chimera reports as
filed is never one that the bound destroys.

A replayed message that settles back into a full set counts as the newest arrival. Its first arrival time does
not apply.

### Validation

Chimera checks all these values when a subscription opens. It checks the retention values when the log opens.
A zero or negative bound would otherwise fail silently and destroy data.

For example, a negative retention age puts the cutoff in the future. The retention pass then removes the
whole log. To turn off a bound, set it to `null`.

`MaxDeferredAge` alone accepts zero. Zero means "age this out at the next pass". The pass files the message as
a dead letter and does not destroy it.

Chimera also refuses a retention `MaxBytes` below the `MaxPayloadBytes` of the topic. Such a topic accepts
messages that no retention pass can keep. The next pass would destroy such a message before any read.
