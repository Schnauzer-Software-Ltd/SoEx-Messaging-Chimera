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
A message that ends the second way is reported as lost on a later read, so it is named somewhere rather than
only being implied by a return value you might not have looked at.

One subscriber reading several topics is one `SqliteSubscription` per topic, all sharing that subscriber's
file and its set-aside store. Open them freely: they do not disturb each other's deliveries. Do not open two
for the same subscriber and topic, though nothing stops you — that is two writers on one file, and the rule
that there is only ever one is what removes the need for a lease or a visibility timeout anywhere here. It
holds across processes as well as inside one: a publisher here and its subscribers over there is a supported
shape, tested that way, because those are separate files with one writer each. Two processes on the *same*
file is the thing to avoid, and what it costs when you do it is duplicate handling rather than damage —
measured at around 210 deliveries for 200 messages, none lost. Holding to the rule is yours to arrange, and
[files](files.md#one-writer-any-number-of-readers) says why it is not arranged for you. A
root written two ways is one root: paths are resolved before they are compared, so `/var/events` and
`/var/./events` are the same files to every part of this. Symbolic links are not resolved.

Disposing a subscription that is still holding deliveries hands them back rather than stranding them. They go
out again on the next read, counted as further attempts, which is what the process stopping would have done
to them — so an overlapping redeploy costs a redelivery rather than a message nobody can reach. That covers
everything the subscription had at the moment you closed it, including a delivery it was in the middle of
claiming and one whose settle was in the middle of failing: the close seals the subscription against new
work, waits for what is already moving to come to rest, and only then hands back what it finds.

You can close a subscription whose read loop is still running, and you do not have to cancel the loop first.
The loop ends with the same closed-store answer as any other call, and it will not be handed a delivery on
the way out: a message claimed as the close began is handed back, not given to a handler who could no longer
settle it. A read waiting for an in-flight slot, or pausing between polls, is woken and told the same thing
rather than left waiting — which matters because `ReadAsync` accepts a token you never cancel.

Using a subscription after it has closed is refused in its own terms rather than as a disposal error: a read
says the store is closed, and a settle says the delivery is not this caller's to settle, naming the hand-back
among the three things that can mean. Not that the message has already been settled: nothing was written, and
it is there for the next subscription to be handed.

Settle each delivery once. A second settle of the same one throws `InvalidOperationException`, and it is the
*delivery* that is finished with rather than the message: if a failed message has since been handed out again,
settling the old delivery is still refused and the new one is still yours to settle. Service Bus draws the
same line with its lock token.

`ScheduleAsync` refuses rather than stores when the topic is already holding as many not-yet-due scheduled
messages as its retention policy allows, throwing `ScheduleRefusedException`. Messages whose due time has
passed do not hold the quota: they are on their way out, and what removes them is `MaxAge`. That includes a
message scheduled for now — its due time is stored as now, so it never holds the quota at all — which makes
the count a bound on future-dated work and `MaxAge` the bound on the rest; [limits](../explanation/limits.md)
has the sizing arithmetic. `DeadLetterAsync` and `FailAsync` refuse in one narrow case too: a subscriber set
to drop the oldest, with a dead-letter byte bound smaller than the message, cannot keep the row it is being
asked to file — the message is over that bound on its own, so the trim destroys it whatever room the set has,
and filing it would be storing the message in order to lose it — so it throws `DeadLetterTooLargeException`.
That applies wherever the message is coming from, including one that has run out of attempts and one stored
here as a deferral and replayed. A settle that threw has settled nothing, so the delivery is still yours —
complete it to lose the message knowingly, or raise the bound.

A message already filed as a dead letter is the exception, and it is re-filed rather than refused: it is
being held, so holding it costs the set nothing it was not already paying. That is how a bound lowered
across a reopen behaves too — the policy goes with the open, not with the row — and the rows it has left
oversized are removed by the first retention pass that sees them, however many that is, counted in
`DeadDropped`, without taking the dead letters that still fit with them. The one that goes alone is the row a
settle has just filed: the trim spares what it is writing, so an oversized one survives its own pass and dies
on the next.

Every settle is one durable write. Nothing runs after it that could fail, which is what makes "a settle that
threw has settled nothing" a rule rather than a hope: a write already queued cannot be recalled, so cancelling
a settle either takes effect before it is queued or does not take effect at all. Closing the subscription
underneath a settle is held to the same rule. Closing drains the writes already queued rather than discarding
them, so a settle that was in progress when the close began commits, and returns to say so.

`DeferredAsync` and `DeadLettersAsync` list what is set aside. `ReplayAsync(asideId)` makes one deliverable
again from its own stored copy, so it works even after the log has dropped the original. `DiscardAsync`
destroys it, which is the only deliberate loss in this library. Both leave a message that is being handled
right now alone and answer false: a row cannot be delivered to two handlers at once, and destroying one under
a handler would make everything it is told afterwards untrue. Wait for the delivery to settle and ask again.

`ReplayAsync` answering true means the row is marked for delivery, not that the delivery will happen. Nothing
has read it yet, and under `DropOldest` a dead-letter bound reached before it is read can still take it —
`ReplayAsync` is no more able to promise that than `AppendAsync` can promise retention will not reach a record
before somebody reads it. Only a message actually in a handler's hands is safe from that bound. Under
`StopDelivering`, which is the default, nothing is destroyed and the question does not arise.

`LagAsync()` reports how far behind this subscriber is: a count of log records it has not reached, the age of
the oldest of them, and a count of scheduled messages that have come due and it has not finished with. The
scheduled count is separate because the two tables are held by different bounds, and it is there at all
because `MaxAge` removes from both — a subscriber can be overtaken on either side. It is safe to call while
the subscription is reading, which is what it is for.

`ApplyRetentionAsync()` ages out deferred messages and enforces the dead-letter bounds. Ageing one out
changes what it is filed as and nothing else, so it does not itself cancel a replay: the row stays on its way
to you and the next read delivers it, as a dead letter, which is what replaying one has always meant. Under
`DropOldest` the same pass then enforces the bound, and that can destroy the row it has just aged out, or one
you replayed earlier — counted, in the same report. The pass leaves a
message a handler is holding alone entirely, because it has not been sitting anywhere; its age is measured
again on the next pass, from wherever its settle leaves it. Its report also
carries any dead letters destroyed since the last pass, wherever they were destroyed — a settle that fills
the set under `DropOldest` causes real loss too, and it would be misleading to put a number next to one path
and a silence next to the other. That count lives in the subscriber's file rather than in memory, so a
process that closes or dies before running a pass does not take it with it.

## Settings worth knowing

`ChimeraOptions.Durability` — `ProcessCrash` by default, which survives a crash but not a power cut.
`PowerLoss` costs an fsync per commit.

`ChimeraOptions.MaxAppendBatch` — how many queued operations one transaction may commit together, 128 by
default. Concurrent appends cost one commit between them rather than one each, which is most of the
throughput here. It must be at least one and is refused below that rather than rounded up to it.

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
buy another crop of deliveries, and neither does a subscription that was already open when the set filled: the
size is read from the file on every poll rather than remembered from the first one. Replaying or discarding
the dead letters is what clears it. What counts
towards the bound is the dead letters parked in the set: replaying one takes it out of the count so that a
stopped subscriber can still work through what stopped it, and a replay that fails its way back in puts it
back. Deferring a replayed dead letter files it as deferred, which takes it out of the dead set too. Under
`DropOldest` the oldest are destroyed instead, and a dead letter being handled right now is never one of
them; it still counts towards the bound, so the set can sit briefly above it until that delivery settles.
Neither is the message the settle is filing at that moment: whatever a bound destroys to make room for it, it
is never the message you were told had been filed. A replayed message settled back into a full set counts as
the newest arrival, not as the row it was when it first arrived.

Every one of these values is checked when the subscription is opened, as retention's are when the log is
opened. A negative or zero bound fails silently and destructively otherwise — a retention age below zero puts
the cutoff in the future and removes the whole log — so `null` is how you say a bound is not wanted. Zero is
allowed for `MaxDeferredAge` alone, where it means "age this out the next time a pass looks" and files the
message dead rather than destroying it. A retention `MaxBytes` below the topic's `MaxPayloadBytes` is refused
for the same kind of reason: it describes a topic that accepts messages no retention pass can keep, and one
of them would be written and then destroyed unread by the next pass.
