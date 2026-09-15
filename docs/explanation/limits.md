# Limits, and how you find out

This library works while you have one machine and one writer per file, and the publisher and its subscribers
stop and start together. Everything below follows from that, and nothing below is hidden.

One writer per file is the constraint, not one process. A publisher in one process and its subscribers in
another is a supported shape — separate files, one writer each — and there are tests that run it that way:
a publisher writing five hundred messages from another process, subscribers in other processes reading them,
and two subscribers in two processes both getting everything. What is not supported is two writers on one
file, and that is a rule rather than something the library refuses. Measured, breaking it costs duplicate
handling and not corruption: two processes on one subscriber file handled 200 messages in about 210
deliveries, with none lost and the file readable afterwards. At-least-once already allows that, which is why
it is a rule you can break and not notice.

## Messages can be removed before they are read

The log is trimmed on retention — an age, a size, a count — and never on what subscribers have got through.
That is a decision rather than an omission. Waiting for the slowest subscriber turns a bounded loss into
unbounded growth, and the growth lands on the publisher, so one stuck reader stops every write. It also needs
a way to forget subscribers that will never come back, which is another timer quietly making the same
decision somewhere less visible. Apache Pulsar retains until every subscription has acknowledged and
consequently ships a quota whose options are to block the publisher or to drop the backlog anyway. There is
no third answer to find.

So the answer here is to make the loss bounded, attributable, and loud.

**Before it happens.** Ask a subscriber how far behind it is. `LagAsync` reports a duration as well as a
count, because a number of messages cannot be compared with a policy written in hours. It counts the
scheduled table as well as the log, since the age bound removes from both. Alarm when the lag approaches the
retention window.

**As it happens.** `ApplyRetentionAsync` reads the subscribers' files and names the ones that had not reached
what it removed, and how many each lost. Log that at warning. It counts from each subscriber's position, so a
subscriber holding a delivery it goes on to complete is named here for a message that was never lost to it —
this number is what the log can see from outside, and the subscriber's own report below is the one that knows
how each delivery ended.

**Afterwards.** The subscriber itself throws `MessagesLostException` on its next read, carrying the count and
the range of sequences. Two things are counted in it: records this subscriber never read, and deliveries it
failed. A failure is a promise that the message will come again, and only the log can keep that promise, so a
record removed while a redelivery is owed against it is as lost as one nobody ever saw. A delivery a handler
still has in its hands is neither, and is not counted while that is true: what became of it is not known yet.
It is counted when the handler is finished with it and the answer is a failure — on a later read, once the
position has already moved — and it is `false` from `FailAsync` at the time, which is that call saying it
cannot keep the promise it would otherwise be making. Between them, every record either reaches a handler that
deals with it or turns up in exactly one of these reports. It then moves its position over the gap and carries on, because a position stuck
below records that will never exist again would never move again. It comes out of the read loop rather than
out of a handler, so the loop has to be written to catch it and start reading again;
[integrating](../how-to/integrating.md) shows the shape. It is reported on the next poll whether or not the
topic is still busy, including when a pass removed everything and left the log empty — the sequence counter
is durable, so an empty log still knows how far it once got. A topic going quiet is exactly when an operator
asks whether everything was drained, and it is no time to start answering yes by default.

The retention size and the payload limit are checked against each other when a topic is opened. A `MaxBytes`
below `MaxPayloadBytes` describes a topic that will accept a message no retention pass can then keep: it is
written, and removed by the next pass as the record that put the log over its size, possibly before anyone
read it. Since nothing can make such a message survive, the configuration is refused rather than the messages.

The same is true on the scheduled side, but only that far: a scheduled delivery that failed is reported when
its row goes, because the subscriber holds its own record of the attempt, while a scheduled message nobody
ever read is not. Positions there are due times rather than sequences, and a due-time position cannot tell a
row that was removed from one that was never published — so there is no gap to notice and nothing honest to
say. Bound the scheduled table where the messages are created, and watch `RetentionReport.ScheduledRemoved`.

Only a subscriber that was there is told. One created later takes its position from the oldest record still
in the log, so records removed before it existed are not reported to it as loss, because they were never its
to read. That is what both of the places this is meant to be swapped for do: a Service Bus subscription
receives nothing sent before it was created, and a JetStream consumer replays the retained window without
complaining about what has aged out of it. Which of those two a new subscriber gets is
`SubscriptionOptions.Start`, and it applies once, when the position is first recorded.

## The other way a message can be destroyed: a full dead-letter set

By default a subscriber that fills its dead-letter set stops delivering until somebody deals with it, and
nothing is destroyed. `DeadLetterOverflow.DropOldest` chooses the other answer, and it is the only setting in
this library that destroys a message to make room. It is not the default for that reason.

What it destroys, it chooses newest-first: the set keeps its newest dead letters and drops what they have
pushed out. A message larger than the whole byte bound is charged to nothing on the way through that sum — it
is over the bound on its own, so it is the row that goes, and the dead letters older than it are not
collateral. That case is refused at the settle wherever a caller is there to refuse to. It still arrives by
two routes with no caller: a deferral that ages out into the dead set, and a bound lowered under rows already
filed, since `SetAsidePolicy` goes with the subscription you open and not with the rows in the file.

What it spares is exactly one thing: a message a handler has in its hands. That handler is about to be told
what happened to it, and telling it about a row that no longer exists is the sort of untruth the rest of this
design goes to some length to avoid. Everything else in the set is eligible, and that includes a message you
have replayed and not yet been given.

Worth stating plainly, because it looks like a hole. `ReplayAsync` answering true means the row was marked
for delivery; it is a statement about the store as it stood, not a promise that the delivery happens. Nothing
has read the row yet. A bound reached in the meantime takes the oldest, and if that is the row you replayed,
that is the row that goes — the same relationship an append has with retention, which can remove a record
before anybody reads it.

We could spare replayed rows too, and chose not to. In-flight deliveries are capped by `MaxInFlight`, so
exempting them costs a known and small amount of slack. There is no cap on how many rows can be made ready at
once, so exempting those would let a caller who replayed a whole set switch the bound off. A bound that can be
suspended is a worse thing to have than loss that is counted, and counted is what this is: every destroyed
dead letter lands in `SetAsideReport.DeadDropped`, which is durable, survives the process, and is reported by
whichever pass asks next. If you need the guarantee rather than the count, `StopDelivering` is the setting
that gives it.

## Scheduled messages stay in the file after they are delivered

A scheduled message is not moved into the log and is not removed when somebody consumes it. Subscribers read
the topic file and cannot write to it, and there may be another subscriber that has not taken the message
yet, so only retention removes these rows. Each subscriber therefore keeps its own position in the scheduled
table, in due-time order, exactly as it keeps a position in the log.

Three consequences worth knowing. A scheduled row occupies disk until retention removes it, which is
`RetentionPolicy.MaxAge` measured from the due time, and never if you have set the policy to unbounded.

`MaxBytes` and `MaxCount` never *remove* these rows — they trim the log. That is deliberate: a scheduled
message is work somebody asked for at a time that has not arrived, and destroying it to stay under a size
bound is not trimming history, it is cancelling the future. So the same two settings are answered at the
other end instead. When the scheduled table is at the bound, `ScheduleAsync` throws
`ScheduleRefusedException` and the message is not stored. Service Bus does exactly this — scheduled messages
count against the entity quota and the send is refused — and it is the only answer that neither destroys
work nor lets the table grow without limit.

Two consequences. Ordinary appends are unaffected while schedules are being refused: the log answers those
settings by trimming, as it always did, so a full scheduled table does not stop anything being published.
And the file's ceiling is the two bounds added together rather than either one — the log held at its bound by
trimming, the scheduled table at its bound by refusing. What clears a refusal is due times arriving, or
cancelling messages nobody wants any more.

What the quota counts is the messages still waiting for a due time, not every row in the table. A delivered
scheduled row stays where it is — fan-out means no one subscriber may remove it — so counting rows would
count delivered history as pending work, and a topic doing steady scheduled traffic would be refused with
nothing whatsoever outstanding. Service Bus counts what is waiting for the same reason, having removed the
row at enqueue.

Which means the two settings bound *future-dated* work, and the rest of the table is bounded by time. Size
them with both halves in mind:

    rows in the file  ≈  MaxCount  +  (rate of due-or-past-due schedules × MaxAge)

The second term is the one that surprises people. A schedule whose due time has already arrived is stored as
due now, so it is never inside the counted set — not even at the instant it is written — and `MaxCount` does
not see it at all. Nothing is wrong with that; those rows are past-due history, the same thing the trailing
end of a stream of short-dated schedules becomes a moment after it is written. But it means a publisher that
schedules for now, or a second ahead, is bounded by `MaxAge` alone. If that traffic is heavy, bound it by
lowering `MaxAge` on the topic — or publish it with `AppendAsync`, which is what a message with no waiting to
do actually is.

We do not count the past-due tail against the quota, and there is no option to. The bound would have to mean
"work still owed", and no publisher can know that: whether a scheduled message has been delivered is recorded
in each subscriber's own file, and with fan-out there is no single answer to give. Counting the whole table
instead is the thing described above, which refuses schedules over delivered history. So the quota says what
it can say honestly — future-dated work is bounded by count and bytes, the due tail by age — rather than
approximating the number nobody here can compute.

And a due time already in the past means due now, and is stored as now: a row that arrived behind a position
already passed would sit below it and never be offered. Service Bus reads a scheduled enqueue time that has
gone the same way. The due time is read when the row is committed rather than when the call was made, so a
publisher whose writes are queued behind a burst cannot produce a row that lands underneath a position that
moved in the meantime.

## A crash loses nothing that was acknowledged

`AppendAsync` returns when the record is durable. The default setting survives a process crash but not a
power cut; `Durability.PowerLoss` costs an fsync per commit and survives that too.

A delivery interrupted by a crash comes back, and it comes back counted. A message that brings the process
down is dead-lettered rather than retried for ever.

## Delivery order is not guaranteed

Append order is total and stable. Delivery order is not, because a failed message returns after ones handed
out later, and handlers run concurrently. Service Bus and JetStream make the same distinction, so code
written against this keeps working when you move to one. If you need ordering, you need one of those, or a
workflow.

## No backoff, no delayed retry, no scheduler

A failed message comes straight back, up to a limit. A message you defer stays deferred until you ask for
it. Anything that has to wait durably belongs in a durable workflow, which is a different tool.

There is one wait in the library, and it is not a schedule: a reader with nothing to deliver pauses before
looking again, for no longer than the poll interval. It is taken from `ChimeraOptions.Clock` along with
everything else here that reads the time, which is worth knowing if you hand it a clock you drive yourself.
Nothing wakes up in order to deliver a particular message, and there is no timer per message anywhere.

## A clock that goes backwards costs latency, not messages

Each file holds its stamps at the newest moment it has already recorded, seeded from the file when it is
opened, so an NTP correction or a resumed virtual machine cannot write a record that looks older than one
already there. Without that, a due-now scheduled message written just after a step back lands underneath the
position a subscriber has already passed and is never offered again, and age retention picks a cutoff by time
that it then deletes by sequence, taking unexpired records with it.

What it costs: after a step back, a message published as due now becomes due when the wall clock catches up,
late by the size of the step. Nothing is dropped and nothing is hidden.

What it does not cover: two processes writing one topic directory have two clocks, and no single file can put
them in order. That is the same boundary as everywhere else here — one machine.

## Deleting is not erasing

Retention removes records from the database. On an SSD the old content survives underneath until wear
levelling reclaims it; on a copy-on-write filesystem it survives until the old extent is released, which a
snapshot can postpone indefinitely. No setting here changes that. If bytes must be unrecoverable they have to
be unreadable before they are written, which means encrypting above this library and destroying the key when
the time comes.
