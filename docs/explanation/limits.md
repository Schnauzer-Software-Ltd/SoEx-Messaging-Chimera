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
count, because a number of messages cannot be compared with a policy written in hours. Alarm when the lag
approaches the retention window.

**As it happens.** `ApplyRetentionAsync` reads the subscribers' files and names the ones that had not reached
what it removed, and how many each lost. Log that at warning.

**Afterwards.** The subscriber itself throws `MessagesLostException` on its next read, carrying the count and
the range of sequences. Two things are counted in it: records this subscriber never read, and deliveries it
failed. A failure is a promise that the message will come again, and only the log can keep that promise, so a
record removed while a redelivery is owed against it is as lost as one nobody ever saw. It then moves its position over the gap and carries on, because a position stuck
below records that will never exist again would never move again. It comes out of the read loop rather than
out of a handler, so the loop has to be written to catch it and start reading again;
[integrating](../how-to/integrating.md) shows the shape.

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
trimming, the scheduled table at its bound by refusing. What clears a refusal is due times arriving and the
messages being consumed, or cancelling ones nobody wants any more.

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

## Deleting is not erasing

Retention removes records from the database. On an SSD the old content survives underneath until wear
levelling reclaims it; on a copy-on-write filesystem it survives until the old extent is released, which a
snapshot can postpone indefinitely. No setting here changes that. If bytes must be unrecoverable they have to
be unreadable before they are written, which means encrypting above this library and destroying the key when
the time comes.
