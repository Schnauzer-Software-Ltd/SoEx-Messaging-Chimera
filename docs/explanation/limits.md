# Limits, and how you find out

Chimera has three operating rules:

1. It runs on one machine.
2. Each file has one writer.
3. The publisher and its subscribers stop and start together.

Each limit on this page follows from these rules. Each section gives the limit, its cause, and how you
find out about it.

## One writer per file

The rule applies to each file. A publisher and its subscribers can run in different processes, because
each one writes to a different file. Tests run this configuration:

- A publisher in one process writes five hundred messages.
- Subscribers in other processes read them.
- Two subscribers in two processes each receive all the messages.

Two writers on one file break the rule. The library does not refuse them. In a measured run, two
processes on one subscriber file handled 200 messages in about 210 deliveries. No message was lost. The
file stayed readable. The result of the break is duplicate handling, which at-least-once delivery
permits. Thus a break of this rule can go unnoticed. Keep one writer on each file.

## Messages can be removed before they are read

Retention removes records from the log by age, size or count, read or unread. Retention does not wait for
the slowest subscriber. Thus one stuck subscriber cannot stop writes to the topic.

Chimera bounds each loss, names the subscriber that had the loss, and reports it at three times: before,
during and after the retention pass.

### Before a loss: lag

`LagAsync` gives the lag of one subscriber as a count and as a duration. Use the duration to compare the
lag with a retention policy written in hours. The lag includes the scheduled table and the log, because
the age bound removes rows from both. Set an alarm for when the lag approaches the retention window.

### During a retention pass: the retention report

`ApplyRetentionAsync` reads the subscriber files. Its report names each subscriber that had not reached
the removed records, and the count that each subscriber lost. Log this report at warning level.

The report counts from the position of each subscriber. It can thus name a subscriber for an in-flight
message that the subscriber later completes. This count is the view of the log from outside. The
subscriber's report, `MessagesLostException`, knows the result of each delivery.

### After a loss: `MessagesLostException`

On its next read, the subscriber throws `MessagesLostException`. The exception gives the count and the
range of sequences. The count includes:

- Records that this subscriber did not read.
- Deliveries that this subscriber failed. A failed delivery is owed again, and only the log can supply it.
  A record that retention removes while a redelivery is owed is lost.

A delivery that a handler holds is not counted while the handler holds it, because its result is not
known yet. If the handler then fails it, a later read counts it, after the position has moved. At the
time of the failure, `FailAsync` returns `false`. This result tells the caller that the redelivery cannot
occur.

Each record thus reaches a handler that settles it, or it appears once in these two reports.

After the exception, the subscriber moves its position over the gap and continues. A position below
removed records could never move again. The exception comes from the read loop, not from a handler.
Write the read loop to catch the exception and read again. [Integrating](../how-to/integrating.md) shows
this loop.

The subscriber reports the loss on the next poll, also when the topic is quiet. This includes a pass
that removed all records and left the log empty. The sequence counter is durable, so an empty log keeps
the highest sequence that it reached.

### Retention size and payload limit

When a topic opens, Chimera compares `MaxBytes` with `MaxPayloadBytes`. If `MaxBytes` is below
`MaxPayloadBytes`, the topic can accept a message that no retention pass can keep. The next pass removes
that message as the record that put the log over its size, possibly before a subscriber reads it.
Chimera refuses this configuration.

### Scheduled messages and retention

On the scheduled side, Chimera reports one kind of loss. It reports a failed scheduled delivery when
retention removes its row, because the subscriber keeps a record of the attempt. It does not report a
scheduled message that no subscriber read.

In the scheduled table, a position is a due time, not a sequence. A due-time position cannot tell a
removed row from a row that was never published, so the subscriber sees no gap. Bound the scheduled
table where your code creates the messages. Monitor `RetentionReport.ScheduledRemoved`.

### New subscribers

Chimera reports a loss only to a subscriber that existed when retention removed the records. A
subscriber created later starts from the oldest record still in the log (`Start = Beginning`, the default),
or from the next message (`Start = Next`). Records removed before it existed were never its to read, so they
are not reported to it as loss.

`SubscriptionOptions.Start` sets where a new subscriber starts. It maps to the two products that Chimera
can replace:

- A Service Bus subscription receives nothing that was sent before its creation.
- A JetStream consumer replays the retained window and reports nothing about the records that aged out.

`SubscriptionOptions.Start` applies once, when Chimera first records the position.

## A full dead-letter set

By default (`DeadLetterOverflow.StopDelivering`), a subscriber with a full dead-letter set stops delivery
until somebody acts. No message is
destroyed. `DeadLetterOverflow.DropOldest` is the other setting. It is the only setting in this library
that destroys a message to make room. For that reason, it is not the default.

### What `DropOldest` destroys

The set keeps its newest dead letters and drops the oldest. A message larger than the whole byte bound
is not added to the byte sum. It is over the bound by itself, so it is the row that goes. The older dead
letters stay.

The settle refuses this case when a caller is present. Two routes with no caller can still bring such a
message into the set:

- A deferral that ages out into the dead-letter set.
- A bound lowered below rows already filed. `SetAsidePolicy` applies to the subscription that you open,
  not to the rows in the file.

### What `DropOldest` keeps

`DropOldest` keeps one kind of message: a message that a handler holds. The handler will receive the
result for that message, so its row must exist. All other messages in the set can be dropped. This
includes a message that you replayed and have not received yet.

### Replayed messages

If `ReplayAsync` returns true, the row was marked for delivery at that time. This is a fact about the
store, not a promise of delivery. No reader has read the row yet. If the set reaches its bound in the
meantime, the oldest row goes. That row can be the row that you replayed. An append has the same
relation with retention, which can remove a record before a subscriber reads it.

Replayed rows are not exempt. `MaxInFlight` caps in-flight deliveries, so their exemption adds a small,
known amount of slack. No cap limits how many rows can be made ready at once. An exemption for those rows
would let a caller that replays a whole set disable the bound.

### How you find out

`SetAsideReport.DeadDropped` counts each destroyed dead letter. The count is durable and survives the
process. The next pass that asks reports it. If you need a guarantee that no dead letter is destroyed, use
`StopDelivering`.

## Scheduled messages stay in the file after they are delivered

A scheduled message stays in the scheduled table of the topic file. It does not move into the log.
Delivery does not remove it. Only retention removes these rows, for two reasons:

- Subscribers read the topic file and cannot write to it.
- Another subscriber can still be due to receive the message.

Each subscriber keeps a separate position in the scheduled table, in due-time order. It works the same
as the subscriber's position in the log.

### Disk use

A scheduled row stays on the disk until retention removes it. `RetentionPolicy.MaxAge`, measured from
the due time, sets when. If the policy is unbounded, the row stays.

### The scheduled quota

`MaxBytes` and `MaxCount` trim the log. They never *remove* scheduled rows. On the scheduled side, they
apply when you schedule a message. When the scheduled table is at the bound, `ScheduleAsync` throws
`ScheduleRefusedException`. Chimera does not store the message. Service Bus has the same limit:
scheduled messages count against the entity quota, and the send is refused.

The quota has these effects:

- Ordinary appends continue while Chimera refuses schedules. The log applies the two settings by
  trimming, so a full scheduled table does not stop publishing.
- The size limit of the file is the sum of the two bounds. Trimming holds the log at its bound. Refusal
  holds the scheduled table at its bound.
- A refusal clears when due times arrive, or when you cancel messages that nobody needs.

The quota counts the messages that wait for a due time. It does not count every row in the table. A
delivered scheduled row stays in the table, because with fan-out no single subscriber can remove it. A
count of all rows would count delivered history as pending work. A topic with steady scheduled traffic
would then get refusals with no work outstanding. Service Bus also counts the messages that wait. It
removes the row at enqueue.

### Sizing the scheduled table

The two settings bound *future-dated* work. Time bounds the rest of the table. Size them with both parts
in mind:

    rows in the file  ≈  MaxCount  +  (rate of due-or-past-due schedules × MaxAge)

The second term often surprises people. Chimera stores a schedule whose due time has arrived as due
now. That schedule is never in the counted set, also at the moment Chimera writes it. `MaxCount` does not
see it. These rows are past-due history. A stream of short-dated schedules becomes the same history a
moment after Chimera writes it.

Thus `MaxAge` alone bounds a publisher that schedules for now, or for a second ahead. If that traffic is
heavy, lower `MaxAge` on the topic. You can also publish the messages with `AppendAsync`, because a
message with no wait is an append.

### The past-due tail

The quota does not count the past-due tail. No option enables that count. To count it, the bound would
have to mean "work still owed". No publisher can know that value. Each subscriber records in its own file
whether it received a scheduled message. With fan-out, there is no single answer. A count of the whole
table refuses schedules because of delivered history, as the section above shows. So the quota bounds
future-dated work by count and bytes, and the due tail by age.

### Due times in the past

Chimera stores a due time in the past as now. Otherwise, a row could arrive behind a position that a
subscriber has passed, and nobody would receive it. Service Bus reads a past scheduled enqueue time the
same way. Chimera reads the due time when it commits the row, not when the call occurs. Thus a publisher
with writes queued behind a burst cannot put a row under a position that moved in the meantime.

## A crash loses nothing that was acknowledged

`AppendAsync` returns when the record is durable. The default setting survives a process crash, but not
a power cut. `Durability.PowerLoss` adds an fsync per commit and also survives a power cut.

A delivery that a crash interrupts comes back. Chimera counts it. A message that crashes the process is
dead-lettered. Chimera does not retry it for ever.

## Delivery order is not guaranteed

Append order is total and stable. Delivery order can differ from append order for two reasons:

- A failed message returns after messages that were delivered later.
- Handlers run concurrently.

Service Bus and JetStream make the same distinction. Code written for Chimera thus continues to work
when you move to one of them. If you need ordering, use one of them, or a workflow.

## Retries and waits

A failed message comes back at once, up to a limit. A deferred message stays deferred until you request
it. For a durable wait, use a durable workflow.

The library has one wait. A reader with nothing to deliver pauses for no longer than the poll interval
before it reads again. The wait uses `ChimeraOptions.Clock`, as all the time reads in the library do.
This matters if you supply a clock that you control. No timer runs per message. Nothing starts in order
to deliver one particular message.

## A clock that goes backwards

Each file stamps a record with a time no earlier than the newest time it has recorded. Chimera reads that
time from the file when it opens the file. An NTP correction or a resumed virtual machine thus cannot
write a record that looks older than a record already there. This rule prevents two faults:

- A due-now scheduled message, written immediately after a step back, lands below a position that a subscriber
  has passed. Nobody receives it.
- Age retention picks a cutoff by time, then deletes by sequence. It removes records that have not
  expired.

The rule has a cost. After a step back, a message published as due now becomes due when the wall clock
catches up. It is late by the size of the step. No message is dropped or hidden.

The rule holds within one file. Processes that write different files in one root directory use separate
clocks. No single file can put the stamps of two clocks in order. This is the one-machine rule again.

## Deleting is not erasing

Retention removes records from the database. The old content stays on the disk for a time:

- On an SSD, it stays until wear levelling reclaims it.
- On a copy-on-write filesystem, it stays until the old extent is released. A snapshot can postpone this
  indefinitely.

No setting in this library changes this. If bytes must be unrecoverable, they must be unreadable before
Chimera writes them. Encrypt them above this library. Destroy the key when the time comes.
