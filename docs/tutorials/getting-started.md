# Getting started

This tutorial publishes messages to a topic and reads them with two subscribers. Chimera needs one directory,
the root directory.

## Publish a message

1. Set the root directory in `ChimeraOptions`.
2. Open the topic log for the `orders` topic.
3. Append a record to the topic.

```csharp
var options = new ChimeraOptions { RootDirectory = "/var/lib/myapp/events" };

await using var orders = SqliteTopicLog.Open("orders", options);
await orders.AppendAsync(Encoding.UTF8.GetBytes("order-1234 placed"));
```

`AppendAsync` returns when the record is on disk. If the process stops immediately after that, the record
stays in the topic. The record is there when the process starts again.

## Read the messages

4. Open a subscription for the subscriber `billing` on the `orders` topic.
5. Read the messages in a loop.
6. Settle each message. Call `CompleteAsync` after the handler succeeds. Call `FailAsync` if the handler throws.

```csharp
await using var worker = SqliteSubscription.Open("billing", "orders", options);

await foreach (EventEnvelope message in worker.ReadAsync(cancellationToken))
{
    try
    {
        await Handle(message.Payload);
        await worker.CompleteAsync(message);
    }
    catch (Exception ex)
    {
        // Straight back into the queue, and a dead letter once the attempts run out.
        await worker.FailAsync(message, ex.Message);
    }
}
```

The subscriber name identifies the reader. Settle every message that you read: complete, fail, defer or
dead-letter it.

Each message that is not settled holds one in-flight slot. When all the in-flight slots are full, delivery
stops until you settle a message. This stop keeps all work in progress tracked.

This loop is a short version. Retention can remove records that a long-running loop has not read yet. The
enumeration then ends. No exception goes to a handler. [Integrating](../how-to/integrating.md) gives the full
loop to copy.

## Add a second subscriber

7. Open a subscription for a second subscriber, `audit`, on the same topic.

```csharp
await using var audit = SqliteSubscription.Open("audit", "orders", options);
```

Each subscriber keeps a separate position in the topic. Both subscribers get every message. A slow subscriber
does not delay the other subscriber. The new subscriber costs nothing. The first subscriber continues as before.

## What to do next

8. Run a retention pass from a maintenance task.

Retention removes messages older than the retention window, read or unread.

```csharp
RetentionReport report = await orders.ApplyRetentionAsync();
if (report.AnyoneOvertaken)
{
    foreach (SubscriberOvertaken loss in report.Overtaken)
        logger.LogWarning("{Subscriber} lost {Count} messages it had not read", loss.Subscriber, loss.Lost);
}
```

9. Ask each subscriber for its lag.

`LagAsync` reports the lag in the same units as the retention window.

```csharp
SubscriptionLag lag = await worker.LagAsync();
// lag.OldestUnread is a duration. Compare it with the retention window and alarm well before they meet.
// lag.DueScheduled counts scheduled messages that have come due and nobody has dealt with yet.
```
