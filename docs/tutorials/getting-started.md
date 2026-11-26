# Getting started

This tutorial publishes a message to a topic and reads it with two subscribers.

## Publish a message

1. Set the root directory in `ChimeraOptions`. Chimera keeps its files there.
2. Open the topic log for the `orders` topic.
3. Append a record.

```csharp
var options = new ChimeraOptions { RootDirectory = "/var/lib/myapp/events" };

await using var orders = SqliteTopicLog.Open("orders", options);
await orders.AppendAsync(Encoding.UTF8.GetBytes("order-1234 placed"));
```

`AppendAsync` returns when the record is on disk. If the process crashes after that, the record is still in
the topic on restart.

## Read the messages

4. Open a subscription for the subscriber `billing` on the `orders` topic.
5. Read the messages in a loop.
6. Settle each message. After the handler succeeds, call `CompleteAsync`. If the handler throws, call `FailAsync`.

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

Settle every message you read: complete, fail, defer or dead-letter it. An unsettled message holds an
in-flight slot. When all slots are in use, delivery stops until you settle a message.

This is the minimal loop. In a long-running loop, retention can remove unread records. The enumeration then ends with `MessagesLostException`. [Integrating](../how-to/integrating.md) has the
full loop.

## Add a second subscriber

7. Open a subscription for a second subscriber, `audit`, on the same topic.

```csharp
await using var audit = SqliteSubscription.Open("audit", "orders", options);
```

Each subscriber has a separate position in the topic. Both subscribers receive every message. A slow subscriber
delays only itself.

## Next steps

8. Run a retention pass from a maintenance task. Retention removes messages older than the retention window,
   read or unread.

```csharp
RetentionReport report = await orders.ApplyRetentionAsync();
if (report.AnyoneOvertaken)
{
    foreach (SubscriberOvertaken loss in report.Overtaken)
        logger.LogWarning("{Subscriber} lost {Count} messages it had not read", loss.Subscriber, loss.Lost);
}
```

9. Check the lag of each subscriber. `LagAsync` reports lag as a duration, so you can compare it with the
   retention window.

```csharp
SubscriptionLag lag = await worker.LagAsync();
// lag.OldestUnread is a duration. Compare it with the retention window and alarm well before they meet.
// lag.DueScheduled counts scheduled messages that have come due and nobody has dealt with yet.
```
