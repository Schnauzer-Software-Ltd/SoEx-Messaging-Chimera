# Getting started

You need a directory. There is nothing to install and nothing to run.

```csharp
var options = new ChimeraOptions { RootDirectory = "/var/lib/myapp/events" };

await using var orders = SqliteTopicLog.Open("orders", options);
await orders.AppendAsync(Encoding.UTF8.GetBytes("order-1234 placed"));
```

`AppendAsync` returns once the record is on disk. If your process dies immediately afterwards, that message
is still there when it comes back.

Reading is a subscription, named after whoever is doing the reading:

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

Every message you take must be settled — completed, failed, deferred or dead-lettered. Until you settle it,
it occupies one of the in-flight slots, and once they are all occupied you stop being given work. That is
deliberate: it is better to stop than to accumulate work nobody is tracking.

This loop is the short version. A long-running one also has to survive retention removing records it had not
reached, which ends the enumeration rather than throwing at a handler;
[integrating](../how-to/integrating.md) has the loop to copy.

## Adding a second reader

Add one. It costs nothing and the first one does not notice.

```csharp
await using var audit = SqliteSubscription.Open("audit", "orders", options);
```

Each subscriber keeps its own position, so both see every message, and one falling behind does not hold the
other up. This is the difference between a log and a queue, and it is why adding a reader is not a change to
anything that already exists.

## What to do next

Messages older than the retention window are removed whether or not anybody read them, so run a maintenance
pass somewhere:

```csharp
RetentionReport report = await orders.ApplyRetentionAsync();
if (report.AnyoneOvertaken)
{
    foreach (SubscriberOvertaken loss in report.Overtaken)
        logger.LogWarning("{Subscriber} lost {Count} messages it had not read", loss.Subscriber, loss.Lost);
}
```

And ask a subscriber how far behind it is, in the units your retention window is written in:

```csharp
SubscriptionLag lag = await worker.LagAsync();
// lag.OldestUnread is a duration. Compare it with the retention window and alarm well before they meet.
// lag.DueScheduled counts scheduled messages that have come due and nobody has dealt with yet.
```
