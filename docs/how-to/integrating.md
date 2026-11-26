# Integrating with a messaging framework

Chimera publishes and reads byte payloads on named topics. A binding in your messaging framework maps each
contract to a topic name and each handler to a subscription. It needs three things: one shared topic log per
topic, a consume loop, and a fixed topic name.

## Share one log per topic

A topic file has one writer. Frameworks often create a sender per call, so open each `SqliteTopicLog` once
and share it:

```csharp
static readonly ConcurrentDictionary<string, SqliteTopicLog> Logs = new();

static SqliteTopicLog LogFor(ChimeraOptions options, string topic) =>
    Logs.GetOrAdd($"{Path.GetFullPath(options.RootDirectory)}|{topic}", _ => SqliteTopicLog.Open(topic, options));
```

## Run a consume loop

```csharp
while (!stopping.IsCancellationRequested)
{
    try
    {
        await foreach (EventEnvelope message in subscription.ReadAsync(stopping))
        {
            try
            {
                await DispatchToHandler(message.Payload);
                await subscription.CompleteAsync(message);
            }
            catch (Exception ex)
            {
                await subscription.FailAsync(message, ex.Message);
            }
        }
    }
    catch (MessagesLostException lost)
    {
        // Retention removed records this subscriber had not finished with — ones it never read, and ones it
        // failed and was owed again. The position has already moved over the gap, so reading again carries
        // on from the oldest record that still exists.
        Logger.LogWarning("Lost {Count} message(s) on {Topic}: {Range}",
            lost.Count, lost.Topic, $"{lost.FromSequence}..{lost.ToSequence}");
    }
    catch (OperationCanceledException) when (stopping.IsCancellationRequested)
    {
        break;
    }
}
```

1. Catch `MessagesLostException` in an outer loop. The exception comes from the enumeration and ends the
   `await foreach`. A catch inside the loop body does not see it.
2. Restart the enumeration. Chimera raises the exception once per gap, after the position has moved past it.
3. Start the loop when your endpoint starts to listen.
4. Cancel the loop when your endpoint closes.

## Fix the topic name

1. Take the topic name from configuration or from an attribute.
2. Do not derive the topic name from a fully-qualified type name.

   The topic name is a filename. If you move a class to another namespace, the binding points at an empty
   file. The old backlog stays on disk under the old name.

## Encryption

Chimera stores payloads as given. Encrypt sensitive payloads before publication. With one key per
subject, destroying a key makes that person's data unreadable in place.

## Delayed retries

`FailAsync` makes a message deliverable again at once. To retry after a delay, such as five minutes, wait
inside the handler. The wait holds an in-flight slot. For a wait that must survive a restart, use a durable
workflow.
