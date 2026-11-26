# Integrating with a messaging framework

Chimera takes bytes and names. Your binding maps your contracts to topic names and your handlers to consumers.
Broker clients use the same split: the framework maps a type to a queue name.

A binding has three parts: a shared log per topic, a consume loop, and a declared topic name.

## Share one log per topic

1. Keep one `SqliteTopicLog` per topic in a shared place.

   Frameworks usually create a new sender object for each call. A topic file permits one writer. If each sender
   opens its own `SqliteTopicLog`, the file gets several writers.

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

1. Catch `MessagesLostException` in an outer loop, outside the `await foreach`.

   The outer loop is necessary. `MessagesLostException` comes from the enumeration itself, and it ends the
   `await foreach`. If the loop catches exceptions only inside the body, consumption stops the first time
   retention overtakes the subscriber.

2. Restart the enumeration after the exception.

   A restart loses nothing. Chimera raises the exception once, after the position has moved past the gap.

3. Start the loop when your endpoint starts to listen.
4. Cancel the loop when your endpoint closes.

## Declare the topic name

1. Take the topic name from configuration or from an attribute.
2. Do not derive the topic name from a fully-qualified type name.

   The topic name is a filename. If you move a class to another namespace, the binding points at an empty
   file. The old backlog stays on disk under the old name.

## Encryption and waiting

### Encryption

Chimera stores payloads as given.

1. Encrypt each message above Chimera.

   At that level, you can scope a key to a subject. When you destroy that key, you erase the data of one
   person. The store stays as it is.

### Waiting

`FailAsync` makes a message deliverable again at once.

1. To try a message again after a delay, such as five minutes, wait inside the handler.

   The wait holds an in-flight slot.

2. If the wait must survive a restart, put it in a durable workflow.
