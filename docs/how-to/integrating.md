# Integrating with a messaging framework

This library takes bytes and names. Turning your contracts into topic names, and your handlers into something
that consumes, is the binding you write on top — the same division as any broker client, where mapping a type
to a queue name is the framework's job rather than the SDK's.

A binding needs three things.

## One log per topic, shared

Frameworks usually create the object that sends a message per call. If each of those opened its own
`SqliteTopicLog`, you would have several writers on a file that permits one. Keep them somewhere shared:

```csharp
static readonly ConcurrentDictionary<string, SqliteTopicLog> Logs = new();

static SqliteTopicLog LogFor(ChimeraOptions options, string topic) =>
    Logs.GetOrAdd($"{Path.GetFullPath(options.RootDirectory)}|{topic}", _ => SqliteTopicLog.Open(topic, options));
```

## A consume loop

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
        // Retention removed records this subscriber had not read. The position has already moved over the
        // gap, so reading again carries on from the oldest record that still exists.
        Logger.LogWarning("Lost {Count} message(s) on {Topic}: {Range}",
            lost.Count, lost.Topic, $"{lost.FromSequence}..{lost.ToSequence}");
    }
    catch (OperationCanceledException) when (stopping.IsCancellationRequested)
    {
        break;
    }
}
```

The outer loop is not optional. `MessagesLostException` comes out of the enumeration itself rather than out
of a handler, so it ends the `await foreach`, and a loop that only catches inside the body stops consuming
the first time retention overtakes it. Nothing is lost by restarting the enumeration: the exception is
raised once, after the position has already moved past the gap.

Start the loop when your endpoint starts listening and cancel it when it closes.

## A declared topic name

Take the topic from configuration or from an attribute. Do not derive it from a fully-qualified type name:
here the topic is a filename, so moving a class to another namespace would point the binding at an empty file
and leave the old backlog stranded on disk under the old name.

## Two things this library will not do for you

**Encryption.** It stores the bytes you hand it. Encrypt above it, per message, where a key can be scoped to
a subject and destroying that key erases one person's data without rewriting the store.

**Waiting.** There is no delayed retry and no scheduler. If a handler wants to try again in five minutes, it
can await inside the handler at the cost of holding an in-flight slot; if that wait has to survive a restart,
it belongs in a durable workflow.
