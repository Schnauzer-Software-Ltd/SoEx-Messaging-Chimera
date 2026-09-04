namespace SoEx.Messaging.Chimera;

/// <summary>Base for every failure this library raises deliberately.</summary>
public class ChimeraException : Exception
{
    public ChimeraException(string message) : base(message) { }
    public ChimeraException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// A subscriber's stored position does not belong to the log it is now reading — a different log identity, or a
/// position beyond the log's end. Both mean a restore has put the two out of step, and continuing would silently
/// skip messages as the log reissues sequences the cursor already treats as done.
/// </summary>
public sealed class LogIdentityException : ChimeraException
{
    public LogIdentityException(string message) : base(message) { }
}

/// <summary>
/// Retention removed log records this subscriber had not read. Carries how many, because a count that nobody can
/// state is the difference between a known loss and a silent one.
/// </summary>
public sealed class MessagesLostException : ChimeraException
{
    public MessagesLostException(string topic, string subscriber, long count, long from, long to)
        : base($"Retention removed {count} unread message(s) from topic '{topic}' before subscriber " +
               $"'{subscriber}' read them (sequences {from}..{to}).")
    {
        Topic = topic;
        Subscriber = subscriber;
        Count = count;
        FromSequence = from;
        ToSequence = to;
    }

    public string Topic { get; }
    public string Subscriber { get; }
    public long Count { get; }
    public long FromSequence { get; }
    public long ToSequence { get; }
}

/// <summary>
/// A payload exceeded the topic's limit. Thrown at publish, where the mistake is, rather than accepted and
/// discovered later by whoever the message was meant for.
/// </summary>
public sealed class PayloadTooLargeException : ChimeraException
{
    public PayloadTooLargeException(string topic, int size, int limit)
        : base($"A payload of {size} bytes exceeds the {limit}-byte limit on topic '{topic}'.")
    {
        Topic = topic;
        Size = size;
        Limit = limit;
    }

    public string Topic { get; }
    public int Size { get; }
    public int Limit { get; }
}

/// <summary>
/// This subscriber has as many dead letters as it is allowed and is configured to stop rather than destroy
/// them. It holds its position and blocks nobody else; deal with the dead letters and it carries on.
/// </summary>
public sealed class SubscriberStoppedException : ChimeraException
{
    public SubscriberStoppedException(string topic, string subscriber, long deadLetters, long limit)
        : base($"Subscriber '{subscriber}' has stopped reading topic '{topic}': {deadLetters} dead letters " +
               $"reaches the limit of {limit}. Replay or discard them to continue.")
    {
        Topic = topic;
        Subscriber = subscriber;
        DeadLetters = deadLetters;
        Limit = limit;
    }

    public string Topic { get; }
    public string Subscriber { get; }
    public long DeadLetters { get; }
    public long Limit { get; }
}
