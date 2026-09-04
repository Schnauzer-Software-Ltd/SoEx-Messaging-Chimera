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
/// Retention removed records this subscriber had not finished with. Carries how many, because a count that
/// nobody can state is the difference between a known loss and a silent one.
/// </summary>
/// <remarks>
/// Two things end up here. One is a record this subscriber never read. The other is a delivery it failed,
/// which the library promised to make again and now cannot, because only the log could offer it and the log
/// no longer holds it. Both are messages this subscriber was owed and will never see, so both are counted.
/// </remarks>
public sealed class MessagesLostException : ChimeraException
{
    public MessagesLostException(string topic, string subscriber, long count, long from, long to,
        bool scheduled = false)
        : base(Describe(topic, subscriber, count, from, to, scheduled))
    {
        Topic = topic;
        Subscriber = subscriber;
        Count = count;
        FromSequence = from;
        ToSequence = to;
        Scheduled = scheduled;
    }

    public string Topic { get; }
    public string Subscriber { get; }
    public long Count { get; }

    /// <summary>
    /// The range the loss was found in. Log sequences ordinarily; identifiers from the scheduled table when
    /// <see cref="Scheduled"/> is set, since the two tables number their rows separately.
    /// </summary>
    public long FromSequence { get; }

    /// <inheritdoc cref="FromSequence"/>
    public long ToSequence { get; }

    /// <summary>
    /// The loss was in the scheduled table rather than the log. Only failed deliveries are ever reported from
    /// there: a scheduled message nobody read leaves nothing behind to notice its removal by, whereas a
    /// delivery that was made and failed leaves a record of the attempt that outlives the row.
    /// </summary>
    public bool Scheduled { get; }

    private static string Describe(string topic, string subscriber, long count, long from, long to,
        bool scheduled)
        => scheduled
            ? $"Retention removed {count} scheduled message(s) from topic '{topic}' that subscriber " +
              $"'{subscriber}' had failed and was owed again (scheduled {from}..{to})."
            : $"Retention removed {count} message(s) from topic '{topic}' before subscriber " +
              $"'{subscriber}' finished with them (sequences {from}..{to}).";
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

/// <summary>Which bound on the dead-letter set was reached.</summary>
public enum DeadLetterBound
{
    /// <summary><see cref="SetAsidePolicy.MaxDeadCount"/>.</summary>
    Count = 0,

    /// <summary><see cref="SetAsidePolicy.MaxDeadBytes"/>.</summary>
    Bytes = 1,
}

/// <summary>
/// This subscriber's dead-letter set has reached one of the bounds it was given, and it is configured to stop
/// rather than destroy anything. It holds its position and blocks nobody else; deal with the dead letters and
/// it carries on.
/// </summary>
public sealed class SubscriberStoppedException : ChimeraException
{
    public SubscriberStoppedException(string topic, string subscriber, long deadLetters, long deadBytes,
        long limit, DeadLetterBound bound)
        : base(Describe(topic, subscriber, deadLetters, deadBytes, limit, bound))
    {
        Topic = topic;
        Subscriber = subscriber;
        DeadLetters = deadLetters;
        DeadBytes = deadBytes;
        Limit = limit;
        Bound = bound;
    }

    public string Topic { get; }
    public string Subscriber { get; }

    /// <summary>Dead letters held, whichever bound stopped the subscriber.</summary>
    public long DeadLetters { get; }

    /// <summary>Payload bytes they hold between them.</summary>
    public long DeadBytes { get; }

    /// <summary>The limit that was reached, in the units of <see cref="Bound"/>.</summary>
    public long Limit { get; }

    public DeadLetterBound Bound { get; }

    private static string Describe(string topic, string subscriber, long deadLetters, long deadBytes,
        long limit, DeadLetterBound bound)
        => bound == DeadLetterBound.Bytes
            ? $"Subscriber '{subscriber}' has stopped reading topic '{topic}': {deadBytes} bytes across " +
              $"{deadLetters} dead letters reaches the limit of {limit} bytes. Replay or discard them to continue."
            : $"Subscriber '{subscriber}' has stopped reading topic '{topic}': {deadLetters} dead letters " +
              $"reaches the limit of {limit}. Replay or discard them to continue.";
}
