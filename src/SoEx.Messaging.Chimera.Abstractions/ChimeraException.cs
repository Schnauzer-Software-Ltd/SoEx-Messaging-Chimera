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

/// <summary>Which kind of limit a store reached: how many rows it holds, or how many bytes.</summary>
public enum SizeBound
{
    Count = 0,
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
        long limit, SizeBound bound)
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

    public SizeBound Bound { get; }

    private static string Describe(string topic, string subscriber, long deadLetters, long deadBytes,
        long limit, SizeBound bound)
        => bound == SizeBound.Bytes
            ? $"Subscriber '{subscriber}' has stopped reading topic '{topic}': {deadBytes} bytes across " +
              $"{deadLetters} dead letters reaches the limit of {limit} bytes. Replay or discard them to continue."
            : $"Subscriber '{subscriber}' has stopped reading topic '{topic}': {deadLetters} dead letters " +
              $"reaches the limit of {limit}. Replay or discard them to continue.";
}

/// <summary>
/// A dead letter could not be filed, because it could not then be kept. Only ever raised by a subscriber set
/// to drop the oldest with a byte bound smaller than the message: keeping this row means destroying every
/// other dead letter the subscriber holds and still standing over the bound, so it is refused instead.
/// </summary>
/// <remarks>
/// The delivery is still outstanding, exactly as after any settle that threw. There are two ways on:
/// complete it, which loses the message knowingly, or raise <see cref="SetAsidePolicy.MaxDeadBytes"/> above
/// the topic's payload limit and settle it again. Trying the same settle again will be refused again — the
/// bound cannot hold this message however many times it is asked.
/// </remarks>
public sealed class DeadLetterTooLargeException : ChimeraException
{
    public DeadLetterTooLargeException(string topic, string subscriber, int size, long limit)
        : base($"Subscriber '{subscriber}' cannot file a {size}-byte dead letter on topic '{topic}': its " +
               $"dead-letter bound is {limit} bytes and it drops the oldest to stay inside it, so keeping " +
               "this one would destroy every other dead letter it holds and still leave the set over the " +
               "bound. Complete the delivery to lose the message deliberately, or raise MaxDeadBytes above " +
               "the topic's payload limit.")
    {
        Topic = topic;
        Subscriber = subscriber;
        Size = size;
        Limit = limit;
    }

    public string Topic { get; }
    public string Subscriber { get; }
    public int Size { get; }
    public long Limit { get; }
}

/// <summary>
/// A scheduled message was refused because the topic's scheduled table is at the bound the retention policy
/// sets. Back-pressure at publish, because the alternative is not available here: a scheduled message is work
/// somebody asked for at a time that has not arrived, and removing it to stay under a size bound would be
/// cancelling the future rather than trimming history.
/// </summary>
/// <remarks>
/// Service Bus does the same thing for the same reason — scheduled messages count against the entity quota
/// and the send is refused. What clears it is the same as there: due times arriving and the messages being
/// consumed, or cancelling ones that are no longer wanted. <see cref="Scheduled"/> and <see cref="Bytes"/>
/// count what is still waiting for a due time, which is what the bound is on; rows already past due are held
/// by <c>RetentionPolicy.MaxAge</c> instead, so a schedule made for now is never refused by this.
/// </remarks>
public sealed class ScheduleRefusedException : ChimeraException
{
    public ScheduleRefusedException(string topic, long scheduled, long bytes, long limit, SizeBound bound)
        : base(bound == SizeBound.Bytes
            ? $"Topic '{topic}' is holding {bytes} bytes of scheduled messages that are not yet due, which " +
              $"is its bound of {limit} bytes. Scheduled messages are not trimmed by retention, so the " +
              "schedule is refused rather than accepted into a table nothing will empty."
            : $"Topic '{topic}' is holding {scheduled} scheduled message(s) that are not yet due, which is " +
              $"its bound of {limit}. Scheduled messages are not trimmed by retention, so the schedule is " +
              "refused rather than accepted into a table nothing will empty.")
    {
        Topic = topic;
        Scheduled = scheduled;
        Bytes = bytes;
        Limit = limit;
        Bound = bound;
    }

    public string Topic { get; }

    /// <summary>
    /// Scheduled messages the topic is holding for a due time that has not arrived. Rows already past due are
    /// not counted: they are work on its way out rather than work waiting, and what removes them is the age
    /// bound rather than this one.
    /// </summary>
    public long Scheduled { get; }

    /// <summary>Payload bytes those messages hold between them.</summary>
    public long Bytes { get; }

    /// <summary>The limit that was reached, in the units of <see cref="Bound"/>.</summary>
    public long Limit { get; }

    public SizeBound Bound { get; }
}
