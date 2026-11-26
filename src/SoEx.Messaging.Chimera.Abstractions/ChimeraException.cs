namespace SoEx.Messaging.Chimera;

/// <summary>Base for every failure this library raises deliberately.</summary>
public class ChimeraException : Exception
{
    /// <summary>Creates the exception with a message that says what failed.</summary>
    /// <param name="message">What failed, for the person reading the log.</param>
    public ChimeraException(string message) : base(message) { }

    /// <summary>Creates the exception with a message and the failure that caused it.</summary>
    /// <param name="message">What failed, for the person reading the log.</param>
    /// <param name="inner">The exception that caused this one.</param>
    public ChimeraException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// A subscriber's stored position does not belong to the log it is now reading — a different log identity, or a
/// position beyond the log's end. Both mean a restore has put the two out of step, and continuing would silently
/// skip messages as the log reissues sequences the cursor already treats as done.
/// </summary>
public sealed class LogIdentityException : ChimeraException
{
    /// <summary>Creates the exception with a message that names the subscriber and the log.</summary>
    /// <param name="message">What does not match, for the person reading the log.</param>
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
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic the messages were removed from.</param>
    /// <param name="subscriber">The subscriber that was owed them.</param>
    /// <param name="count">How many messages were lost.</param>
    /// <param name="from">The first sequence or identifier in the range the loss was found in.</param>
    /// <param name="to">The last sequence or identifier in that range.</param>
    /// <param name="scheduled">True when the loss was in the scheduled table.</param>
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

    /// <summary>The topic the messages were removed from.</summary>
    public string Topic { get; }

    /// <summary>The subscriber that was owed them.</summary>
    public string Subscriber { get; }

    /// <summary>How many messages this subscriber lost.</summary>
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
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic the payload was published to.</param>
    /// <param name="size">The size of the payload, in bytes.</param>
    /// <param name="limit">The topic's payload limit, in bytes.</param>
    public PayloadTooLargeException(string topic, int size, int limit)
        : base($"A payload of {size} bytes exceeds the {limit}-byte limit on topic '{topic}'.")
    {
        Topic = topic;
        Size = size;
        Limit = limit;
    }

    /// <summary>The topic the payload was published to.</summary>
    public string Topic { get; }

    /// <summary>The size of the payload, in bytes.</summary>
    public int Size { get; }

    /// <summary>The topic's payload limit, in bytes.</summary>
    public int Limit { get; }
}

/// <summary>Which kind of limit a store reached: how many rows it holds, or how many bytes.</summary>
public enum SizeBound
{
    /// <summary>The bound is on the number of rows.</summary>
    Count = 0,

    /// <summary>The bound is on the payload bytes the rows hold between them.</summary>
    Bytes = 1,
}

/// <summary>
/// This subscriber's dead-letter set has reached one of the bounds it was given, and it is configured to stop
/// rather than destroy anything. It holds its position and blocks nobody else; deal with the dead letters and
/// it carries on.
/// </summary>
public sealed class SubscriberStoppedException : ChimeraException
{
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic the subscriber has stopped reading.</param>
    /// <param name="subscriber">The subscriber that has stopped.</param>
    /// <param name="deadLetters">How many dead letters it holds.</param>
    /// <param name="deadBytes">The payload bytes they hold between them.</param>
    /// <param name="limit">The limit that was reached, in the units of <paramref name="bound"/>.</param>
    /// <param name="bound">Which bound was reached.</param>
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

    /// <summary>The topic the subscriber has stopped reading.</summary>
    public string Topic { get; }

    /// <summary>The subscriber that has stopped.</summary>
    public string Subscriber { get; }

    /// <summary>Dead letters held, whichever bound stopped the subscriber.</summary>
    public long DeadLetters { get; }

    /// <summary>Payload bytes they hold between them.</summary>
    public long DeadBytes { get; }

    /// <summary>The limit that was reached, in the units of <see cref="Bound"/>.</summary>
    public long Limit { get; }

    /// <summary>Which bound was reached: the number of dead letters, or their bytes.</summary>
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
/// to drop the oldest with a byte bound smaller than the message: this row is over that bound on its own, so
/// the trim destroys it whatever else the set holds and filing it would be storing a message in order to lose
/// it. It is refused instead.
/// </summary>
/// <remarks>
/// <para>
/// The delivery is still outstanding, exactly as after any settle that threw. There are two ways on:
/// complete it, which loses the message knowingly, or raise <see cref="SetAsidePolicy.MaxDeadBytes"/> above
/// the topic's payload limit and settle it again. Trying the same settle again will be refused again — the
/// bound cannot hold this message however many times it is asked.
/// </para>
/// <para>
/// Raised where the row would be joining the dead set. A message already filed dead — replayed, and then
/// failed out of its attempts or dead-lettered again — is re-filed as what it already is without complaint:
/// it is held, and holding it costs the set nothing it is not already paying. Refusing it there would offer
/// a reason untrue of it and leave a replayed dead letter no way back into the store it came out of.
/// </para>
/// </remarks>
public sealed class DeadLetterTooLargeException : ChimeraException
{
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic the delivery came from.</param>
    /// <param name="subscriber">The subscriber that tried to file the dead letter.</param>
    /// <param name="size">The size of the message's payload, in bytes.</param>
    /// <param name="limit">The subscriber's dead-letter byte bound.</param>
    public DeadLetterTooLargeException(string topic, string subscriber, int size, long limit)
        : base($"Subscriber '{subscriber}' cannot file a {size}-byte dead letter on topic '{topic}': its " +
               $"dead-letter bound is {limit} bytes and it drops the oldest to stay inside it, so this one " +
               "is over the bound on its own and would be destroyed by the next trim however much room the " +
               "set has. Filing it would be storing the message in order to lose it. Complete the delivery " +
               "to lose it deliberately, or raise MaxDeadBytes above the topic's payload limit.")
    {
        Topic = topic;
        Subscriber = subscriber;
        Size = size;
        Limit = limit;
    }

    /// <summary>The topic the delivery came from.</summary>
    public string Topic { get; }

    /// <summary>The subscriber that tried to file the dead letter.</summary>
    public string Subscriber { get; }

    /// <summary>The size of the message's payload, in bytes.</summary>
    public int Size { get; }

    /// <summary>The subscriber's dead-letter byte bound, which the message exceeds on its own.</summary>
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
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic the message was scheduled on.</param>
    /// <param name="scheduled">Scheduled messages the topic holds that are not yet due.</param>
    /// <param name="bytes">The payload bytes those messages hold between them.</param>
    /// <param name="limit">The limit that was reached, in the units of <paramref name="bound"/>.</param>
    /// <param name="bound">Which bound was reached.</param>
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

    /// <summary>The topic the message was scheduled on.</summary>
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

    /// <summary>Which bound was reached: the number of scheduled messages, or their bytes.</summary>
    public SizeBound Bound { get; }
}
