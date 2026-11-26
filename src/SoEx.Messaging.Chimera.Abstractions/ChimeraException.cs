namespace SoEx.Messaging.Chimera;

/// <summary>The base class for each failure that this library throws on purpose.</summary>
public class ChimeraException : Exception
{
    /// <summary>Creates the exception with a message that tells what failed.</summary>
    /// <param name="message">What failed. The message is for the person who reads the log.</param>
    public ChimeraException(string message) : base(message) { }

    /// <summary>Creates the exception with a message and the failure that caused it.</summary>
    /// <param name="message">What failed. The message is for the person who reads the log.</param>
    /// <param name="inner">The exception that caused this one.</param>
    public ChimeraException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// The stored position of a subscriber does not match the log that the subscriber reads.
/// </summary>
/// <remarks>
/// This exception has two causes: a different log identity, or a position after the end of the log. Each cause
/// comes from a restore that put the subscriber and the log out of step. If the subscriber continued, it would
/// skip messages with no report. The log would issue sequences again that the position already marks as done.
/// </remarks>
public sealed class LogIdentityException : ChimeraException
{
    /// <summary>Creates the exception with a message that names the subscriber and the log.</summary>
    /// <param name="message">What does not match. The message is for the person who reads the log.</param>
    public LogIdentityException(string message) : base(message) { }
}

/// <summary>
/// Retention removed records before this subscriber finished with them. <see cref="Count"/> gives the number.
/// </summary>
/// <remarks>
/// <para>
/// The count makes the loss known. A loss with no count is a silent loss.
/// </para>
/// <para>
/// The count includes two kinds of message:
/// <list type="bullet">
/// <item>A record that this subscriber did not read.</item>
/// <item>A failed delivery that the library must deliver again. Only the log can supply the message, and the
/// log no longer holds it.</item>
/// </list>
/// This subscriber was owed both kinds and cannot receive them now. Thus the count includes both.
/// </para>
/// </remarks>
public sealed class MessagesLostException : ChimeraException
{
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic that retention removed the messages from.</param>
    /// <param name="subscriber">The subscriber that was owed the messages.</param>
    /// <param name="count">The number of lost messages.</param>
    /// <param name="from">The first sequence or identifier of the range that contains the loss.</param>
    /// <param name="to">The last sequence or identifier of that range.</param>
    /// <param name="scheduled">True if the loss is in the scheduled table.</param>
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

    /// <summary>The topic that retention removed the messages from.</summary>
    public string Topic { get; }

    /// <summary>The subscriber that was owed the messages.</summary>
    public string Subscriber { get; }

    /// <summary>The number of messages that this subscriber lost.</summary>
    public long Count { get; }

    /// <summary>
    /// The range that contains the loss.
    /// </summary>
    /// <remarks>
    /// The values are log sequences. If <see cref="Scheduled"/> is true, the values are identifiers from the
    /// scheduled table. The two tables number their rows separately.
    /// </remarks>
    public long FromSequence { get; }

    /// <inheritdoc cref="FromSequence"/>
    public long ToSequence { get; }

    /// <summary>
    /// True if the loss is in the scheduled table. False if the loss is in the log.
    /// </summary>
    /// <remarks>
    /// The scheduled table reports only failed deliveries. If retention removes an unread scheduled message,
    /// nothing records the removal. A delivery that failed leaves a record of the attempt. That record stays
    /// after retention removes the row.
    /// </remarks>
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
/// A payload is larger than the payload limit of the topic.
/// </summary>
/// <remarks>
/// The publish operation throws this exception. Thus the error shows at its source, before a subscriber
/// receives the message.
/// </remarks>
public sealed class PayloadTooLargeException : ChimeraException
{
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic that the payload was published to.</param>
    /// <param name="size">The size of the payload, in bytes.</param>
    /// <param name="limit">The payload limit of the topic, in bytes.</param>
    public PayloadTooLargeException(string topic, int size, int limit)
        : base($"A payload of {size} bytes exceeds the {limit}-byte limit on topic '{topic}'.")
    {
        Topic = topic;
        Size = size;
        Limit = limit;
    }

    /// <summary>The topic that the payload was published to.</summary>
    public string Topic { get; }

    /// <summary>The size of the payload, in bytes.</summary>
    public int Size { get; }

    /// <summary>The payload limit of the topic, in bytes.</summary>
    public int Limit { get; }
}

/// <summary>The kind of limit that a store reached: a number of rows or a number of bytes.</summary>
public enum SizeBound
{
    /// <summary>A bound on the number of rows.</summary>
    Count = 0,

    /// <summary>A bound on the total payload bytes of the rows.</summary>
    Bytes = 1,
}

/// <summary>
/// The dead-letter set of this subscriber reached one of its bounds. The subscriber stopped.
/// </summary>
/// <remarks>
/// The subscriber is configured to stop at the bound. This setting destroys no message. The stopped subscriber
/// keeps its position. Other subscribers continue. To continue this subscriber, handle its dead letters.
/// </remarks>
public sealed class SubscriberStoppedException : ChimeraException
{
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic that the subscriber stopped reading.</param>
    /// <param name="subscriber">The subscriber that stopped.</param>
    /// <param name="deadLetters">The number of dead letters that the subscriber holds.</param>
    /// <param name="deadBytes">The total payload bytes of the dead letters.</param>
    /// <param name="limit">The limit that was reached, in the units of <paramref name="bound"/>.</param>
    /// <param name="bound">The bound that was reached.</param>
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

    /// <summary>The topic that the subscriber stopped reading.</summary>
    public string Topic { get; }

    /// <summary>The subscriber that stopped.</summary>
    public string Subscriber { get; }

    /// <summary>The number of dead letters held. This value is set for each kind of bound.</summary>
    public long DeadLetters { get; }

    /// <summary>The total payload bytes of the dead letters.</summary>
    public long DeadBytes { get; }

    /// <summary>The limit that was reached, in the units of <see cref="Bound"/>.</summary>
    public long Limit { get; }

    /// <summary>The bound that was reached: the number of dead letters or their total bytes.</summary>
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
/// The subscriber refused to file a dead letter that it cannot keep.
/// </summary>
/// <remarks>
/// <para>
/// Only one configuration throws this exception: a subscriber that drops the oldest dead letters, with a byte
/// bound smaller than the message. This row is over the bound by itself. The next trim destroys it, whatever
/// else the set holds. Filing it would store the message only to lose it. Thus the subscriber refuses it.
/// </para>
/// <para>
/// The delivery stays outstanding, as after each settle that throws. To continue, do one of these:
/// <list type="bullet">
/// <item>Complete the delivery. This loses the message on purpose.</item>
/// <item>Raise <see cref="SetAsidePolicy.MaxDeadBytes"/> above the payload limit of the topic. Then settle the
/// delivery again.</item>
/// </list>
/// The same settle again gets the same refusal. The bound cannot hold this message.
/// </para>
/// <para>
/// This exception applies only to a row that joins the dead set. A message that is already a dead letter goes
/// back into the dead set with no error. An example is a replayed dead letter that failed out of its attempts or
/// was dead-lettered again. The set already holds that message, so to file it again adds no cost. A
/// refusal there would give a false reason. The replayed dead letter would then have no path back into the
/// store it came from.
/// </para>
/// </remarks>
public sealed class DeadLetterTooLargeException : ChimeraException
{
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic that the delivery came from.</param>
    /// <param name="subscriber">The subscriber that tried to file the dead letter.</param>
    /// <param name="size">The size of the message payload, in bytes.</param>
    /// <param name="limit">The dead-letter byte bound of the subscriber.</param>
    public DeadLetterTooLargeException(string topic, string subscriber, int size, long limit)
        : base($"Subscriber '{subscriber}' cannot file a {size}-byte dead letter on topic '{topic}'. Its " +
               $"dead-letter bound is {limit} bytes. The subscriber drops the oldest dead letters to stay inside " +
               "the bound. A dead letter larger than the bound is destroyed by the next trim. To " +
               "discard the message, complete the delivery. To keep it, raise MaxDeadBytes above the topic's " +
               "payload limit.")
    {
        Topic = topic;
        Subscriber = subscriber;
        Size = size;
        Limit = limit;
    }

    /// <summary>The topic that the delivery came from.</summary>
    public string Topic { get; }

    /// <summary>The subscriber that tried to file the dead letter.</summary>
    public string Subscriber { get; }

    /// <summary>The size of the message payload, in bytes.</summary>
    public int Size { get; }

    /// <summary>The dead-letter byte bound of the subscriber. The message by itself is larger than this bound.</summary>
    public long Limit { get; }
}

/// <summary>
/// The topic refused a scheduled message. Its scheduled table is at the bound that the retention policy sets.
/// </summary>
/// <remarks>
/// <para>
/// This exception is back-pressure at publish. A scheduled message is work that somebody asked for at a future
/// time. Removal of that message to stay under a size bound would cancel future work. Thus the topic refuses
/// the new message.
/// </para>
/// <para>
/// Service Bus does the same for the same reason. In Service Bus, scheduled messages count against the entity
/// quota, and Service Bus refuses the send. The same events clear the bound in both:
/// <list type="bullet">
/// <item>Due times arrive and subscribers consume the messages.</item>
/// <item>Somebody cancels messages that are no longer necessary.</item>
/// </list>
/// </para>
/// <para>
/// <see cref="Scheduled"/> and <see cref="Bytes"/> count the messages that wait for a due time. The bound
/// applies to these messages. <c>RetentionPolicy.MaxAge</c> holds the rows that are past due. Thus this bound
/// never refuses a schedule for now.
/// </para>
/// </remarks>
public sealed class ScheduleRefusedException : ChimeraException
{
    /// <summary>Creates the exception with a message built from its values.</summary>
    /// <param name="topic">The topic that the message was scheduled on.</param>
    /// <param name="scheduled">The number of scheduled messages in the topic that wait for a due time.</param>
    /// <param name="bytes">The total payload bytes of those messages.</param>
    /// <param name="limit">The limit that was reached, in the units of <paramref name="bound"/>.</param>
    /// <param name="bound">The bound that was reached.</param>
    public ScheduleRefusedException(string topic, long scheduled, long bytes, long limit, SizeBound bound)
        : base(bound == SizeBound.Bytes
            ? $"Topic '{topic}' holds {bytes} bytes of scheduled messages that are not yet due. Its bound is " +
              $"{limit} bytes. Retention removes a scheduled message only after its due time. Consume due " +
              "messages or cancel scheduled ones, then schedule again."
            : $"Topic '{topic}' holds {scheduled} scheduled message(s) that are not yet due. Its bound is " +
              $"{limit}. Retention removes a scheduled message only after its due time. Consume due messages " +
              "or cancel scheduled ones, then schedule again.")
    {
        Topic = topic;
        Scheduled = scheduled;
        Bytes = bytes;
        Limit = limit;
        Bound = bound;
    }

    /// <summary>The topic that the message was scheduled on.</summary>
    public string Topic { get; }

    /// <summary>
    /// The number of scheduled messages in the topic that wait for a due time in the future.
    /// </summary>
    /// <remarks>
    /// The count holds only rows with a future due time. Rows that are past due are work on the way out. The
    /// age bound removes them.
    /// </remarks>
    public long Scheduled { get; }

    /// <summary>The total payload bytes of those messages.</summary>
    public long Bytes { get; }

    /// <summary>The limit that was reached, in the units of <see cref="Bound"/>.</summary>
    public long Limit { get; }

    /// <summary>The bound that was reached: the number of scheduled messages or their total bytes.</summary>
    public SizeBound Bound { get; }
}
