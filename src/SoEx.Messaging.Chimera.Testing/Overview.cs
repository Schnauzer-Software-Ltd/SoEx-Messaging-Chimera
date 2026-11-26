namespace SoEx.Messaging.Chimera.Testing;

/// <summary>
/// The state of one subscriber for one topic, at the time of the read.
/// </summary>
/// <param name="Subscriber">The name of the subscriber.</param>
/// <param name="Topic">The topic that the subscriber reads.</param>
/// <param name="Behind">
/// The number of published log records that this subscriber has not settled. It includes the records that the
/// subscriber holds now and the records not yet delivered. The position moves past a record only after the
/// record is settled. <see cref="ISubscription.LagAsync"/> reports the same number.
/// </param>
/// <param name="DueScheduled">The number of due scheduled messages that this subscriber has not settled.</param>
/// <param name="InFlight">
/// The number of unsettled delivered messages. A handler runs, or it stopped without settling. This
/// count is a subset of <see cref="Behind"/> and <see cref="DueScheduled"/>. It shows which subscriber is stuck
/// on which message when a test does not settle.
/// </param>
/// <param name="AwaitingRetry">
/// The number of failed deliveries that wait to go out again. This count is also a subset of
/// <see cref="Behind"/> and <see cref="DueScheduled"/>.
/// </param>
/// <param name="AwaitingReplay">
/// The number of set-aside messages with a replay request that are unsettled. Each one waits to go out, or a
/// handler has it now. <see cref="SubscriberActivity.Settled"/> counts both as work in motion. A count of the
/// waiting messages only would show a subscriber as quiet while a handler had a replayed message.
/// </param>
/// <param name="Deferred">The number of deferred messages. Each one stays out of delivery until this subscriber requests it.</param>
/// <param name="DeadLettered">The number of dead letters for this subscriber. Each dead letter keeps its payload.</param>
/// <param name="Stranded">
/// True if this position belongs to a different incarnation of the log. The subscriber refuses to read. The
/// numbers above have no meaning for the current log.
/// </param>
public sealed record SubscriberActivity(
    string Subscriber,
    string Topic,
    long Behind,
    long DueScheduled,
    long InFlight,
    long AwaitingRetry,
    long AwaitingReplay,
    long Deferred,
    long DeadLettered,
    bool Stranded)
{
    /// <summary>
    /// True if this subscriber has no work in motion.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Deferred messages and dead letters have no effect on this value, by design. Both are parked on purpose,
    /// and both stay parked until a request. A system that holds them is quiet. A quiet system can be unhealthy.
    /// Assert on health separately. With this rule, a test whose topology dead-letters every message fails with
    /// a count and a reason. With parked messages included, the same test would wait for a timeout.
    /// </para>
    /// <para>
    /// The in-flight and awaiting-retry terms are redundant, because a message in either state is also above a
    /// position and in that count. The terms stay on purpose. If a position moved past an unsettled message,
    /// this value reports unsettled and disagrees with the incorrect position.
    /// </para>
    /// </remarks>
    public bool Settled =>
        !Stranded && Behind == 0 && DueScheduled == 0 && InFlight == 0 && AwaitingRetry == 0
        && AwaitingReplay == 0;
}

/// <summary>The state of each subscriber under one root directory, at one time.</summary>
public sealed record ChimeraOverview(IReadOnlyList<SubscriberActivity> Subscribers)
{
    /// <summary>
    /// True if no subscriber under this root directory has work in motion.
    /// </summary>
    /// <remarks>
    /// This value is a snapshot. The system continues to run while the files are read. The value gives the
    /// state at the time of the read. A handler that settled its message and will publish the next message
    /// looks quiet for that instant. A test that waits for the system to come to rest must get this value
    /// twice in a row. If the binding publishes before it settles, the window closes fully. That order is the
    /// at-least-once order, and we recommend it.
    /// </remarks>
    public bool Settled => Subscribers.All(s => s.Settled);

    /// <summary>The number of dead letters for all subscribers. Assert on this value separately from <see cref="Settled"/>.</summary>
    /// <remarks>
    /// This value counts the dead letters held now. With <see cref="DeadLetterOverflow.DropOldest"/>, a dead
    /// letter that the bound destroyed leaves the set and this count. Zero means that no dead letter is held now.
    /// <c>SetAsideReport.DeadDropped</c> counts the dead letters that the bound destroyed.
    /// </remarks>
    public long DeadLettered => Subscribers.Sum(s => s.DeadLettered);

    /// <summary>Gets the state of one subscriber for one topic. If the subscriber does not read that topic, returns null.</summary>
    public SubscriberActivity? For(string subscriber, string topic) =>
        Subscribers.FirstOrDefault(s =>
            string.Equals(s.Subscriber, subscriber, StringComparison.Ordinal)
            && string.Equals(s.Topic, topic, StringComparison.Ordinal));
}

/// <summary>The state of one message for one subscriber.</summary>
/// <remarks>
/// <see cref="Completed"/> and <see cref="Unknown"/> come from the data that the files still hold. The two
/// ways to destroy a message also destroy the evidence of it. Each of the two values alone can include a loss,
/// and the summary of each tells how. To rule out loss, a test reads the report from
/// <see cref="ISubscription.ApplyRetentionAsync"/>. That report durably counts each drop and each removal by
/// age. Alternatively, do not use <see cref="DeadLetterOverflow.DropOldest"/>. It is not the default.
/// </remarks>
public enum MessageState
{
    /// <summary>
    /// The topic has no record with that id. The message was never published, or retention removed the record.
    /// This includes the record of a message that this subscriber completed long ago. The answer starts from
    /// the topic file. If the record is gone, the answer is Unknown, whatever the subscriber file holds.
    /// </summary>
    Unknown = 0,

    /// <summary>Published and due. This subscriber has not received it yet.</summary>
    Pending = 1,

    /// <summary>Scheduled. Its due time is in the future.</summary>
    NotYetDue = 2,

    /// <summary>Delivered and unsettled. A handler runs, or it stopped without settling.</summary>
    InFlight = 3,

    /// <summary>Returned after a failure. It goes out again on the next read.</summary>
    AwaitingRetry = 4,

    /// <summary>
    /// Settled successfully, or destroyed by a dead-letter bound. A drop removes the same records as a
    /// completion. After a drop, the two states look the same. <c>SetAsideReport.DeadDropped</c> counts the
    /// drop. A test reads drops there.
    /// </summary>
    Completed = 5,

    /// <summary>Deferred. It stays out of delivery until this subscriber requests it.</summary>
    Deferred = 6,

    /// <summary>A dead letter. The payload stays in the file, so a replay is possible.</summary>
    DeadLettered = 7,
}

/// <summary>The state of one message for one subscriber.</summary>
/// <param name="MessageId">The id of the message.</param>
/// <param name="Topic">The topic that the message was published to.</param>
/// <param name="Subscriber">The subscriber that this status is for.</param>
/// <param name="State">The state of the message for this subscriber.</param>
/// <param name="Attempts">
/// The number of deliveries that the files still record. A successful message reports zero. When the position
/// absorbs the message, the row with its attempt count is deleted. This keeps the subscriber file bounded as
/// the log grows. To get the attempt history of a successful message, a test must watch the deliveries as
/// they occur. Each envelope carries its attempt number.
/// </param>
/// <param name="Reason">The reason for the deferral or the dead letter, if one occurred.</param>
/// <param name="UpdatedAt">The time of the last action by this subscriber on the message, if one occurred.</param>
public sealed record MessageStatus(
    string MessageId,
    string Topic,
    string Subscriber,
    MessageState State,
    long Attempts,
    string? Reason,
    DateTimeOffset? UpdatedAt);
