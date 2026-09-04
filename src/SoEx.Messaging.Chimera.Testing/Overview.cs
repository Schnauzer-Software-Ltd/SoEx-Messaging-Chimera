namespace SoEx.Messaging.Chimera.Testing;

/// <summary>
/// What one subscriber is doing about one topic, at the moment it was read.
/// </summary>
/// <param name="Behind">
/// Log records published that this subscriber has not finished with. It counts what it is holding right now
/// as well as what it has not been handed, because the position only moves past a record once that record is
/// settled — the same number <see cref="ISubscription.LagAsync"/> reports.
/// </param>
/// <param name="DueScheduled">Scheduled messages that are due and that this subscriber has not settled.</param>
/// <param name="InFlight">
/// Deliveries handed out and not yet settled. A handler is running, or has stopped without settling. This is
/// a subset of <see cref="Behind"/> and <see cref="DueScheduled"/> rather than something in addition to them:
/// it is here to say who is stuck on what, which is the question asked when a test does not settle.
/// </param>
/// <param name="AwaitingRetry">
/// Deliveries handed back after a failure, waiting to go out again. Also a subset of the two counts above.
/// </param>
/// <param name="AwaitingReplay">
/// Set-aside messages someone has asked to be replayed and that are not finished with: waiting to go out, or
/// out with a handler now. Both are work in motion, which is what <see cref="SubscriberActivity.Settled"/>
/// needs to know; a count of only the ones still waiting would call a subscriber quiet while a replay was
/// being handled.
/// </param>
/// <param name="Deferred">Messages taken out of the flow until this subscriber asks for them.</param>
/// <param name="DeadLettered">Messages this subscriber gave up on, with their payloads kept.</param>
/// <param name="Stranded">
/// This position belongs to a different incarnation of the log than the one now present, so the subscriber
/// will refuse to read and none of the numbers above mean anything against the log that is there.
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
    /// Nothing here will move on its own.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Deferred and dead-lettered messages do not count against this, deliberately. Both were parked on
    /// purpose and neither is going anywhere until somebody asks, so a system holding them is quiet — it is
    /// not necessarily healthy, which is a different question and worth asserting separately. Folding them in
    /// would mean a test whose topology dead-letters everything waits for a timeout instead of failing with a
    /// count and a reason.
    /// </para>
    /// <para>
    /// The in-flight and awaiting-retry terms are redundant here, since anything in either state also sits
    /// above a position and is counted there. They are left in deliberately: if a position ever moved past
    /// something unsettled, this would report unsettled rather than agree with the position that lied.
    /// </para>
    /// </remarks>
    public bool Settled =>
        !Stranded && Behind == 0 && DueScheduled == 0 && InFlight == 0 && AwaitingRetry == 0
        && AwaitingReplay == 0;
}

/// <summary>Everything every subscriber under one root is doing, at one moment.</summary>
public sealed record ChimeraOverview(IReadOnlyList<SubscriberActivity> Subscribers)
{
    /// <summary>
    /// Nothing anywhere under this root will move on its own.
    /// </summary>
    /// <remarks>
    /// This is a snapshot taken while everything carries on running, not a barrier: it says what was true
    /// when the files were read. A handler that has settled its message and is about to publish what comes
    /// next looks quiet for that instant, so a test waiting for a system to come to rest should require this
    /// twice in a row rather than once. The window closes entirely if the binding publishes before it settles,
    /// which is the at-least-once shape and the one worth having anyway.
    /// </remarks>
    public bool Settled => Subscribers.All(s => s.Settled);

    /// <summary>Dead letters across every subscriber. Assert on this separately from <see cref="Settled"/>.</summary>
    public long DeadLettered => Subscribers.Sum(s => s.DeadLettered);

    /// <summary>What one subscriber is doing about one topic, or null if it does not read that topic.</summary>
    public SubscriberActivity? For(string subscriber, string topic) =>
        Subscribers.FirstOrDefault(s =>
            string.Equals(s.Subscriber, subscriber, StringComparison.Ordinal)
            && string.Equals(s.Topic, topic, StringComparison.Ordinal));
}

/// <summary>Where one message has got to, as far as one subscriber is concerned.</summary>
public enum MessageState
{
    /// <summary>This topic holds no message with that id. It was never published, or retention has removed it.</summary>
    Unknown = 0,

    /// <summary>Published and due, and this subscriber has not been handed it yet.</summary>
    Pending = 1,

    /// <summary>Scheduled, and its due time has not arrived.</summary>
    NotYetDue = 2,

    /// <summary>Handed out and not settled. A handler is running, or has stopped without settling.</summary>
    InFlight = 3,

    /// <summary>Handed back after a failure. It goes out again on the next read.</summary>
    AwaitingRetry = 4,

    /// <summary>Settled successfully.</summary>
    Completed = 5,

    /// <summary>Taken out of the flow until this subscriber asks for it.</summary>
    Deferred = 6,

    /// <summary>Given up on. The payload is kept, so it stays replayable.</summary>
    DeadLettered = 7,
}

/// <summary>What became of one message, for one subscriber.</summary>
/// <param name="Attempts">
/// Deliveries so far, as the files still record them. A message that succeeded reports zero: the row carrying
/// its attempt count is deleted when the position absorbs it, which is what keeps a subscriber's file bounded
/// rather than growing with the log. If a test needs the attempt history of a message that succeeded, it has
/// to watch the deliveries as they happen — every envelope carries its own attempt number.
/// </param>
/// <param name="Reason">Why it was deferred or dead-lettered, if it was.</param>
/// <param name="UpdatedAt">When this subscriber last did something about it, if it has.</param>
public sealed record MessageStatus(
    string MessageId,
    string Topic,
    string Subscriber,
    MessageState State,
    long Attempts,
    string? Reason,
    DateTimeOffset? UpdatedAt);
