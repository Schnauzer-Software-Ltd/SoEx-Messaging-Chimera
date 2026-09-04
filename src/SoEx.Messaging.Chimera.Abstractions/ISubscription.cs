namespace SoEx.Messaging.Chimera;

/// <summary>
/// Where a brand-new subscriber begins. It decides one thing only: what a subscriber reading a topic for the
/// first time does about messages that were published before it existed.
/// </summary>
/// <remarks>
/// The two destinations this library is meant to be swapped for disagree here, so both are offered rather than
/// one being picked for you. A JetStream consumer created with DeliverAll replays whatever the retained window
/// still holds; a Service Bus subscription receives nothing sent before it was created. Neither treats the
/// messages it does not get as loss, and neither does this.
/// </remarks>
public enum StartPosition
{
    /// <summary>
    /// Everything the topic still holds, which is the retained window and not the whole history: records
    /// retention has already removed are not loss to a subscriber that did not exist when they were there.
    /// JetStream's DeliverAll.
    /// </summary>
    Beginning = 0,

    /// <summary>
    /// Only what arrives from now on. Scheduled messages already stored but not yet due still arrive when they
    /// fall due, exactly as a Service Bus subscription receives a message scheduled before it was created.
    /// </summary>
    Next = 1,
}

/// <summary>Settings for one subscriber reading one topic.</summary>
public sealed record SubscriptionOptions
{
    /// <summary>
    /// How many deliveries may be outstanding at once. Reading stops at this number until something is settled,
    /// which is the backpressure: a consumer that never settles stops receiving rather than growing a backlog
    /// of work nobody is tracking.
    /// </summary>
    /// <remarks>
    /// It spans every source. Counting the log, the scheduled table and replayed messages separately would give
    /// three times the concurrency someone configured.
    /// </remarks>
    public int MaxInFlight { get; init; } = 16;

    /// <summary>
    /// Deliveries of one message before <see cref="ISubscription.FailAsync"/> dead-letters it instead of
    /// handing it back. There is no delay between attempts: nothing here schedules a later delivery, and a wait
    /// that has to survive a restart is a workflow, not a queue.
    /// </summary>
    public int MaxAttempts { get; init; } = 3;

    /// <summary>How long to wait before looking again when there is nothing to deliver.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Rows read from the log in one go.</summary>
    public int ReadBatchSize { get; init; } = 64;

    /// <summary>
    /// Where a subscriber that has never read this topic before starts. It applies once, when the position is
    /// first recorded; after that the stored position is what matters and this setting is ignored.
    /// </summary>
    public StartPosition Start { get; init; } = StartPosition.Beginning;

    /// <summary>Bounds on what this subscriber may set aside.</summary>
    public SetAsidePolicy SetAside { get; init; } = new();
}

/// <summary>How far behind a subscriber is, in the units the retention window is set in.</summary>
/// <param name="Behind">Messages between the position and the end of the log.</param>
/// <param name="OldestUnread">
/// Age of the oldest message not yet dealt with, or null when there is nothing to read. Compare this with the
/// retention window: a count of messages cannot be compared with a policy written in hours.
/// </param>
public readonly record struct SubscriptionLag(long Behind, TimeSpan? OldestUnread);

/// <summary>
/// One subscriber reading one topic — the same granularity as a Service Bus processor or a JetStream consumer,
/// and for the same reason: it is the unit that has a position, a concurrency setting and a lifecycle.
/// </summary>
public interface ISubscription : IAsyncDisposable
{
    string Subscriber { get; }
    string Topic { get; }

    /// <summary>
    /// Messages to handle, in no guaranteed order. Append order is total and stable; delivery order is not,
    /// because a failed message comes back after ones delivered later and handlers run concurrently. Service
    /// Bus and JetStream say the same, so nothing written against this needs to change when it moves.
    /// </summary>
    /// <remarks>
    /// Every delivery must be settled — completed, failed, deferred or dead-lettered — or it occupies one of
    /// the in-flight slots until this subscription is closed. Closing one that is still holding deliveries
    /// hands them back rather than stranding them: they go out again, counted as further attempts, exactly as
    /// they would after the process had stopped.
    /// </remarks>
    IAsyncEnumerable<EventEnvelope> ReadAsync(CancellationToken cancellationToken = default);

    /// <summary>Handled. The position absorbs it once everything before it is settled too.</summary>
    ValueTask CompleteAsync(EventEnvelope envelope, CancellationToken cancellationToken = default);

    /// <summary>
    /// Handling threw. Hands the message back for another attempt, or dead-letters it once
    /// <see cref="SubscriptionOptions.MaxAttempts"/> is reached. Redelivery is immediate.
    /// </summary>
    /// <returns>True if the message will be delivered again; false if it is now a dead letter.</returns>
    ValueTask<bool> FailAsync(EventEnvelope envelope, string reason, CancellationToken cancellationToken = default);

    /// <summary>
    /// Not now. Takes the message out of the flow into this subscriber's own store, where it stays until
    /// <see cref="ReplayAsync"/> asks for it. Nothing brings it back on its own, deliberately: deciding when is
    /// the caller's job, and if that decision has to survive a restart it belongs in a workflow.
    /// </summary>
    ValueTask DeferAsync(EventEnvelope envelope, string? reason = null, CancellationToken cancellationToken = default);

    /// <summary>Done with, unsuccessfully. Keeps the payload so it stays replayable after the log has moved on.</summary>
    ValueTask DeadLetterAsync(EventEnvelope envelope, string reason, CancellationToken cancellationToken = default);

    /// <summary>What this subscriber has set aside. A view over its own store, not a copy in another topic.</summary>
    ValueTask<IReadOnlyList<SetAsideEntry>> DeferredAsync(CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<SetAsideEntry>> DeadLettersAsync(CancellationToken cancellationToken = default);

    /// <summary>Makes a set-aside message deliverable again, from its own stored copy.</summary>
    ValueTask<bool> ReplayAsync(long asideId, CancellationToken cancellationToken = default);

    /// <summary>Destroys a set-aside message. The only deliberate loss in this library.</summary>
    ValueTask<bool> DiscardAsync(long asideId, CancellationToken cancellationToken = default);

    /// <summary>How far behind this subscriber is, for alarming before retention overtakes it.</summary>
    ValueTask<SubscriptionLag> LagAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Ages out deferred messages and enforces the dead-letter bound. A maintenance pass, not a scheduler:
    /// nothing waits on it, and calling it is the host's business, exactly like applying retention to a log.
    /// </summary>
    ValueTask<SetAsideReport> ApplyRetentionAsync(CancellationToken cancellationToken = default);
}
