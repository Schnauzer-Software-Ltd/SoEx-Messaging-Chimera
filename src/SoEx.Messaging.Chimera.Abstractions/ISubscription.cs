namespace SoEx.Messaging.Chimera;

/// <summary>
/// The start point of a new subscriber. It applies only to messages published before the subscriber existed.
/// </summary>
/// <remarks>
/// The two usual replacement destinations for this library differ here. Thus this library offers both choices.
/// A JetStream consumer created with DeliverAll replays all that the retained window holds. A Service Bus
/// subscription receives only the messages sent after its creation. Both treat the skipped messages as no
/// loss. This library does the same.
/// </remarks>
public enum StartPosition
{
    /// <summary>
    /// All that the topic still holds. This is the retained window, which can be shorter than the full history.
    /// </summary>
    /// <remarks>
    /// Records that retention removed before the subscriber existed are no loss to that subscriber. This is the
    /// JetStream DeliverAll option.
    /// </remarks>
    Beginning = 0,

    /// <summary>
    /// Only the messages that arrive from now on.
    /// </summary>
    /// <remarks>
    /// Scheduled messages that are already stored and not yet due arrive at their due time. A Service Bus
    /// subscription does the same with a message scheduled before the subscription was created.
    /// </remarks>
    Next = 1,
}

/// <summary>Settings for one subscriber that reads one topic.</summary>
public sealed record SubscriptionOptions
{
    /// <summary>
    /// The maximum number of in-flight deliveries. At this number, reading stops until a delivery is settled.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This limit is the backpressure. If a consumer never settles, it stops receiving messages. Thus no
    /// untracked backlog of work grows.
    /// </para>
    /// <para>
    /// The limit spans all sources: the log, the scheduled table and replayed messages. A separate limit for
    /// each source would give three times the configured concurrency.
    /// </para>
    /// </remarks>
    public int MaxInFlight { get; init; } = 16;

    /// <summary>
    /// The number of deliveries of one message before <see cref="ISubscription.FailAsync"/> dead-letters it.
    /// Below this number, <c>FailAsync</c> gives the message back for another attempt.
    /// </summary>
    /// <remarks>
    /// The next attempt follows with no delay. This library schedules no later delivery. A wait that must
    /// survive a restart belongs in a workflow.
    /// </remarks>
    public int MaxAttempts { get; init; } = 3;

    /// <summary>The time to wait before the next check when there is nothing to deliver.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    /// <summary>The number of rows read from the log in one operation.</summary>
    public int ReadBatchSize { get; init; } = 64;

    /// <summary>
    /// The start point of a subscriber that has not read this topic before.
    /// </summary>
    /// <remarks>
    /// This setting applies once, when the position is first recorded. After that, the stored position applies
    /// and this setting has no effect.
    /// </remarks>
    public StartPosition Start { get; init; } = StartPosition.Beginning;

    /// <summary>The bounds on the messages that this subscriber can set aside.</summary>
    public SetAsidePolicy SetAside { get; init; } = new();
}

/// <summary>How far behind a subscriber is, in the units of the retention window.</summary>
/// <param name="Behind">The number of messages between the position and the end of the log.</param>
/// <param name="OldestUnread">
/// The age of the oldest message that the subscriber has not finished with, or null if there is nothing to read.
/// Compare this value with the retention window. A count of messages cannot be compared with a policy in hours.
/// </param>
/// <param name="DueScheduled">
/// The number of due scheduled messages that this subscriber has not finished with. This count is separate from
/// <paramref name="Behind"/>, because different bounds hold the two tables. Size, count and age bound the log.
/// Age alone bounds the scheduled table. A sum would hide which table grows. This count exists because
/// <see cref="RetentionPolicy.MaxAge"/> removes rows from both tables. Retention overtakes a subscriber on the
/// scheduled side in the same way as on the log side.
/// </param>
public readonly record struct SubscriptionLag(long Behind, TimeSpan? OldestUnread, long DueScheduled);

/// <summary>
/// One subscriber that reads one topic.
/// </summary>
/// <remarks>
/// This is the same unit as a Service Bus processor or a JetStream consumer, for the same reason. It is the unit
/// that has a position, a concurrency setting and a lifecycle.
/// </remarks>
public interface ISubscription : IAsyncDisposable
{
    /// <summary>The subscriber name of this subscription. The position and the set-aside messages are stored under this name.</summary>
    string Subscriber { get; }

    /// <summary>The topic that this subscription reads.</summary>
    string Topic { get; }

    /// <summary>
    /// Gives the messages to handle, in no guaranteed order.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Append order is total and stable. Delivery order can differ from append order. A failed message comes
    /// back after messages delivered later, and handlers run concurrently. Service Bus and JetStream have the
    /// same rule. Thus code written against this interface needs no change after a move.
    /// </para>
    /// <para>
    /// Settle each delivery: complete, fail, defer or dead-letter it. An unsettled delivery holds one in-flight
    /// slot until this subscription closes. When a subscription closes with deliveries in flight, it gives them
    /// back. They go out again as further attempts, as after a stop of the process.
    /// </para>
    /// </remarks>
    IAsyncEnumerable<EventEnvelope> ReadAsync(CancellationToken cancellationToken = default);

    /// <summary>Settles the delivery as handled. The position moves past it when all earlier deliveries are settled too.</summary>
    ValueTask CompleteAsync(EventEnvelope envelope, CancellationToken cancellationToken = default);

    /// <summary>
    /// Settles a delivery whose handler threw. At <see cref="SubscriptionOptions.MaxAttempts"/>, the message
    /// becomes a dead letter. Below it, the message goes back for another attempt.
    /// </summary>
    /// <remarks>Redelivery is immediate.</remarks>
    /// <returns>
    /// True if the message will be delivered again. False if the message will not be delivered again. In that
    /// case, the message is now a dead letter, or retention removed its source record while you held the
    /// delivery. The caller cannot check for the second case. The library counts a message lost in that way. A
    /// later read reports it through <see cref="MessagesLostException"/>.
    /// </returns>
    ValueTask<bool> FailAsync(EventEnvelope envelope, string reason, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves the message into this subscriber's set-aside store as a deferred message. It stays there until
    /// <see cref="ReplayAsync"/> asks for it.
    /// </summary>
    /// <remarks>
    /// Only a replay brings the message back. This is by design: the caller decides when. If that decision must
    /// survive a restart, put it in a workflow.
    /// </remarks>
    ValueTask DeferAsync(EventEnvelope envelope, string? reason = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Settles the delivery as a failure that is final. The dead letter keeps the payload, so you can replay it
    /// after the log moves on.
    /// </summary>
    ValueTask DeadLetterAsync(EventEnvelope envelope, string reason, CancellationToken cancellationToken = default);

    /// <summary>The deferred messages of this subscriber. The list is a view of this subscriber's set-aside store.</summary>
    ValueTask<IReadOnlyList<SetAsideEntry>> DeferredAsync(CancellationToken cancellationToken = default);

    /// <summary>The dead letters of this subscriber. The list is a view of this subscriber's set-aside store.</summary>
    ValueTask<IReadOnlyList<SetAsideEntry>> DeadLettersAsync(CancellationToken cancellationToken = default);

    /// <summary>Makes a set-aside message deliverable again from its stored copy.</summary>
    /// <returns>
    /// True if the row was marked for delivery. True describes the store at that time. It does not promise a
    /// delivery, because nothing has read the row yet. Under <see cref="DeadLetterOverflow.DropOldest"/>, a
    /// bound reached before the read can still remove the row. Retention can remove a record between an append
    /// and a read in the same way. False if there is no such row, or if a handler has the row now.
    /// </returns>
    ValueTask<bool> ReplayAsync(long asideId, CancellationToken cancellationToken = default);

    /// <summary>Destroys a set-aside message. This is the only intentional loss in this library.</summary>
    ValueTask<bool> DiscardAsync(long asideId, CancellationToken cancellationToken = default);

    /// <summary>
    /// How far behind this subscriber is. Use it to raise an alarm before retention overtakes the subscriber.
    /// </summary>
    /// <remarks>
    /// The lag counts both tables, because the age bound also applies to the scheduled table. A value for the log
    /// alone would report on only half of the risk.
    /// </remarks>
    ValueTask<SubscriptionLag> LagAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Ages out deferred messages and enforces the dead-letter bound.
    /// </summary>
    /// <remarks>
    /// This is a maintenance pass. No operation waits for it. The host calls it, as the host applies retention
    /// to a log.
    /// </remarks>
    ValueTask<SetAsideReport> ApplyRetentionAsync(CancellationToken cancellationToken = default);
}
