namespace SoEx.Messaging.Chimera;

/// <summary>
/// The publisher's end of one topic. Written by exactly one instance in one process: that is the invariant the
/// whole design rests on, and it is why no lease or visibility timeout exists anywhere in this library.
/// </summary>
public interface ITopicLog : IAsyncDisposable
{
    /// <summary>The topic this log holds.</summary>
    string Topic { get; }

    /// <summary>
    /// Appends a message. The returned task completes only once the record is durable, so a caller that awaits
    /// it and then crashes knows the message survived.
    /// </summary>
    /// <returns>The log sequence, for diagnostics.</returns>
    ValueTask<long> AppendAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default);

    /// <summary>
    /// Stores a message to become deliverable at <paramref name="dueAt"/>. It is never moved into the log:
    /// subscribers read the scheduled table themselves and pick it up, so there is no timer per message.
    /// Delivery is no earlier than the due time, and no promise is made about how much later.
    /// </summary>
    /// <remarks>
    /// A due time already in the past means due now, and is recorded as now rather than as the time asked for.
    /// Service Bus does the same with a scheduled enqueue time that has passed. It also matters here: a
    /// subscriber's place in the scheduled table is a due time, so a message arriving with a due time behind
    /// one already delivered would have nowhere to go.
    /// </remarks>
    /// <returns>An id that can cancel the message before it is picked up.</returns>
    ValueTask<long> ScheduleAsync(ReadOnlyMemory<byte> payload, DateTimeOffset dueAt,
        CancellationToken cancellationToken = default);

    /// <summary>Removes a scheduled message. False if it was already removed.</summary>
    /// <remarks>
    /// <para>
    /// True means the row was there and is now gone, not that the message was caught in time. A delivered
    /// message keeps its row — fan-out means no one subscriber may remove it, and only retention does — so
    /// cancelling one that has already been handled removes the row and returns true. There is nothing here
    /// that could tell the difference: whether it was delivered is each subscriber's own business, held in
    /// each subscriber's own file.
    /// </para>
    /// <para>
    /// Cancelling a message a subscriber is in the middle of failing is reported to that subscriber as loss.
    /// It has an entry saying the message reached it and is still owed another attempt — that is what a failed
    /// delivery below the attempt limit leaves — and the row that attempt would have come from has gone, which
    /// is <see cref="MessagesLostException"/> with <c>Scheduled</c> set. Nothing in the file distinguishes a
    /// cancellation from a retention removal, and the loud reading is the safe one: a redelivery was promised
    /// to a handler and is not going to happen. Cancel a message that may already be in delivery only if that
    /// report is one the consumer can live with.
    /// </para>
    /// </remarks>
    ValueTask<bool> CancelScheduledAsync(long scheduledId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Where every subscriber of this topic has got to, read from their own files without disturbing them.
    /// </summary>
    /// <remarks>
    /// This is a read, and only a read. Truncation is never gated on it: waiting for the slowest subscriber
    /// would turn bounded loss into unbounded growth landing on the publisher, let one stalled subscriber stop
    /// every append, and need an expiry timer for abandoned subscribers that would quietly make the same
    /// decision somewhere less visible. Knowing who is about to lose data is worth having; letting them hold
    /// the publisher hostage is not.
    /// </remarks>
    ValueTask<IReadOnlyList<SubscriberPosition>> InspectSubscribersAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies the retention policy, removing records that have aged or overflowed. Returns what it removed so a
    /// caller can report it: for the scheduled table this is the only place a removal can be noticed at all,
    /// since a subscriber has no contiguous sequence to spot a gap in.
    /// </summary>
    ValueTask<RetentionReport> ApplyRetentionAsync(CancellationToken cancellationToken = default);
}

/// <summary>What one retention pass removed.</summary>
/// <param name="LogRecordsRemoved">Log records removed.</param>
/// <param name="HighestSequenceRemoved">Highest log sequence removed, or 0 if none.</param>
/// <param name="ScheduledRemoved">
/// Scheduled rows removed, whether or not anybody consumed them first. Report these: a scheduled message that
/// aged out unread leaves nothing downstream to notice it by, since a subscriber has no contiguous sequence to
/// spot a gap in. The count is an upper bound on what was missed, not a measure of it — consumption is recorded
/// in each subscriber's own file and a delete cannot see it.
/// </param>
/// <param name="Overtaken">
/// Subscribers that had not reached the records this pass removed, and how many each lost. They will notice on
/// their next read, but by name and at the moment it happens is better than after the fact.
/// </param>
public readonly record struct RetentionReport(
    long LogRecordsRemoved,
    long HighestSequenceRemoved,
    long ScheduledRemoved,
    IReadOnlyList<SubscriberOvertaken> Overtaken)
{
    /// <summary>The pass removed at least one record from the log or the scheduled table.</summary>
    public bool RemovedAnything => LogRecordsRemoved > 0 || ScheduledRemoved > 0;

    /// <summary>Somebody lost messages they had not read. Worth a log line at warning, not information.</summary>
    public bool AnyoneOvertaken => Overtaken.Count > 0;
}

/// <summary>How far a subscriber has got through a topic.</summary>
/// <param name="Subscriber">The subscriber's name.</param>
/// <param name="Mark">Its position: the log sequence it has finished with everything up to.</param>
/// <param name="Behind">Messages between its position and the end of the log.</param>
public readonly record struct SubscriberPosition(string Subscriber, long Mark, long Behind);

/// <summary>A subscriber that a retention pass removed messages from before it had read them.</summary>
/// <param name="Subscriber">The subscriber's name.</param>
/// <param name="Mark">Its position when the pass ran.</param>
/// <param name="Lost">
/// Messages removed above this subscriber's position. Measured from the position, which is all a log can see
/// of a subscriber, so it counts a record whose delivery was out at the time and went on to be completed.
/// The subscriber's own <see cref="MessagesLostException"/> knows how each delivery ended and can be lower;
/// where the two disagree, it is the one to believe.
/// </param>
public readonly record struct SubscriberOvertaken(string Subscriber, long Mark, long Lost);
