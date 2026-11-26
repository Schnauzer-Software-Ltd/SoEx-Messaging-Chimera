namespace SoEx.Messaging.Chimera;

/// <summary>
/// The publisher end of one topic.
/// </summary>
/// <remarks>
/// One instance in one process writes the topic. The design rests on this invariant. For this reason, this
/// library uses no lease and no visibility timeout.
/// </remarks>
public interface ITopicLog : IAsyncDisposable
{
    /// <summary>The topic that this log holds.</summary>
    string Topic { get; }

    /// <summary>
    /// Appends a message. The returned task completes when the record is durable.
    /// </summary>
    /// <remarks>
    /// If a caller awaits the task and then crashes, the message is safe.
    /// </remarks>
    /// <returns>The log sequence, for diagnostics.</returns>
    ValueTask<long> AppendAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default);

    /// <summary>
    /// Stores a message that becomes deliverable at <paramref name="dueAt"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The message stays in the scheduled table. Subscribers read the scheduled table directly, so each message
    /// needs no timer. Delivery occurs at or after the due time. The library sets no limit on the delay.
    /// </para>
    /// <para>
    /// A due time in the past means due now. The library records the current time as the due time. Service Bus
    /// does the same with a scheduled enqueue time in the past. Here the rule has a second cause. The position
    /// of a subscriber in the scheduled table is a due time. A message with a due time before an already
    /// delivered message would have no place.
    /// </para>
    /// </remarks>
    /// <returns>An id that can cancel the message before a subscriber reads it.</returns>
    ValueTask<long> ScheduleAsync(ReadOnlyMemory<byte> payload, DateTimeOffset dueAt,
        CancellationToken cancellationToken = default);

    /// <summary>Removes a scheduled message. Returns false if the row was already removed.</summary>
    /// <remarks>
    /// <para>
    /// True means that the row was there and is now removed. True does not mean that the cancel was in time. A
    /// delivered message keeps its row. With fan-out, no single subscriber can remove the row. Only retention
    /// removes it. Thus a cancel of a message that a subscriber already handled removes the row and returns
    /// true. The topic log cannot see the difference. Each subscriber records its deliveries in its subscriber
    /// file.
    /// </para>
    /// <para>
    /// If a subscriber fails the message at the time of the cancel, that subscriber reports the cancel as loss.
    /// Its file records that the message reached it and is owed another attempt. A failed delivery below the
    /// attempt limit leaves this record. The row for that attempt is gone. Thus the subscriber reports
    /// <see cref="MessagesLostException"/> with <c>Scheduled</c> set. The file cannot tell a cancel from a
    /// retention removal. The loud report is the safe one, because a handler was promised a redelivery that will
    /// not occur. Cancel a message that can already be in delivery only if the consumer accepts that report.
    /// </para>
    /// </remarks>
    ValueTask<bool> CancelScheduledAsync(long scheduledId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the position of each subscriber of this topic. It reads the subscriber files and changes nothing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method only reads. Truncation never waits for it. A wait for the slowest subscriber would cause
    /// these problems:
    /// <list type="bullet">
    /// <item>Bounded loss would become unbounded growth on the publisher.</item>
    /// <item>One stalled subscriber would stop all appends.</item>
    /// <item>Abandoned subscribers would need an expiry timer. That timer would make the same decision in a
    /// less visible place.</item>
    /// </list>
    /// </para>
    /// <para>
    /// This method shows which subscribers will lose data soon. Those subscribers cannot block the publisher.
    /// </para>
    /// </remarks>
    ValueTask<IReadOnlyList<SubscriberPosition>> InspectSubscribersAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies the retention policy: removes the records that are too old or over a bound. Returns a report of
    /// the removals.
    /// </summary>
    /// <remarks>
    /// Report the result. For the scheduled table, this report is the only place where a removal is visible. A
    /// subscriber has no contiguous sequence in the scheduled table in which to find a gap.
    /// </remarks>
    ValueTask<RetentionReport> ApplyRetentionAsync(CancellationToken cancellationToken = default);
}

/// <summary>What one retention pass removed.</summary>
/// <param name="LogRecordsRemoved">The number of log records removed.</param>
/// <param name="HighestSequenceRemoved">The highest log sequence removed, or 0 if none.</param>
/// <param name="ScheduledRemoved">
/// The number of scheduled rows removed, consumed or not. Report these. A subscriber has no contiguous sequence
/// in the scheduled table. Thus no subscriber sees the gap when an unread scheduled message ages out. This count
/// is an upper bound on the missed messages. Each subscriber records consumption in its subscriber file, and a
/// delete cannot see that file.
/// </param>
/// <param name="Overtaken">
/// The subscribers that had not reached the records that this pass removed, with the number that each lost.
/// Each subscriber finds the loss on its next read. This list gives the name of each subscriber at the time of
/// the loss.
/// </param>
public readonly record struct RetentionReport(
    long LogRecordsRemoved,
    long HighestSequenceRemoved,
    long ScheduledRemoved,
    IReadOnlyList<SubscriberOvertaken> Overtaken)
{
    /// <summary>True if the pass removed at least one record from the log or the scheduled table.</summary>
    public bool RemovedAnything => LogRecordsRemoved > 0 || ScheduledRemoved > 0;

    /// <summary>True if a subscriber lost messages that it had not read. Log this at warning level.</summary>
    public bool AnyoneOvertaken => Overtaken.Count > 0;
}

/// <summary>How far a subscriber is through a topic.</summary>
/// <param name="Subscriber">The name of the subscriber.</param>
/// <param name="Mark">The subscriber's position: the log sequence up to which it has finished with all messages.</param>
/// <param name="Behind">The number of messages between the position and the end of the log.</param>
public readonly record struct SubscriberPosition(string Subscriber, long Mark, long Behind);

/// <summary>A subscriber that lost messages to a retention pass before it read them.</summary>
/// <param name="Subscriber">The name of the subscriber.</param>
/// <param name="Mark">The subscriber's position when the pass ran.</param>
/// <param name="Lost">
/// The number of messages removed above this subscriber's position. The log sees only the position of a
/// subscriber. Thus this count includes a record that was in flight at that time and was then completed. The
/// <see cref="MessagesLostException"/> of the subscriber knows the result of each delivery, and its count can be
/// lower. If the two counts differ, use the count from <see cref="MessagesLostException"/>.
/// </param>
public readonly record struct SubscriberOvertaken(string Subscriber, long Mark, long Lost);
