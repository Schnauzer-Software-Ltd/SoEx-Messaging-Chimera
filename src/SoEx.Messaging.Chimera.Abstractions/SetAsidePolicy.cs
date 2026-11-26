namespace SoEx.Messaging.Chimera;

/// <summary>The action when the dead-letter set is full. These two values are the only options. See the remarks.</summary>
/// <remarks>
/// At each other bound, this library moves the message to a place where it stays visible. The dead set is the
/// last place. There is no further place to move a message to. Thus the options are to stop or to drop. Stop is
/// the default. A stopped subscriber keeps its position. The publisher and the other subscribers continue. A
/// stop delays loss. If the subscriber stays stopped, it eventually falls out of the retention window of the
/// log. That failure is slow, visible, and attributable to one component.
/// </remarks>
public enum DeadLetterOverflow
{
    /// <summary>Stops deliveries to this subscriber until somebody handles the dead letters.</summary>
    StopDelivering = 0,

    /// <summary>
    /// Destroys the oldest dead letters to make room. The loss is bounded. Only the drop count shows it.
    /// </summary>
    /// <remarks>
    /// This setting keeps only the row that a handler holds, because the library reports the result to that
    /// handler. All other rows can be destroyed. This includes a replayed message that is not yet delivered. A
    /// replay marks a row for delivery. A bound reached before that delivery still takes the oldest row. If that
    /// is important to you, count <c>SetAsideReport.DeadDropped</c>, or use <see cref="StopDelivering"/>.
    /// <see cref="StopDelivering"/> destroys nothing.
    /// </remarks>
    DropOldest = 1,
}

/// <summary>
/// The bounds on what one subscriber can set aside.
/// </summary>
/// <remarks>
/// Set-aside rows hold copies of their payloads. Without these bounds, frequent deferrals fill the disk.
/// </remarks>
public sealed record SetAsidePolicy
{
    /// <summary>
    /// The maximum time that a message stays deferred. After this time, the message becomes a dead letter.
    /// </summary>
    /// <remarks>
    /// The time counts from the set-aside time, not from the publish time. This is the meaning that a reader of
    /// <c>MaxDeferredAge</c> expects. No handler depends on the start point. Only an operator does.
    /// </remarks>
    public TimeSpan? MaxDeferredAge { get; init; } = TimeSpan.FromDays(7);

    /// <summary>The maximum number of deferred messages at one time. Above it, a deferral dead-letters the message.</summary>
    public long? MaxDeferredCount { get; init; } = 10_000;

    /// <summary>The maximum number of dead letters. At this bound, <see cref="DeadOverflow"/> sets what happens.</summary>
    public long? MaxDeadCount { get; init; } = 10_000;

    /// <summary>
    /// The maximum stored payload bytes of the deferred messages of this subscriber. Above it, a deferral
    /// dead-letters the message, as above <see cref="MaxDeferredCount"/>.
    /// </summary>
    public long? MaxDeferredBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// The maximum stored payload bytes of the dead letters of this subscriber. Above it,
    /// <see cref="DeadOverflow"/> sets what happens, as above <see cref="MaxDeadCount"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// There are two byte bounds, because the deferred set and the dead set respond differently to a bound. A
    /// deferral that does not fit moves to the dead set. Thus the deferred bound works by refusing growth of the
    /// deferred set. The dead set is the last place. Its only options are to stop the subscriber or to destroy
    /// a message. <see cref="DeadOverflow"/> makes that choice. One number for both sets could apply only one of
    /// these two meanings. In use, it applied the deferred meaning. The dead set then grew past the bound, one
    /// payload at a time, while the option stated a bound.
    /// </para>
    /// <para>
    /// Under <see cref="DeadLetterOverflow.DropOldest"/>, a single dead letter larger than this bound cannot be
    /// kept. A settle never destroys the row that it files. To keep this row, the subscriber would destroy all
    /// its other dead letters and still be over the bound. Thus the subscriber refuses to file it
    /// and throws <c>DeadLetterTooLargeException</c>. The set stays as it is. The exception states that the bound
    /// cannot hold this message. Set this bound above the payload limit of the topic. Under
    /// <see cref="DeadLetterOverflow.StopDelivering"/>, nothing is destroyed or refused.
    /// </para>
    /// </remarks>
    public long? MaxDeadBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>The action at a full dead-letter bound. The default is <see cref="DeadLetterOverflow.StopDelivering"/>.</summary>
    public DeadLetterOverflow DeadOverflow { get; init; } = DeadLetterOverflow.StopDelivering;
}

/// <summary>What one pass over the set-aside store of a subscriber did.</summary>
/// <param name="DeferredAgedOut">The number of deferred messages that became dead letters because they stayed
/// deferred too long. The count includes this pass and each earlier pass whose report reached nobody. The count
/// is stored in the subscriber file next to the changed rows, as is the drop count. A pass makes several writes.
/// If a pass failed after the age-out committed, the count would be lost. The messages are dead by then, and no
/// later pass can calculate the count.</param>
/// <param name="DeadDropped">The number of dead letters destroyed to stay within the bound. The count includes
/// this pass and each settle since the last pass. It is above zero only under
/// <see cref="DeadLetterOverflow.DropOldest"/>. Raise an alarm on it, because it is real loss. A new dead letter
/// can cause it as often as a maintenance pass can. The count is stored in the subscriber file next to the
/// destroyed rows. Thus the count survives a close of the subscription or a loss of the process. The first pass
/// that asks gets the count, once. The loss appears only in this count. If a subscriber runs no pass, it never
/// learns of the loss. That is the trade-off of the drop-oldest setting.</param>
public readonly record struct SetAsideReport(long DeferredAgedOut, long DeadDropped)
{
    /// <summary>True if the pass aged out or dropped at least one message.</summary>
    public bool DidAnything => DeferredAgedOut > 0 || DeadDropped > 0;
}
