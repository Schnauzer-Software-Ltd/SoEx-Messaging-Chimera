namespace SoEx.Messaging.Chimera;

/// <summary>What to do when the dead-letter set is full. There is no third option — see the remarks.</summary>
/// <remarks>
/// Everything else this library does when a bound is reached moves the message somewhere it can still be seen.
/// Dead is where that ends: there is nowhere further to pass it. So the choice is between stopping and
/// dropping, and stopping is the default because a subscriber that stops holds its position, does not block
/// the publisher, and does not affect any other subscriber. It is a delay rather than a rescue — a subscriber
/// that stays stopped will eventually fall off the log's retention window — but it is slow, visible, and
/// attributable to one component.
/// </remarks>
public enum DeadLetterOverflow
{
    /// <summary>Stop delivering to this subscriber until somebody deals with the dead letters.</summary>
    StopDelivering = 0,

    /// <summary>
    /// Destroy the oldest dead letters to make room. Bounded, silent unless the count is watched.
    /// </summary>
    /// <remarks>
    /// The only row this spares is one a handler has in its hands, because that handler is about to be told
    /// what happened to it. Everything else is eligible, including a message you have replayed and not yet
    /// been given: a replay marks a row for delivery, and a bound reached before that delivery happens still
    /// takes the oldest. Count <c>SetAsideReport.DeadDropped</c> if that matters to you, or use
    /// <see cref="StopDelivering"/>, which destroys nothing.
    /// </remarks>
    DropOldest = 1,
}

/// <summary>
/// Bounds on what one subscriber may set aside. Set-aside rows carry their own payload copies, so without
/// these a consumer that defers freely fills the disk.
/// </summary>
public sealed record SetAsidePolicy
{
    /// <summary>
    /// How long a deferred message may sit before it becomes a dead letter. Measured from when it was set
    /// aside, not from when it was published: <c>MaxDeferredAge</c> should mean what somebody reading it
    /// assumes, and no handler depends on which end it is measured from — only an operator does.
    /// </summary>
    public TimeSpan? MaxDeferredAge { get; init; } = TimeSpan.FromDays(7);

    /// <summary>Deferred messages allowed at once. Beyond this, deferring dead-letters instead.</summary>
    public long? MaxDeferredCount { get; init; } = 10_000;

    /// <summary>Dead letters allowed before <see cref="DeadOverflow"/> decides what happens.</summary>
    public long? MaxDeadCount { get; init; } = 10_000;

    /// <summary>
    /// Stored payload allowed across this subscriber's deferred messages. Beyond it, deferring dead-letters
    /// instead, exactly as it does beyond <see cref="MaxDeferredCount"/>.
    /// </summary>
    public long? MaxDeferredBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// Stored payload allowed across this subscriber's dead letters. Beyond it, <see cref="DeadOverflow"/>
    /// decides what happens, the same as beyond <see cref="MaxDeadCount"/>.
    /// </summary>
    /// <remarks>
    /// There are two byte bounds rather than one because the two sets answer a bound differently and a single
    /// number could only honour one of them. A deferral that does not fit is passed along to the dead set,
    /// which is why the deferred bound can be enforced by refusing to grow that set. Dead is where passing
    /// along stops, so the only answers there are to stop the subscriber or to destroy something — which is
    /// exactly the choice <see cref="DeadOverflow"/> already exists to make. One number spanning both would
    /// have to mean both things at once, and the one it meant in practice was the deferred one: the dead set
    /// grew past it unchecked, one payload at a time, while the option said it was bounded.
    /// <para>
    /// Under <see cref="DeadLetterOverflow.DropOldest"/> a single dead letter larger than this whole bound
    /// cannot be kept at all: a settle never destroys the row it is filing, so keeping this one means
    /// destroying every other dead letter the subscriber holds and still sitting over the bound. Filing one is
    /// refused with <c>DeadLetterTooLargeException</c> rather than emptying the set to no purpose, which is a
    /// bound saying plainly that it cannot hold this message. Size the bound above the topic's
    /// payload limit; under <see cref="DeadLetterOverflow.StopDelivering"/> nothing is destroyed and nothing
    /// is refused.
    /// </para>
    /// </remarks>
    public long? MaxDeadBytes { get; init; } = 64L * 1024 * 1024;

    public DeadLetterOverflow DeadOverflow { get; init; } = DeadLetterOverflow.StopDelivering;
}

/// <summary>What one pass over a subscriber's set-aside store did.</summary>
/// <param name="DeferredAgedOut">Deferred messages that became dead letters because they sat too long.</param>
/// <param name="DeadDropped">Dead letters destroyed to stay within the bound, by this pass and by any settle
/// since the last one. Only ever non-zero under <see cref="DeadLetterOverflow.DropOldest"/>, and worth
/// alarming on: it is real loss, and a dead letter arriving is as likely to cause it as a maintenance pass.
/// The count is kept in the subscriber's file beside the rows it destroyed, so closing the subscription or
/// losing the process does not lose it; it is reported once, to whichever pass asks first. Counted rather
/// than raised, so a subscriber that never runs a pass never learns of it — which is the trade the
/// drop-oldest setting is.</param>
public readonly record struct SetAsideReport(long DeferredAgedOut, long DeadDropped)
{
    public bool DidAnything => DeferredAgedOut > 0 || DeadDropped > 0;
}
