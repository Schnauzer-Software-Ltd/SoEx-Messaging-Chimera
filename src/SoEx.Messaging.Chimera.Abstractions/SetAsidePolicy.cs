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

    /// <summary>Destroy the oldest dead letters to make room. Bounded, silent unless the count is watched.</summary>
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

    /// <summary>Total stored payload across everything this subscriber has set aside, deferred and dead.</summary>
    public long? MaxSetAsideBytes { get; init; } = 64L * 1024 * 1024;

    public DeadLetterOverflow DeadOverflow { get; init; } = DeadLetterOverflow.StopDelivering;
}

/// <summary>What one pass over a subscriber's set-aside store did.</summary>
/// <param name="DeferredAgedOut">Deferred messages that became dead letters because they sat too long.</param>
/// <param name="DeadDropped">Dead letters destroyed to stay within the bound. Only ever non-zero under
/// <see cref="DeadLetterOverflow.DropOldest"/>, and worth alarming on: it is real loss.</param>
public readonly record struct SetAsideReport(long DeferredAgedOut, long DeadDropped)
{
    public bool DidAnything => DeferredAgedOut > 0 || DeadDropped > 0;
}
