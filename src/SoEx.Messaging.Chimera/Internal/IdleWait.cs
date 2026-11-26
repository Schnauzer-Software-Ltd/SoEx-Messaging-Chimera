namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// How long a reader waits when it has nothing to deliver. This is a separate class because it is the one piece of
/// timing logic in this library where an error can go unseen. If the reader waits too long, a scheduled message is
/// late, and no other test catches that.
/// </summary>
internal static class IdleWait
{
    /// <summary>
    /// The poll interval, or the time until the next scheduled message falls due if that time is shorter. The
    /// result is never negative and never longer than the poll interval.
    /// </summary>
    internal static TimeSpan Until(DateTimeOffset now, DateTimeOffset? nextDue, TimeSpan pollInterval)
    {
        if (pollInterval < TimeSpan.Zero)
            pollInterval = TimeSpan.Zero;
        if (nextDue is not { } due)
            return pollInterval;

        TimeSpan untilDue = due - now;
        if (untilDue <= TimeSpan.Zero)
            return TimeSpan.Zero;   // already due: loop again now, with no sleep past the due time

        return untilDue < pollInterval ? untilDue : pollInterval;
    }
}
