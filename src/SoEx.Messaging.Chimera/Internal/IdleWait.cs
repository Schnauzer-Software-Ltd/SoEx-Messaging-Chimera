namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// How long a reader waits when there is nothing to deliver. Separated out because it is the one piece of
/// timing logic in this library that can be wrong in a way nothing else would notice: get it wrong in the
/// direction of waiting too long and a scheduled message is simply late, which no other test would catch.
/// </summary>
internal static class IdleWait
{
    /// <summary>
    /// The poll interval, unless a scheduled message falls due sooner, in which case wait exactly that long.
    /// Never negative, and never longer than the poll interval.
    /// </summary>
    internal static TimeSpan Until(DateTimeOffset now, DateTimeOffset? nextDue, TimeSpan pollInterval)
    {
        if (pollInterval < TimeSpan.Zero)
            pollInterval = TimeSpan.Zero;
        if (nextDue is not { } due)
            return pollInterval;

        TimeSpan untilDue = due - now;
        if (untilDue <= TimeSpan.Zero)
            return TimeSpan.Zero;   // already due; go round again rather than sleep past it

        return untilDue < pollInterval ? untilDue : pollInterval;
    }
}
