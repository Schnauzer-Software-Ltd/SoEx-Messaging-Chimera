namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// The clock as a writer sees it. It never answers earlier than its last answer, or earlier than the newest stamp
/// already in the file that it writes.
/// </summary>
/// <remarks>
/// Three operations read a stamp and then act on the sequence beside it: the scheduled floor, the retention cutoff,
/// and the seek that finds the oldest record after a time. Each of them assumes that a later row never carries an
/// earlier stamp. The default clock is the wall clock. An NTP correction, a resumed virtual machine or a manual
/// change of the date can move it backwards. One backward step can write a scheduled row below a floor that only
/// rises. Nothing offers that row again. One backward step can also give retention a cutoff sequence that removes
/// unexpired records.
///
/// A clock that holds time still has a cost for a message published immediately after a step. A message due now becomes
/// due when the wall clock catches up. It arrives late by the size of the step, and it does arrive. The seed
/// carries the high-water across a restart. The high-water itself is only a field.
///
/// The rule holds within one file. Writers of different files in one root directory use separate clocks. The data in
/// one file cannot order the stamps of two clocks.
/// This remaining risk is disclosed, and it has no fix.
/// </remarks>
internal sealed class MonotonicClock
{
    private readonly TimeProvider _clock;
    private long _highWater;

    internal MonotonicClock(TimeProvider clock, long seed)
    {
        _clock = clock;
        _highWater = seed;
    }

    /// <summary>Unix milliseconds, never below a value already answered or already in the file.</summary>
    internal long NowMs()
    {
        long read = Sqlite.ToUnixMs(_clock.GetUtcNow());
        long seen = Interlocked.Read(ref _highWater);

        // Every caller today is on a writer thread, so this loop spins only against itself. The compare-exchange
        // keeps the guarantee for a second thread too.
        while (read > seen)
        {
            long found = Interlocked.CompareExchange(ref _highWater, read, seen);
            if (found == seen)
                return read;
            seen = found;
        }

        return seen;
    }

    /// <summary>The same answer as <see cref="NowMs"/>, for callers that work in offsets.</summary>
    internal DateTimeOffset Now() => Sqlite.FromUnixMs(NowMs());
}
