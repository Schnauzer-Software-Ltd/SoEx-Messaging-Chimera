namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// The clock as a writer is allowed to see it: never earlier than the last time it answered, and never
/// earlier than the newest stamp already in the file being written.
/// </summary>
/// <remarks>
/// Three things here read a stamp and then act on the sequence beside it — the scheduled floor, the
/// retention cutoff, and the seek that finds the oldest record after a time — and each of them assumes a
/// later row never carries an earlier stamp. The default clock is the wall clock, which an NTP correction,
/// a resumed virtual machine or somebody setting the date can move backwards. One step backwards is enough
/// to write a scheduled row underneath a floor that only ever rises, where nothing will offer it again, or
/// to hand retention a cutoff sequence that carries unexpired records away with it.
///
/// Holding time still instead costs a message published just after a step: due now becomes due when the
/// wall clock catches up, which is late by the size of the step rather than never. The seed is what carries
/// that across a restart, the high-water itself being no more than a field.
///
/// Two writers with two clocks on one topic directory are still two clocks, and nothing a single file knows
/// can order them. That residue is disclosed rather than fixed.
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

        // Every caller today is on a writer thread, so this only ever spins against itself. It is written to
        // survive a second one anyway: a compare-exchange is the difference between a guarantee and a habit.
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
