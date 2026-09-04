using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using SoEx.Messaging.Chimera.Internal;

namespace SoEx.Messaging.Chimera;

/// <summary>One subscriber reading one topic from local files.</summary>
public sealed class SqliteSubscription : ISubscription
{
    private readonly ChimeraOptions _options;
    private readonly SubscriptionOptions _subscriptionOptions;
    private readonly SubscriberStore _store;
    private readonly TopicReader _reader;
    private readonly SemaphoreSlim _inFlight;
    private readonly ConcurrentDictionary<DeliveryTag, byte> _outstanding = new();
    private readonly StartAt _start;
    private bool _attached;
    private long _seenMinimum;
    private SubscriberStoppedException? _stopped;

    private SqliteSubscription(string subscriber, string topic, ChimeraOptions options,
        SubscriptionOptions subscriptionOptions, SubscriberStore store, TopicReader reader, StartAt start,
        bool attached)
    {
        _start = start;
        _attached = attached;
        Subscriber = subscriber;
        Topic = topic;
        _options = options;
        _subscriptionOptions = subscriptionOptions;
        _store = store;
        _reader = reader;
        _inFlight = new SemaphoreSlim(subscriptionOptions.MaxInFlight, subscriptionOptions.MaxInFlight);
    }

    public string Subscriber { get; }
    public string Topic { get; }

    public static SqliteSubscription Open(string subscriber, string topic, ChimeraOptions options,
        SubscriptionOptions? subscriptionOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriber);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(options);

        subscriptionOptions ??= new SubscriptionOptions();
        if (subscriptionOptions.MaxInFlight <= 0)
            throw new ArgumentOutOfRangeException(nameof(subscriptionOptions),
                subscriptionOptions.MaxInFlight, "At least one delivery must be allowed in flight.");
        if (subscriptionOptions.MaxAttempts <= 0)
            throw new ArgumentOutOfRangeException(nameof(subscriptionOptions),
                subscriptionOptions.MaxAttempts, "At least one attempt must be allowed.");

        // Both of these fail quietly rather than loudly if they are wrong, which is why they are checked here.
        // A batch size of zero delivers nothing at all and looks like an empty topic; a negative one reaches
        // SQLite as LIMIT -1, which means no limit and reads the whole table into memory. A poll interval of
        // zero or less turns the idle wait into a spin on a core.
        if (subscriptionOptions.ReadBatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(subscriptionOptions),
                subscriptionOptions.ReadBatchSize, "At least one record must be read at a time.");
        if (subscriptionOptions.PollInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(subscriptionOptions),
                subscriptionOptions.PollInterval, "The poll interval must be greater than zero.");

        // The position is bound here, while the subscription is being created, rather than at its first read.
        // That is what makes a starting position mean anything: what a subscriber is entitled to is decided by
        // when it came into existence, not by when its consumer got round to reading.
        var reader = new TopicReader(FileNames.TopicFile(options.RootDirectory, topic), options);
        try
        {
            // A topic that does not exist yet has nothing this subscriber can have missed, whichever starting
            // position was asked for, and the position is worked out now rather than when the file turns up:
            // messages published between here and the first read arrived after this subscriber existed.
            bool exists = reader.TryOpen();
            StartAt start = exists ? StartOf(reader, options, subscriptionOptions) : default;
            Attachment? attach = exists ? new Attachment(topic, reader.LogUuid!, start) : null;

            SubscriberStore store = SubscriberStore.Open(subscriber, options, topic, attach);
            return new SqliteSubscription(subscriber, topic, options, subscriptionOptions, store, reader,
                start, attached: attach is not null);
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Where a position that does not exist yet begins. Reading the log's lower bound rather than starting at
    /// zero is the difference between a subscriber that reports the retained window as loss and one that
    /// knows records removed before it existed were never its to read.
    /// </summary>
    private static StartAt StartOf(TopicReader reader, ChimeraOptions options, SubscriptionOptions subscription)
    {
        (long min, long max) = reader.Bounds();

        return subscription.Start == StartPosition.Next
            ? new StartAt(max, reader.DueSoFar(options.Clock.GetUtcNow()))
            : new StartAt(Math.Max(0, min - 1), ScheduledFloor.Start);
    }

    /// <summary>
    /// Binds the position to the log once the file exists. A topic nobody has published to yet is simply empty,
    /// so this waits rather than failing — and rather than creating the file, which would make a subscriber a
    /// writer of a log it is only supposed to read.
    /// </summary>
    private async ValueTask<bool> TryAttachAsync(CancellationToken ct)
    {
        if (_attached)
            return true;
        if (!_reader.TryOpen())
            return false;

        await _store.AttachTopicAsync(Topic, _reader.LogUuid!, _start, ct).ConfigureAwait(false);
        _attached = true;
        return true;
    }

    public async IAsyncEnumerable<EventEnvelope> ReadAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // Taking the slot first is what makes the cap real: nothing is claimed that there is no room to
            // deliver, so a consumer that stops settling stops being read to.
            await _inFlight.WaitAsync(cancellationToken).ConfigureAwait(false);

            EventEnvelope? next;
            try
            {
                next = await NextAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                _inFlight.Release();
                throw;
            }

            if (next is null)
            {
                _inFlight.Release();
                await IdleAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            _outstanding[next.Value.Tag] = 0;
            yield return next.Value;
        }
    }

    /// <summary>
    /// The three sources in order of urgency. Draining the log first would starve scheduled messages under a
    /// sustained backlog — invisible at development volumes and arbitrarily late in production — and the
    /// set-aside and scheduled sets are both bounded, so taking them first costs nothing.
    /// </summary>
    private async ValueTask<EventEnvelope?> NextAsync(CancellationToken ct)
    {
        if (_stopped is not null)
        {
            // Recheck rather than latch: replaying or discarding the dead letters is how someone clears this,
            // and it would be unhelpful to make them restart the process as well.
            (_, long dead, _) = await _store.SetAsideSizeAsync(Topic, ct).ConfigureAwait(false);
            if (_subscriptionOptions.SetAside.MaxDeadCount is { } limit && dead >= limit)
                throw _stopped;
            _stopped = null;
        }

        if (!await TryAttachAsync(ct).ConfigureAwait(false))
            return null;

        SetAsideEntry? replayed = await _store.ClaimReadySetAsideAsync(Topic, ct).ConfigureAwait(false);
        if (replayed is not null)
        {
            return new EventEnvelope(replayed.MessageId, replayed.EnqueuedAt, _options.Clock.GetUtcNow(),
                replayed.Attempts, replayed.Payload, new DeliveryTag(EntrySource.SetAside, replayed.AsideId));
        }

        EventEnvelope? scheduled = await TakeScheduledAsync(ct).ConfigureAwait(false);
        if (scheduled is not null)
            return scheduled;

        long mark = await _store.ReadMarkAsync(Topic, ct).ConfigureAwait(false);
        mark = await DetectLossAsync(mark, ct).ConfigureAwait(false);

        Dictionary<long, EntryState> states =
            await _store.EntryStatesAsync(Topic, EntrySource.Log, ct).ConfigureAwait(false);
        return await TakeAsync(EntrySource.Log, _reader.After(mark, _subscriptionOptions.ReadBatchSize),
            0, states, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Takes the next due scheduled message, moving this subscriber's floor over whatever it has finished with
    /// on the way.
    /// </summary>
    /// <remarks>
    /// A consumed scheduled row stays in the topic file — a subscriber reads that file and cannot delete from
    /// it, and fan-out means it must not, since another subscriber may not have taken the row yet. So without
    /// a floor the oldest due rows fill every batch this reads and a newer message never appears in one: the
    /// scheduled table stops delivering entirely, and stays stopped until retention removes the rows. Moving
    /// the floor over the settled run is what keeps the window pointed at work rather than at history, and it
    /// is also what lets the entries recording those rows be deleted rather than kept for ever.
    /// </remarks>
    private async ValueTask<EventEnvelope?> TakeScheduledAsync(CancellationToken ct)
    {
        DateTimeOffset now = _options.Clock.GetUtcNow();
        ScheduledFloor floor = await _store.ReadScheduledFloorAsync(Topic, ct).ConfigureAwait(false);

        // Read before the window rather than inside it, so it happens on a poll that finds nothing as well as
        // on one that finds work. That costs one indexed query against this subscriber's own file per idle
        // poll, and buys the only chance there is to notice an entry whose row has gone: a topic with nothing
        // due is exactly where one sits unnoticed.
        Dictionary<long, EntryState> states =
            await _store.EntryStatesAsync(Topic, EntrySource.Scheduled, ct).ConfigureAwait(false);
        await ForgetRemovedScheduledAsync(states, ct).ConfigureAwait(false);

        while (true)
        {
            List<Candidate> candidates = _reader.Due(floor, now, _subscriptionOptions.ReadBatchSize);
            if (candidates.Count == 0)
                return null;

            // Only the leading run, and only rows that are finished with. Absorbing the highest settled row
            // instead would swallow everything in flight below it, and that message would never come back.
            var absorbed = new List<long>();
            while (absorbed.Count < candidates.Count
                && states.TryGetValue(candidates[absorbed.Count].Id, out EntryState settled)
                && settled == EntryState.Complete)
            {
                absorbed.Add(candidates[absorbed.Count].Id);
            }

            if (absorbed.Count > 0)
            {
                Candidate last = candidates[absorbed.Count - 1];
                floor = new ScheduledFloor(Sqlite.ToUnixMs(last.EligibleAt), last.Id);
                await _store.AbsorbScheduledAsync(Topic, floor, absorbed, ct).ConfigureAwait(false);
            }

            EventEnvelope? taken = await TakeAsync(EntrySource.Scheduled, candidates, absorbed.Count, states, ct)
                .ConfigureAwait(false);
            if (taken is not null)
                return taken;

            // Nothing takeable here. Another batch is worth reading only if the floor moved past this one,
            // which it did exactly when every candidate was absorbed.
            if (absorbed.Count < candidates.Count)
                return null;

            states = await _store.EntryStatesAsync(Topic, EntrySource.Scheduled, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Drops the entries for scheduled rows the topic file no longer holds.
    /// </summary>
    /// <remarks>
    /// The floor absorbs an entry when it moves over the row that entry records, and it only moves over rows
    /// it has seen as candidates. Retention removing a row before that happened — a settle just before a
    /// shutdown, a subscriber offline longer than the retention window, a crash whose redelivery the window
    /// outlived — leaves an entry nothing can absorb, so it stays for as long as the file does and is counted
    /// for that long by everything that reads it.
    /// <para>
    /// A delivery still in flight is left alone even when its row has gone. It is being handled now; forgetting
    /// it would report this subscriber as having nothing left to do while a handler is running, and the settle
    /// that follows would write the entry straight back. Once that settle lands the next poll takes it.
    /// </para>
    /// </remarks>
    private async ValueTask ForgetRemovedScheduledAsync(Dictionary<long, EntryState> states, CancellationToken ct)
    {
        List<long> settled = states.Where(entry => entry.Value != EntryState.InFlight)
            .Select(entry => entry.Key).ToList();
        if (settled.Count == 0)
            return;

        // A reader with no file to answer from says nothing, which is not the same as saying none of these
        // rows exist — and treating the two the same would delete every entry the moment a file went missing.
        if (_reader.ExistingScheduled(settled) is not { } present)
            return;

        List<long> gone = settled.Where(id => !present.Contains(id)).ToList();
        if (gone.Count == 0)
            return;

        await _store.ForgetScheduledAsync(Topic, gone, ct).ConfigureAwait(false);
        foreach (long id in gone)
            states.Remove(id);
    }

    /// <summary>
    /// The first candidate this subscriber can take. What it is already holding or has already finished with
    /// is known from its own store, so a batch mostly made of those costs one read rather than a write per row.
    /// </summary>
    private async ValueTask<EventEnvelope?> TakeAsync(EntrySource source, List<Candidate> candidates, int from,
        Dictionary<long, EntryState> states, CancellationToken ct)
    {
        for (int i = from; i < candidates.Count; i++)
        {
            Candidate candidate = candidates[i];
            if (states.TryGetValue(candidate.Id, out EntryState state) && state != EntryState.Available)
                continue;   // in flight now, or finished with and waiting to be absorbed

            // The payload is fetched before the claim, and only for the row about to be delivered. Claiming
            // first would leave an entry recording a delivery of a record retention had already removed.
            byte[]? payload = _reader.PayloadOf(source, candidate.Id);
            if (payload is null)
                continue;

            long attempt = await _store.TryClaimAsync(Topic, source, candidate.Id, ct).ConfigureAwait(false);
            if (attempt == 0)
                continue;

            return new EventEnvelope(candidate.MessageId, candidate.EnqueuedAt, candidate.EligibleAt,
                (int)attempt, payload, new DeliveryTag(source, candidate.Id));
        }
        return null;
    }

    /// <summary>
    /// Retention removing records a subscriber had not read is bounded, attributable loss rather than a
    /// surprise — but only if somebody notices. Comparing the position with the lowest surviving sequence is
    /// the whole detection, and it costs one query against a log this is reading anyway.
    /// </summary>
    private async ValueTask<long> DetectLossAsync(long mark, CancellationToken ct)
    {
        (long min, _) = _reader.Bounds();
        if (min <= mark + 1 || min <= _seenMinimum)
            return mark;

        // Records this subscriber was handed and is still settling are not lost to it, however they left the
        // log. Counting them would name messages a handler has in its hands as ones that never arrived.
        long held = await _store.HeldBetweenAsync(Topic, EntrySource.Log, mark, min, ct).ConfigureAwait(false);
        long lost = min - mark - 1 - held;

        // The position has to move over the gap before anything is said about it. The mark only advances over
        // contiguous entries, so leaving it below records that will never exist again would pin it there for
        // good, and every later completion would pile up as an entry that can never be absorbed.
        long moved = await _store.ForceMarkAsync(Topic, min - 1, ct).ConfigureAwait(false);

        // And only now is this minimum a seen one. A detection that runs once has to be one that finished:
        // a consumer cancelling a read is ordinary, and cancelling it above would leave the flag set with the
        // mark unmoved, so no later poll would look again and the loss would never be reported at all. A
        // cancelled attempt instead retries the whole detection, which is safe because it is idempotent —
        // the mark only moves forward, and the second pass measures the gap against where it moved to.
        _seenMinimum = min;
        if (lost <= 0)
            return moved;

        throw new MessagesLostException(Topic, Subscriber, lost, mark + 1, min - 1);
    }

    private async ValueTask IdleAsync(CancellationToken ct)
    {
        // Waiting until the next due time rather than polling for it: the timer lives here, in memory, and a
        // restart simply reads the table again and works it out afresh.
        DateTimeOffset now = _options.Clock.GetUtcNow();
        TimeSpan wait = IdleWait.Until(now, _reader.NextDue(now), _subscriptionOptions.PollInterval);
        if (wait > TimeSpan.Zero)
            await Task.Delay(wait, _options.Clock, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Claims the right to settle a delivery, so that settling one twice is refused rather than acted on.
    /// </summary>
    /// <remarks>
    /// The in-flight slot is the reason this has to be checked. Each settle releases one, so a message settled
    /// twice releases two and the cap quietly grows by one for the life of the subscription — or the release
    /// throws, from a call the handler could not connect to the one that caused it. Both are worse than being
    /// told plainly, on the second call, that this delivery is already finished with.
    /// </remarks>
    /// <summary>
    /// Runs one settle: claims the right to settle this delivery, does the work, and frees the in-flight slot
    /// once it is done.
    /// </summary>
    /// <remarks>
    /// The slot is freed on success only, and the delivery goes back to being outstanding if the work threw.
    /// A settle that failed has settled nothing — the entry in the file still says in flight — so the caller
    /// has to be able to try again, and the slot it is holding is not free. Freeing it anyway would let the
    /// cap the consumer configured drift up by one for every settle that failed, and forgetting the delivery
    /// would leave it in flight in the file with nothing able to settle it short of opening the subscription
    /// again.
    /// </remarks>
    private async ValueTask<T> SettlingAsync<T>(EventEnvelope envelope, Func<ValueTask<T>> settle)
    {
        if (!_outstanding.TryRemove(envelope.Tag, out _))
            throw new InvalidOperationException(
                $"Message '{envelope.MessageId}' on topic '{Topic}' has already been settled, or was not " +
                $"delivered by subscriber '{Subscriber}'. Each delivery is settled once.");

        T result;
        try
        {
            result = await settle().ConfigureAwait(false);
        }
        catch
        {
            _outstanding[envelope.Tag] = 0;
            throw;
        }

        _inFlight.Release();
        return result;
    }

    public ValueTask CompleteAsync(EventEnvelope envelope, CancellationToken cancellationToken = default)
        => Discard(SettlingAsync(envelope, async () =>
        {
            if (envelope.Tag.Source == EntrySource.SetAside)
                await _store.RemoveSetAsideAsync(envelope.Tag.Id, cancellationToken).ConfigureAwait(false);
            else
            {
                await _store.CompleteAsync(Topic, envelope.Tag.Source, envelope.Tag.Id, cancellationToken)
                    .ConfigureAwait(false);
            }
            return true;
        }));

    public ValueTask<bool> FailAsync(EventEnvelope envelope, string reason,
        CancellationToken cancellationToken = default)
        => SettlingAsync(envelope, async () =>
        {
            bool exhausted = envelope.Attempt >= _subscriptionOptions.MaxAttempts;

            if (envelope.Tag.Source == EntrySource.SetAside)
            {
                long settled = await _store.SettleSetAsideAsync(envelope.Tag.Id,
                    exhausted ? SetAsideDelivery.Held : SetAsideDelivery.Ready, cancellationToken)
                    .ConfigureAwait(false);
                if (exhausted)
                {
                    await MakeDeadAsync(envelope.Tag.Id, reason, cancellationToken).ConfigureAwait(false);
                    return false;
                }

                // What the settle actually did, exactly as on the log path below. A set-aside row can go
                // while a handler holds it — a crash between the claim and the settle, or retention on the
                // dead set — and saying it will be delivered again when there is nothing left to deliver is
                // the one answer a caller cannot check for itself.
                return settled > 0;
            }

            if (exhausted)
            {
                await SetAsideAsync(SetAsideKind.Dead, envelope, reason, cancellationToken).ConfigureAwait(false);
                return false;
            }

            // What the release actually did, not what it was asked to do. Retention can overtake a subscriber
            // while it is holding a delivery, and the position then moves over a record that no longer exists;
            // saying the message will come back when no read will ever offer it again is the one answer a
            // caller cannot check for itself.
            long released = await _store.ReleaseAsync(Topic, envelope.Tag.Source, envelope.Tag.Id,
                cancellationToken).ConfigureAwait(false);
            return released > 0;
        });

    public ValueTask DeferAsync(EventEnvelope envelope, string? reason = null,
        CancellationToken cancellationToken = default)
        => Discard(SettlingAsync(envelope, async () =>
        {
            await SetAsideAsync(SetAsideKind.Deferred, envelope, reason, cancellationToken).ConfigureAwait(false);
            return true;
        }));

    public ValueTask DeadLetterAsync(EventEnvelope envelope, string reason,
        CancellationToken cancellationToken = default)
        => Discard(SettlingAsync(envelope, async () =>
        {
            await SetAsideAsync(SetAsideKind.Dead, envelope, reason, cancellationToken).ConfigureAwait(false);
            return true;
        }));

    private static async ValueTask Discard<T>(ValueTask<T> settle) => await settle.ConfigureAwait(false);

    /// <summary>
    /// Files a message in this subscriber's own store, downgrading a deferral to a dead letter if the deferred
    /// set is full. The downgrade is decided in the write that inserts the row, so that the set it was
    /// measured against is the set it joined.
    /// </summary>
    private async ValueTask SetAsideAsync(SetAsideKind kind, EventEnvelope envelope, string? reason,
        CancellationToken ct)
    {
        if (envelope.Tag.Source == EntrySource.SetAside)
        {
            // Already in this subscriber's store; it only changes what it is filed as. The bounds below are
            // not consulted, and that is the point: re-filing a row adds neither a row nor a byte, so a
            // message replayed and deferred again at the bound was being measured against itself and
            // dead-lettered for a set that was not growing.
            await _store.SettleSetAsideAsync(envelope.Tag.Id, SetAsideDelivery.Held, ct).ConfigureAwait(false);
            if (kind == SetAsideKind.Dead)
                await MakeDeadAsync(envelope.Tag.Id, reason ?? "dead-lettered", ct).ConfigureAwait(false);
            return;
        }

        // The bounds go with the row rather than being checked here: two handlers deferring at once would
        // both read a set with room in it and both insert into it, and the bound the consumer configured
        // would hold only as long as nothing happened concurrently.
        (_, SetAsideKind filed) = await _store.SetAsideAsync(kind, Topic, envelope.Tag.Source, envelope.Tag.Id,
            envelope.MessageId, envelope.EnqueuedAt, envelope.Attempt, reason, envelope.Payload,
            _subscriptionOptions.SetAside, ct).ConfigureAwait(false);

        if (filed == SetAsideKind.Dead)
            await EnforceDeadBoundAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Files a message already in this subscriber's store as dead, and holds the dead bound to it.
    /// </summary>
    /// <remarks>
    /// A row moving from deferred to dead grows the dead set by one just as a fresh dead letter does, and it
    /// is the same set with the same bound. Enforcing it only where a row is newly inserted left the count
    /// free to pass the limit whenever a message was replayed and then given up on — which is the ordinary way
    /// a deferred message ends. Ageing one out already enforced it, from the retention pass; this brings the
    /// two together.
    /// </remarks>
    private async ValueTask MakeDeadAsync(long asideId, string reason, CancellationToken ct)
    {
        await _store.MakeDeadAsync(asideId, reason, ct).ConfigureAwait(false);
        await EnforceDeadBoundAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Dead is where the passing-along stops, so this is the one place a bound has to be settled rather than
    /// deferred to somewhere else. Stopping is the default because a stopped subscriber keeps its position,
    /// does not block the publisher and does not touch any other subscriber.
    /// </summary>
    private async ValueTask EnforceDeadBoundAsync(CancellationToken ct)
    {
        if (_subscriptionOptions.SetAside.MaxDeadCount is not { } limit)
            return;

        (_, long dead, _) = await _store.SetAsideSizeAsync(Topic, ct).ConfigureAwait(false);

        // The limit is a maximum, so dropping trims back to it and stopping happens on reaching it — a
        // subscriber that stops has no way to take another one anyway.
        if (_subscriptionOptions.SetAside.DeadOverflow == DeadLetterOverflow.DropOldest)
        {
            if (dead > limit)
                await _store.DropOldestDeadAsync(Topic, dead - limit, ct).ConfigureAwait(false);
            return;
        }

        if (dead >= limit)
            _stopped = new SubscriberStoppedException(Topic, Subscriber, dead, limit);
    }

    public async ValueTask<SetAsideReport> ApplyRetentionAsync(CancellationToken cancellationToken = default)
    {
        SetAsidePolicy policy = _subscriptionOptions.SetAside;
        long agedOut = 0;

        if (policy.MaxDeferredAge is { } maxAge)
        {
            agedOut = await _store.AgeOutDeferredAsync(Topic, _options.Clock.GetUtcNow() - maxAge,
                $"deferred longer than {maxAge}", cancellationToken).ConfigureAwait(false);
        }

        long dropped = 0;
        if (policy.MaxDeadCount is { } limit)
        {
            (_, long dead, _) = await _store.SetAsideSizeAsync(Topic, cancellationToken).ConfigureAwait(false);
            if (dead > limit)
            {
                if (policy.DeadOverflow == DeadLetterOverflow.DropOldest)
                    dropped = await _store.DropOldestDeadAsync(Topic, dead - limit, cancellationToken)
                        .ConfigureAwait(false);
                else
                    _stopped = new SubscriberStoppedException(Topic, Subscriber, dead, limit);
            }
        }

        return new SetAsideReport(agedOut, dropped);
    }

    public async ValueTask<IReadOnlyList<SetAsideEntry>> DeferredAsync(CancellationToken cancellationToken = default)
        => await _store.ListSetAsideAsync(Topic, SetAsideKind.Deferred, cancellationToken).ConfigureAwait(false);

    public async ValueTask<IReadOnlyList<SetAsideEntry>> DeadLettersAsync(CancellationToken cancellationToken = default)
        => await _store.ListSetAsideAsync(Topic, SetAsideKind.Dead, cancellationToken).ConfigureAwait(false);

    public ValueTask<bool> ReplayAsync(long asideId, CancellationToken cancellationToken = default)
        => _store.ReplayAsync(asideId, cancellationToken);

    public ValueTask<bool> DiscardAsync(long asideId, CancellationToken cancellationToken = default)
        => _store.DiscardAsync(asideId, cancellationToken);

    public async ValueTask<SubscriptionLag> LagAsync(CancellationToken cancellationToken = default)
    {
        if (!await TryAttachAsync(cancellationToken).ConfigureAwait(false))
            return new SubscriptionLag(0, null);

        long mark = await _store.ReadMarkAsync(Topic, cancellationToken).ConfigureAwait(false);
        (_, long max) = _reader.Bounds();
        DateTimeOffset? oldest = _reader.OldestAfter(mark);

        // A duration, not a count: retention is set in hours, and a number of messages cannot be compared with it.
        return new SubscriptionLag(Math.Max(0, max - mark),
            oldest is null ? null : _options.Clock.GetUtcNow() - oldest.Value);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await ReleaseOutstandingAsync().ConfigureAwait(false);
        }
        finally
        {
            _reader.Dispose();
            _inFlight.Dispose();
            await _store.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Hands back whatever this subscription is still holding, so that closing it does the same thing to its
    /// deliveries as the process going away would.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Recovery on the next open cannot do this instead. It is skipped while anything in this process is
    /// still reading the same subscriber and topic — it has to be, or a second subscription would reset the
    /// deliveries the first one's handlers are working on — so a subscription closed with work in its hands
    /// next to a sibling that stays open leaves that work in flight in the file with nobody holding it, and
    /// nothing would offer it again until every instance had closed. In a host that keeps running, that is
    /// indefinite. What this instance holds is known exactly here, so it hands it back itself.
    /// </para>
    /// <para>
    /// The attempt count stays where it is, exactly as recovery leaves it, and a failure to write is not
    /// swallowed: a release that did not happen is the same stranding by a quieter route.
    /// </para>
    /// </remarks>
    private async ValueTask ReleaseOutstandingAsync()
    {
        foreach (DeliveryTag tag in _outstanding.Keys)
        {
            // Taking the tag out first is what keeps this and a handler settling at the same moment from both
            // acting on one delivery, for the same reason settling does it.
            if (!_outstanding.TryRemove(tag, out _))
                continue;

            if (tag.Source == EntrySource.SetAside)
                await _store.SettleSetAsideAsync(tag.Id, SetAsideDelivery.Ready).ConfigureAwait(false);
            else
                await _store.ReleaseAsync(Topic, tag.Source, tag.Id).ConfigureAwait(false);
        }
    }
}
