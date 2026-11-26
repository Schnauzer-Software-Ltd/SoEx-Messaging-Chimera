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

    /// <summary>
    /// The in-flight cap, deliberately never disposed.
    /// </summary>
    /// <remarks>
    /// A settle frees its slot after its durable write has committed, and closing drains the writer's queue
    /// rather than discarding it, so a settle whose write was queued when the close began commits during it
    /// and then releases against an object the close has been through. A disposed semaphore throws there, and
    /// the caller is told a settle that is on disk failed. Disposing buys nothing to set against that: the
    /// only thing <see cref="SemaphoreSlim.Dispose()"/> releases is the wait handle behind
    /// <see cref="SemaphoreSlim.AvailableWaitHandle"/>, which is never asked for here, and it does not wake
    /// anyone already waiting.
    /// </remarks>
    private readonly SemaphoreSlim _inFlight;
    private readonly ConcurrentDictionary<DeliveryTag, byte> _outstanding = new();

    /// <summary>
    /// Cancelled when the close begins, so that a reader parked on the in-flight cap or on an idle poll is
    /// answered instead of left waiting.
    /// </summary>
    /// <remarks>
    /// Never disposed, for the reason <see cref="_inFlight"/> is not: a reader can be about to link against
    /// it at the moment the close runs, and a disposed source throws there — turning the one thing that was
    /// meant to answer a parked reader into a different way of failing it. Cancelling is what this is for and
    /// cancelling needs no disposal; the registrations that matter belong to the linked sources the readers
    /// own, and those they dispose themselves.
    /// </remarks>
    private readonly CancellationTokenSource _closed = new();

    /// <summary>
    /// Deliveries this instance has in transit: claimed but not yet recorded as outstanding, or being settled
    /// right now. Guarded by <see cref="_gate"/> along with the seal and the quiet signal.
    /// </summary>
    /// <remarks>
    /// It exists because <see cref="_outstanding"/> is not a complete list of what this subscription is
    /// holding at every instant. A claim reaches the file before its tag reaches the dictionary, and a settle
    /// takes its tag out before its write and puts it back if the write failed. A hand-back that reads the
    /// dictionary in either window sees nothing to hand back and leaves the delivery in flight in the file
    /// with nobody holding it — which recovery cannot repair for as long as a sibling on the same subscriber
    /// and topic stays open, because recovery is skipped exactly then.
    /// </remarks>
    private readonly object _gate = new();
    private int _inTransit;
    private volatile bool _closing;
    private TaskCompletionSource? _quiet;

    private readonly StartAt _start;
    private bool _attached;
    private long _seenMinimum;

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

    /// <summary>What taking a delivery out of <see cref="_outstanding"/> in order to settle it found.</summary>
    private enum Held
    {
        /// <summary>Not this caller's to settle: already settled, handed back by the close, or never delivered.</summary>
        No,

        /// <summary>Taken, and counted as in transit until the settle has finished one way or the other.</summary>
        Yes,

        /// <summary>Still this instance's, but the close has sealed and will hand it back itself.</summary>
        Closing,
    }

    /// <summary>
    /// Takes a delivery out in order to settle it, and takes a place among the deliveries in transit with it.
    /// </summary>
    /// <remarks>
    /// One step, under the lock the close's own sweep takes, because the two are otherwise a race with a
    /// delivery in it. A settle that arrives after the seal has to put the tag back for the close to find,
    /// and a sweep already part-way through the dictionary would not look at it again — which is the defect
    /// this whole arrangement exists to close, reappearing inside its own fix.
    /// </remarks>
    private Held Take(DeliveryTag tag)
    {
        lock (_gate)
        {
            if (!_outstanding.TryRemove(tag, out _))
                return Held.No;

            if (_closing)
            {
                _outstanding[tag] = 0;
                return Held.Closing;
            }

            _inTransit++;
            return Held.Yes;
        }
    }

    /// <summary>
    /// Takes a place among the deliveries in transit, unless the close has sealed this subscription. A false
    /// answer means the caller must not start: the close is about to read <see cref="_outstanding"/> and act
    /// on what it finds there.
    /// </summary>
    private bool Begin()
    {
        lock (_gate)
        {
            if (_closing)
                return false;
            _inTransit++;
            return true;
        }
    }

    /// <summary>Files a claimed delivery as held and gives that place up, in the one step.</summary>
    private void Recorded(DeliveryTag tag)
    {
        lock (_gate)
        {
            _outstanding[tag] = 0;
            if (--_inTransit == 0)
                _quiet?.TrySetResult();
        }
    }

    /// <summary>Gives that place up, having left the delivery filed wherever it now belongs.</summary>
    private void End()
    {
        lock (_gate)
        {
            if (--_inTransit == 0)
                _quiet?.TrySetResult();
        }
    }

    /// <summary>
    /// Seals the subscription against new claims and new settles, and answers with the wait for the ones
    /// already in transit.
    /// </summary>
    private Task SealAsync()
    {
        lock (_gate)
        {
            _closing = true;
            if (_inTransit == 0)
                return Task.CompletedTask;

            _quiet ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _quiet.Task;
        }
    }

    /// <summary>
    /// What a closed subscription tells anybody who reaches it, in the same words the store uses, so that a
    /// caller cannot tell from the answer which of the two it got there first.
    /// </summary>
    private static ChimeraException Closed() => new("The store is closed.");

    /// <inheritdoc/>
    public string Subscriber { get; }

    /// <inheritdoc/>
    public string Topic { get; }

    /// <summary>Opens a subscription for <paramref name="subscriber"/> on <paramref name="topic"/>.</summary>
    /// <remarks>
    /// The first open records the subscriber's starting position. The subscriber name is a durable identity:
    /// a new name starts from a new position. If the topic's file is absent, the subscription waits for the
    /// publisher to create it.
    /// </remarks>
    /// <param name="subscriber">The subscriber's name. Its position and set-aside messages are stored under it.</param>
    /// <param name="topic">The topic to read.</param>
    /// <param name="options">The root directory and the settings shared by every topic and subscriber under it.</param>
    /// <param name="subscriptionOptions">Settings for this subscription. The defaults apply if this is null.</param>
    /// <returns>The open subscription. Dispose it to close it.</returns>
    /// <exception cref="LogIdentityException">
    /// The subscriber's stored position belongs to a different incarnation of the log, for example after a restore.
    /// </exception>
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

        // Same reason, on the bounds that decide what this subscriber may keep. Null means unbounded, so only
        // supplied values are checked, and zero or less is not a tight bound but a nonsensical one: a set
        // bounded at nothing dead-letters or stops on the very first row. The deferred age is the exception
        // and only refuses a negative — zero there means "age out the next time a pass sees it", which files
        // the message dead rather than destroying it, and is a legitimate thing to ask for.
        SetAsidePolicy setAside = subscriptionOptions.SetAside;
        if (setAside.MaxDeferredAge is { } deferredAge && deferredAge < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(subscriptionOptions), deferredAge,
                "A deferred age cannot be negative. Use null for no age bound.");
        Positive(setAside.MaxDeferredCount, "A deferred count bound");
        Positive(setAside.MaxDeadCount, "A dead-letter count bound");
        Positive(setAside.MaxDeferredBytes, "A deferred byte bound");
        Positive(setAside.MaxDeadBytes, "A dead-letter byte bound");

        static void Positive<T>(T? value, string what) where T : struct, IComparable<T>
        {
            if (value is { } supplied && supplied.CompareTo(default) <= 0)
                throw new ArgumentOutOfRangeException(nameof(subscriptionOptions), supplied,
                    $"{what} must be greater than zero. Use null for no bound.");
        }

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

    /// <inheritdoc/>
    public async IAsyncEnumerable<EventEnvelope> ReadAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Linked to the close as well as to the caller's token. Every wait a reader can be parked in belongs
        // to this loop, and none of them is woken by anything the close does to the file: a consumer that
        // passes an uncancellable token — which this signature allows — would sit on the in-flight cap or on
        // an idle poll for ever after its subscription was closed.
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _closed.Token);

        while (!cancellationToken.IsCancellationRequested)
        {
            // Taking the slot first is what makes the cap real: nothing is claimed that there is no room to
            // deliver, so a consumer that stops settling stops being read to.
            try
            {
                await _inFlight.WaitAsync(stop.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw Closed();
            }

            // Claimed and recorded have to happen inside one place the close can wait for. A claim reaches
            // the file first and the tag reaches _outstanding afterwards, and a hand-back that swept between
            // the two left the delivery in flight with nobody holding it.
            if (!Begin())
            {
                _inFlight.Release();
                throw Closed();
            }

            EventEnvelope? next;
            try
            {
                next = await NextAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                _inFlight.Release();
                End();
                throw;
            }

            if (next is null)
            {
                _inFlight.Release();
                End();
                try
                {
                    await IdleAsync(stop.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    throw Closed();
                }
                continue;
            }

            Recorded(next.Value.Tag);

            // Recorded, so the close is now certain to hand this delivery back — which means it is going out
            // again, to this subscriber's next reader or to a sibling. Handing it to a handler here as well
            // would put two of them on one message, so the loop ends instead and says why.
            if (_closing)
            {
                _inFlight.Release();
                throw Closed();
            }

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
        // The bound is on the stored set, so the stop belongs to the set and not to this object, and it is
        // read from the set on every poll. A subscriber that stopped at a full dead set, was closed and opened
        // again used to deliver until the next dead letter re-armed a flag the old process took with it, so a
        // restart bought another crop of deliveries. Asking once per instance instead fixed the restart and
        // left the same hole open sideways: a second subscription on this subscriber and topic that happened
        // to ask while the set had room kept that answer for as long as it lived, so a set filled by one of
        // them stopped that one and a fresh one while the warm one delivered past it indefinitely. Reading it
        // every time is also what lets the answer go the other way — replaying or discarding the dead letters
        // is how someone clears this, and it would be unhelpful to make them restart the process as well.
        //
        // It costs one indexed read of this subscriber's own file per poll, and only for a subscription that
        // asked to be stopped by a bound. A poll cancelled before the read has an answer simply has not asked
        // yet, which is the ordinary state of the next poll anyway.
        if (StopsAtBound)
        {
            SetAsideSizes size = await _store.SetAsideSizeAsync(Topic, ct).ConfigureAwait(false);
            if (StoppedBy(size) is { } stopped)
                throw stopped;
        }

        if (!await TryAttachAsync(ct).ConfigureAwait(false))
            return null;

        SetAsideEntry? replayed = await _store.ClaimReadySetAsideAsync(Topic, ct).ConfigureAwait(false);
        if (replayed is not null)
        {
            return new EventEnvelope(replayed.MessageId, replayed.EnqueuedAt, _options.Clock.GetUtcNow(),
                replayed.Attempts, replayed.Payload,
                new DeliveryTag(EntrySource.SetAside, replayed.AsideId, replayed.Attempts));
        }

        EventEnvelope? scheduled = await TakeScheduledAsync(ct).ConfigureAwait(false);
        if (scheduled is not null)
            return scheduled;

        long mark = await _store.ReadMarkAsync(Topic, ct).ConfigureAwait(false);
        mark = await DetectLossAsync(mark, ct).ConfigureAwait(false);

        Dictionary<long, EntryState> states =
            await _store.EntryStatesAsync(Topic, EntrySource.Log, ct).ConfigureAwait(false);
        await ForgetStrandedAsync(mark, states, ct).ConfigureAwait(false);

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
    /// Settles the log entries that came to rest under the position, reporting the ones still owed.
    /// </summary>
    /// <remarks>
    /// The log twin of the pass over removed scheduled rows below, and it exists for the same reason: a
    /// delivery that was out when retention overtook this subscriber is settled after the position has moved
    /// past the record, and where that leaves it depends on how it was settled. Neither pass can be done at
    /// the moment the record goes, because at that moment a handler still has the message and the answer is
    /// not known yet.
    /// <para>
    /// The states this already read are what decides whether to run it at all. Entries under the mark are the
    /// exception rather than the rule — a subscriber that is keeping up never has one — so the ordinary poll
    /// pays a comparison over a dictionary it was going to read anyway rather than a write per read. The mark
    /// is read again inside the write, so a stale reading here can only cost a pass that finds nothing.
    /// </para>
    /// </remarks>
    private async ValueTask ForgetStrandedAsync(long mark, Dictionary<long, EntryState> states,
        CancellationToken ct)
    {
        if (!states.Any(entry => entry.Key <= mark && entry.Value != EntryState.InFlight))
            return;

        List<long> owed = await _store.ForgetStrandedAsync(Topic, ct).ConfigureAwait(false);
        foreach (long id in owed)
            states.Remove(id);

        if (owed.Count > 0)
            throw new MessagesLostException(Topic, Subscriber, owed.Count, owed.Min(), owed.Max());
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
    /// <para>
    /// One of the entries it forgets is not a tidy-up but a loss, and is reported as one. An entry left
    /// available is a delivery that failed and was told it would come again, and the row it would have come
    /// from has gone. There is no position on this path to infer a loss from — which is why a scheduled
    /// message nobody ever read goes unremarked — but an entry is not an inference: it is this subscriber's
    /// own record that the message reached it and is still owed.
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

        HashSet<long> owedIfForgotten = gone.Where(id => states[id] == EntryState.Available).ToHashSet();

        // Forgotten first, reported second: the report is the only one there will ever be, and an entry left
        // behind by a cancelled throw would be counted again by every poll after it.
        //
        // And reported for what was forgotten rather than for what this asked to forget. The write turns away
        // an entry that has been claimed since these states were read, and naming that message as lost while
        // a handler holds it would be the same mistake in the report that the guard exists to stop in the file.
        List<long> forgotten = await _store.ForgetScheduledAsync(Topic, gone, ct).ConfigureAwait(false);
        foreach (long id in forgotten)
            states.Remove(id);

        List<long> owed = forgotten.Where(owedIfForgotten.Contains).ToList();
        if (owed.Count > 0)
            throw new MessagesLostException(Topic, Subscriber, owed.Count, owed.Min(), owed.Max(), scheduled: true);
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

            // The claim re-checks the position inside its own write — the mark for a log row, the floor for
            // a scheduled one, which is why the due time goes along. The states and candidates this loop is
            // working from were read in earlier ops, and a second reader of the same subscriber can settle
            // and absorb a row in between; a claim that trusted this method's reading would re-insert an
            // entry for it below the position, and deliver the message twice on a path where nothing failed.
            long attempt = await _store.TryClaimAsync(Topic, source, candidate.Id,
                Sqlite.ToUnixMs(candidate.EligibleAt), ct).ConfigureAwait(false);
            if (attempt == 0)
                continue;

            return new EventEnvelope(candidate.MessageId, candidate.EnqueuedAt, candidate.EligibleAt,
                (int)attempt, payload, new DeliveryTag(source, candidate.Id, (int)attempt));
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

        // Measuring the gap and moving the position over it are one write, and the gap in the report is the
        // one that write measured — not the one this method read a moment ago. Two readers of the same
        // subscriber can reach here together, and when they did the measurement and the move were separate
        // ops: both measured against the un-moved mark and the same loss was reported twice. The write that
        // finds the mark already moved measures nothing and reports nothing, so between however many readers
        // there are, the loss is reported by exactly the one that moved the position over it.
        (long moved, long lost, long first, long last) =
            await _store.SettleLossAsync(Topic, min, ct).ConfigureAwait(false);

        // And only now is this minimum a seen one. A detection that runs once has to be one that finished:
        // a consumer cancelling a read is ordinary, and cancelling it above would leave the flag set with the
        // mark unmoved, so no later poll would look again and the loss would never be reported at all. A
        // cancelled attempt instead retries the whole detection, which is safe because it is idempotent —
        // the mark only moves forward, and the second pass measures the gap against where it moved to.
        _seenMinimum = min;
        if (lost <= 0)
            return moved;

        throw new MessagesLostException(Topic, Subscriber, lost, first, last);
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
    /// Runs one settle: claims the right to settle this delivery, does the work, and frees the in-flight slot
    /// once it is done. Claiming it is also what makes settling one twice refused rather than acted on.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The in-flight slot is the reason the claim has to be checked. Each settle releases one, so a message
    /// settled twice releases two and the cap quietly grows by one for the life of the subscription — or the
    /// release throws, from a call the handler could not connect to the one that caused it. Both are worse
    /// than being told plainly, on the second call, that this delivery is already finished with.
    /// </para>
    /// <para>
    /// The slot is freed on success only, and the delivery goes back to being outstanding if the work threw.
    /// A settle that failed has settled nothing — the entry in the file still says in flight — so the caller
    /// has to be able to try again, and the slot it is holding is not free. Freeing it anyway would let the
    /// cap the consumer configured drift up by one for every settle that failed, and forgetting the delivery
    /// would leave it in flight in the file with nothing able to settle it short of opening the subscription
    /// again.
    /// </para>
    /// <para>
    /// Which is why what runs inside one settle is one durable write and nothing after it. Handing the claim
    /// back is right for a settle that did nothing and a lie about one whose effect committed, and this cannot
    /// tell the two apart: the settles below leave nothing that can throw once their write has gone in. The
    /// release beneath them is part of that and not an exception to it, which is why the semaphore it releases
    /// is one nothing ever disposes — a close that ran while the write was in the queue would otherwise turn
    /// the last step of a committed settle into the report that it had failed.
    /// </para>
    /// <para>
    /// What is claimed is a delivery and not a message, which is why the tag carries the attempt. A tag naming
    /// only the row would be reissued by every redelivery, so a failure and the retry behind it re-armed the
    /// tag a stale caller was still holding: its second settle was accepted, it settled the retry out from
    /// under the handler working on it, and that handler's own settle was refused for a mistake it had not
    /// made.
    /// </para>
    /// </remarks>
    private async ValueTask<T> SettlingAsync<T>(EventEnvelope envelope, Func<ValueTask<T>> settle)
    {
        // A settle that starts after the close has sealed is turned away before it has done anything, and the
        // delivery is left where the hand-back will find it. Letting it run instead is what stranded a
        // failing settle's delivery: the tag is out of the dictionary for the length of the write, the catch
        // below files it again, and a hand-back that has already been past never looks a second time.
        switch (Take(envelope.Tag))
        {
            case Held.No:
                // All three causes are named, because the caller can only act on one of them. A settle
                // arriving after the close was told it had already settled the message, which is a lie about
                // its own delivery: the hand-back took the claim, nothing was written, and the message is
                // there for the next subscription to be handed. Held.No has known these apart all along.
                throw new InvalidOperationException(
                    $"Message '{envelope.MessageId}' on topic '{Topic}' is not this caller's to settle: it " +
                    $"has already been settled, or closing the subscription handed the delivery back, or " +
                    $"subscriber '{Subscriber}' never delivered it. Each delivery is settled once.");
            case Held.Closing:
                throw Closed();
        }

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
        finally
        {
            // After the catch, so that a settle which failed has re-filed its delivery before the close is
            // told this one is finished. The whole point of the wait is that what it waits for is settled
            // one way or the other by the time it ends.
            End();
        }

        _inFlight.Release();
        return result;
    }

    /// <inheritdoc/>
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

    /// <inheritdoc/>
    public ValueTask<bool> FailAsync(EventEnvelope envelope, string reason,
        CancellationToken cancellationToken = default)
        => SettlingAsync(envelope, async () =>
        {
            bool exhausted = envelope.Attempt >= _subscriptionOptions.MaxAttempts;

            if (envelope.Tag.Source == EntrySource.SetAside)
            {
                if (exhausted)
                {
                    // The same refusal the other routes into the dead set make, on this one — decided in the
                    // write, which can tell a deferral arriving in the dead set from a dead letter being
                    // re-filed as what it already is. Refused, nothing was written and the delivery is still
                    // the caller's, to complete deliberately or to fail again once the bound is somewhere it
                    // can live.
                    SetAsideOutcome dead = await _store.RefileAsync(envelope.Tag.Id, Topic, SetAsideKind.Dead,
                        reason, _subscriptionOptions.SetAside, cancellationToken).ConfigureAwait(false);
                    if (dead.Refused)
                        throw Unkeepable(envelope.Payload.Length);
                    return false;
                }

                long settled = await _store.SettleSetAsideAsync(envelope.Tag.Id, SetAsideDelivery.Ready,
                    cancellationToken).ConfigureAwait(false);

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
            return released > 0 && StillInTheTopic(envelope.Tag);
        });

    /// <summary>
    /// Whether the record a handed-back delivery would come from is still in the topic file.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The release answers about this subscriber's own entry, and the promise a failure makes is about the
    /// record. The two part company exactly where retention overtakes a delivery: the entry is there to hand
    /// back long after the only thing that could act on it has gone, and a caller told "it will come again"
    /// has no way to find out that nothing will ever offer it.
    /// </para>
    /// <para>
    /// Asked after the release rather than before it. A record removed in between is one this subscriber is
    /// still owed and is reported as lost, which is the honest ending for it; asking first would answer about
    /// a file the entry had not yet been handed back against, and a stale "gone" there would be the same lie
    /// pointing the other way. A reader with no file to answer from says nothing, and nothing is not "gone".
    /// </para>
    /// </remarks>
    private bool StillInTheTopic(DeliveryTag tag)
    {
        bool? present = tag.Source == EntrySource.Scheduled
            ? _reader.ExistingScheduled([tag.Id])?.Contains(tag.Id)
            : _reader.ExistingLog(tag.Id);
        return present ?? true;
    }

    /// <inheritdoc/>
    public ValueTask DeferAsync(EventEnvelope envelope, string? reason = null,
        CancellationToken cancellationToken = default)
        => Discard(SettlingAsync(envelope, async () =>
        {
            await SetAsideAsync(SetAsideKind.Deferred, envelope, reason, cancellationToken).ConfigureAwait(false);
            return true;
        }));

    /// <inheritdoc/>
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
            // Already in this subscriber's store, so this only changes what it is filed as — in both
            // directions. A dead letter someone replayed and then deferred is filed as deferred, which is what
            // the call says it does and what an operator working a dead set down is asking for.
            //
            // Every bound is read inside the write, against the set the row is joining, and that includes the
            // keepability refusal: whether this row is joining the dead set or is in it already is the thing
            // the decision turns on, and it is the write that knows.
            SetAsideOutcome refiled = await _store.RefileAsync(envelope.Tag.Id, Topic, kind,
                reason ?? (kind == SetAsideKind.Dead ? "dead-lettered" : null),
                _subscriptionOptions.SetAside, ct).ConfigureAwait(false);

            if (refiled.Refused)
                throw Unkeepable(envelope.Payload.Length);
            return;
        }

        // The bounds go with the row rather than being checked here: two handlers deferring at once would
        // both read a set with room in it and both insert into it, and the bound the consumer configured
        // would hold only as long as nothing happened concurrently.
        SetAsideOutcome outcome = await _store.SetAsideAsync(kind, Topic, envelope.Tag.Source, envelope.Tag.Id,
            envelope.MessageId, envelope.EnqueuedAt, envelope.Attempt, reason, envelope.Payload,
            _subscriptionOptions.SetAside, ct).ConfigureAwait(false);

        // Refused inside the write, thrown out here: nothing was stored, so this settle has done nothing and
        // the delivery is still the caller's, exactly as after any settle that threw.
        if (outcome.Refused)
            throw Unkeepable(envelope.Payload.Length);
    }

    /// <summary>
    /// Refuses a dead letter this subscriber could not then keep: with a byte bound smaller than the message
    /// and dropping the oldest to stay inside it, the trim destroys this row on its next pass whatever else
    /// the set holds, so filing it would be storing a message in order to lose it. The refusal itself is made
    /// in the write that would have filed the row — the set the row is measured against is the set it is
    /// joining — and thrown out here, where the caller that caused it is. The caller can complete the
    /// delivery instead and lose the message knowingly, which is a decision rather than an accident.
    /// </summary>
    private DeadLetterTooLargeException Unkeepable(int size)
        => new(Topic, Subscriber, size, _subscriptionOptions.SetAside.MaxDeadBytes ?? 0);

    /// <summary>Whether this subscriber is one a full dead set stops at all.</summary>
    private bool StopsAtBound
        => _subscriptionOptions.SetAside.DeadOverflow == DeadLetterOverflow.StopDelivering
        && (_subscriptionOptions.SetAside.MaxDeadCount is not null
            || _subscriptionOptions.SetAside.MaxDeadBytes is not null);

    /// <summary>
    /// Whether a dead set of this size is one the subscriber should stop on. Both bounds answer the same
    /// question and reaching either is enough, so they are asked in the one place rather than at each of the
    /// three moments a dead letter can arrive.
    /// </summary>
    /// <remarks>
    /// What it counts is the dead letters parked in the set, not every row filed as dead. Replaying them is
    /// the remedy this exception names, and completing a replay is what takes one out of the set — but a
    /// replay has to be delivered before it can be completed, and a stopped subscriber delivers nothing. On
    /// the count of every dead row, the remedy therefore could not run: replay changes only the delivery
    /// state, so the number never moved and the subscriber stayed stopped against the one thing that could
    /// have cleared it. Counting the parked ones lets a replay out and re-arms the stop if it comes back.
    /// </remarks>
    private SubscriberStoppedException? StoppedBy(SetAsideSizes size)
    {
        SetAsidePolicy policy = _subscriptionOptions.SetAside;
        if (policy.DeadOverflow != DeadLetterOverflow.StopDelivering)
            return null;

        if (policy.MaxDeadCount is { } count && size.DeadHeld >= count)
            return new SubscriberStoppedException(Topic, Subscriber, size.DeadHeld, size.DeadHeldBytes, count,
                SizeBound.Count);

        if (policy.MaxDeadBytes is { } bytes && size.DeadHeldBytes >= bytes)
            return new SubscriberStoppedException(Topic, Subscriber, size.DeadHeld, size.DeadHeldBytes, bytes,
                SizeBound.Bytes);

        return null;
    }

    /// <inheritdoc/>
    public async ValueTask<SetAsideReport> ApplyRetentionAsync(CancellationToken cancellationToken = default)
    {
        SetAsidePolicy policy = _subscriptionOptions.SetAside;

        if (policy.MaxDeferredAge is { } maxAge)
        {
            await _store.AgeOutDeferredAsync(Topic, _options.Clock.GetUtcNow() - maxAge,
                $"deferred longer than {maxAge}", cancellationToken).ConfigureAwait(false);
        }

        if (policy.MaxDeadCount is not null || policy.MaxDeadBytes is not null)
        {
            if (policy.DeadOverflow == DeadLetterOverflow.DropOldest)
            {
                await _store.TrimDeadToAsync(Topic, policy.MaxDeadCount, policy.MaxDeadBytes,
                    cancellationToken).ConfigureAwait(false);
            }

            // Nothing to arm on the other policy: this pass can be what takes the set to its limit — a
            // deferral ageing out is a dead letter arriving — and the read that decides whether to stop
            // happens on the next poll against the set this pass leaves behind. Arming it here as well used
            // to be what made the bound hold, which meant a pass cancelled between the ageing and the arming
            // left a set at its limit delivering.
        }

        // Everything since the last pass, not only what this one did. A dead letter arriving is as likely to
        // cause a drop as a maintenance pass is, and they are the same loss for the same reason: reporting
        // one and not the other would put a number next to a silence. The ageing count is read the same way
        // and from the same place, so a pass that fails after ageing leaves its number for the next one
        // instead of taking it away with it.
        (long aged, long dropped) = await _store.TakeCountersAsync(Topic, cancellationToken)
            .ConfigureAwait(false);
        return new SetAsideReport(aged, dropped);
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<SetAsideEntry>> DeferredAsync(CancellationToken cancellationToken = default)
        => await _store.ListSetAsideAsync(Topic, SetAsideKind.Deferred, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<SetAsideEntry>> DeadLettersAsync(CancellationToken cancellationToken = default)
        => await _store.ListSetAsideAsync(Topic, SetAsideKind.Dead, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public ValueTask<bool> ReplayAsync(long asideId, CancellationToken cancellationToken = default)
        => _store.ReplayAsync(asideId, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<bool> DiscardAsync(long asideId, CancellationToken cancellationToken = default)
        => _store.DiscardAsync(asideId, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask<SubscriptionLag> LagAsync(CancellationToken cancellationToken = default)
    {
        if (!await TryAttachAsync(cancellationToken).ConfigureAwait(false))
            return new SubscriptionLag(0, null, 0);

        long mark = await _store.ReadMarkAsync(Topic, cancellationToken).ConfigureAwait(false);
        (_, long max) = _reader.Bounds();
        DateTimeOffset? oldest = _reader.OldestAfter(mark);

        // The scheduled side is counted as well, because the age bound removes from both tables and a call
        // whose purpose is warning before retention overtakes a subscriber cannot be blind to half of what
        // retention reaches. A backlog of due scheduled messages used to read as no lag at all.
        DateTimeOffset now = _options.Clock.GetUtcNow();
        ScheduledFloor floor = await _store.ReadScheduledFloorAsync(Topic, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<long, EntryState> states =
            await _store.EntryStatesAsync(Topic, EntrySource.Scheduled, cancellationToken).ConfigureAwait(false);

        // Settled rows the floor has not reached yet are inside the due window but are not owed: the floor
        // moves in the reader rather than at the moment of settling, so counting them says work is owed for
        // a message already handled. Asked of the topic file rather than subtracted from a number counted
        // here, so a row that has since been removed drops out of both sides instead of one.
        List<long> absorbable = states.Where(entry => entry.Value == EntryState.Complete)
            .Select(entry => entry.Key).ToList();
        long due = Math.Max(0, _reader.DueCount(floor, now) - _reader.DueAmong(floor, now, absorbable));

        // A duration, not a count: retention is set in hours, and a number of messages cannot be compared with it.
        return new SubscriptionLag(Math.Max(0, max - mark),
            oldest is null ? null : now - oldest.Value, due);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            // Two phases, because a hand-back can only be right if what it reads is a complete list. Sealing
            // stops new claims and new settles; the wait lets the ones already in transit finish filing
            // themselves either way — a settle that commits during it keeps its true answer, one that fails
            // puts its delivery back, and a claim that lands records its tag — and only then is _outstanding
            // the whole of what this instance is holding.
            Task quiet = SealAsync();
            _closed.Cancel();
            await quiet.ConfigureAwait(false);
            await ReleaseOutstandingAsync().ConfigureAwait(false);
        }
        finally
        {
            _reader.Dispose();
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
    /// <para>
    /// It runs on a sealed and quiet subscription, which is what makes one pass enough. Reading this
    /// dictionary while claims and settles are still moving through it is reading a snapshot rather than a
    /// list: a claim reaches the file before its tag arrives here, and a settle takes its tag out for the
    /// length of its write. Both were being missed, and a delivery this instance is holding that nobody hands
    /// back is one no other instance may touch either — recovery is skipped while a sibling on the same
    /// subscriber and topic is open, which is precisely the case that makes the strand indefinite.
    /// </para>
    /// </remarks>
    private async ValueTask ReleaseOutstandingAsync()
    {
        // Taken whole, under the lock a settle takes to claim one, so that this and a handler settling at the
        // same moment cannot both act on a delivery and cannot both miss one. Emptying the dictionary here is
        // also what makes a settle arriving afterwards say "already settled", which is the true answer: this
        // is about to hand the delivery back.
        List<DeliveryTag> holding;
        lock (_gate)
        {
            holding = [.. _outstanding.Keys];
            _outstanding.Clear();
        }

        foreach (DeliveryTag tag in holding)
        {
            if (tag.Source == EntrySource.SetAside)
                await _store.SettleSetAsideAsync(tag.Id, SetAsideDelivery.Ready).ConfigureAwait(false);
            else
                await _store.ReleaseAsync(Topic, tag.Source, tag.Id).ConfigureAwait(false);
        }
    }
}
