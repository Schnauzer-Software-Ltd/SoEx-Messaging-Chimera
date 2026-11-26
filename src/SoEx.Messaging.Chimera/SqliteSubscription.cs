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
    /// The in-flight cap. Do not dispose it.
    /// </summary>
    /// <remarks>
    /// A settle frees its slot after its durable write commits. The close drains the queue of the writer and
    /// keeps the queued writes. If the write of a settle is in the queue when the close starts, the settle
    /// commits during the close. The settle then releases its slot on an object that the close has already
    /// processed. A disposed semaphore throws at that release, and the caller gets a failure for a settle that
    /// is on disk. <see cref="SemaphoreSlim.Dispose()"/> releases only the wait handle behind
    /// <see cref="SemaphoreSlim.AvailableWaitHandle"/>. This class never uses that handle. Disposal also does
    /// not wake a caller that already waits.
    /// </remarks>
    private readonly SemaphoreSlim _inFlight;
    private readonly ConcurrentDictionary<DeliveryTag, byte> _outstanding = new();

    /// <summary>
    /// The close cancels this source when it starts. A reader that waits on the in-flight cap or on an idle
    /// poll then gets an answer.
    /// </summary>
    /// <remarks>
    /// Do not dispose it. The reason is the same as for <see cref="_inFlight"/>. A reader can link to this
    /// source at the moment the close runs. A disposed source throws at that link. The signal that must answer
    /// a waiting reader then fails that reader in a different way. Cancellation is the purpose of this source.
    /// Cancellation needs no disposal. The important registrations belong to the linked sources that the
    /// readers own. The readers dispose those sources.
    /// </remarks>
    private readonly CancellationTokenSource _closed = new();

    /// <summary>
    /// The deliveries in transit in this instance. A delivery in transit is claimed and not yet recorded as
    /// outstanding, or it is in a settle now. <see cref="_gate"/> guards this count, the seal and the quiet
    /// signal.
    /// </summary>
    /// <remarks>
    /// <see cref="_outstanding"/> can miss a delivery that this subscription holds, in two windows. A claim
    /// reaches the file before its tag reaches the dictionary. A settle removes its tag before its write. If
    /// the write fails, the settle puts the tag back. If a hand-back reads the dictionary in either window, it
    /// finds nothing to hand back. The delivery then stays in flight in the file with no holder. Recovery
    /// cannot repair this while a sibling on the same subscriber and topic stays open, because recovery is
    /// skipped in that case.
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

    /// <summary>The result when a settle takes a delivery out of <see cref="_outstanding"/>.</summary>
    private enum Held
    {
        /// <summary>The caller cannot settle it. It is already settled, the close handed it back, or this instance never delivered it.</summary>
        No,

        /// <summary>Taken. The delivery counts as in transit until the settle ends, with success or failure.</summary>
        Yes,

        /// <summary>This instance still holds the delivery. The close has sealed and hands the delivery back itself.</summary>
        Closing,
    }

    /// <summary>
    /// Takes a delivery out for a settle. The same step counts the delivery as in transit.
    /// </summary>
    /// <remarks>
    /// This is one step, under the same lock as the sweep of the close. Without the lock, the two race over a
    /// delivery. If a settle arrives after the seal, it must put the tag back for the close to find. If the
    /// sweep is part-way through the dictionary, it does not look at that tag again. The result is the defect
    /// that this arrangement exists to prevent, back inside its own fix.
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
    /// Adds the caller to the deliveries in transit. If the close has sealed this subscription, this returns
    /// false. The caller must then not start, because the close is about to read <see cref="_outstanding"/>
    /// and act on its contents.
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

    /// <summary>Records a claimed delivery as held. The same step removes it from the deliveries in transit.</summary>
    private void Recorded(DeliveryTag tag)
    {
        lock (_gate)
        {
            _outstanding[tag] = 0;
            if (--_inTransit == 0)
                _quiet?.TrySetResult();
        }
    }

    /// <summary>Removes the caller from the deliveries in transit. The caller has already filed the delivery where it belongs.</summary>
    private void End()
    {
        lock (_gate)
        {
            if (--_inTransit == 0)
                _quiet?.TrySetResult();
        }
    }

    /// <summary>
    /// Seals the subscription against new claims and new settles. Returns the wait for the deliveries already
    /// in transit.
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
    /// The exception that a closed subscription gives to each caller. Its message is the same as the message
    /// from the store. A caller cannot tell from it which of the two it reached first.
    /// </summary>
    private static ChimeraException Closed() => new("The store is closed.");

    /// <inheritdoc/>
    public string Subscriber { get; }

    /// <inheritdoc/>
    public string Topic { get; }

    /// <summary>Opens a subscription for <paramref name="subscriber"/> on <paramref name="topic"/>.</summary>
    /// <remarks>
    /// The first open records the starting position of the subscriber. The subscriber name is a durable
    /// identity. A new name starts from a new position. If the topic file is absent, the subscription waits for
    /// the publisher to create it.
    /// </remarks>
    /// <param name="subscriber">The subscriber name. The store keeps its position and set-aside messages under it.</param>
    /// <param name="topic">The topic to read.</param>
    /// <param name="options">The root directory and the settings for all topics and subscribers under it.</param>
    /// <param name="subscriptionOptions">The settings for this subscription. If this is null, the defaults apply.</param>
    /// <returns>The open subscription. Dispose it to close it.</returns>
    /// <exception cref="LogIdentityException">
    /// The stored position of the subscriber belongs to a different incarnation of the log, for example after a restore.
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

        // A wrong value in either of these fails silently, so this method checks them here. A batch size of zero
        // delivers nothing and looks like an empty topic. A negative batch size reaches SQLite as LIMIT -1.
        // LIMIT -1 means no limit, and SQLite then reads the whole table into memory. A poll interval of zero or
        // less makes the idle wait spin on a core.
        if (subscriptionOptions.ReadBatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(subscriptionOptions),
                subscriptionOptions.ReadBatchSize, "At least one record must be read at a time.");
        if (subscriptionOptions.PollInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(subscriptionOptions),
                subscriptionOptions.PollInterval, "The poll interval must be greater than zero.");

        // The same reason applies to the bounds on what this subscriber can keep. Null means unbounded, so this
        // checks only supplied values. A bound of zero or less is invalid, because a set with a bound of zero
        // dead-letters or stops on the first row. The deferred age is the exception. It refuses only a negative
        // value. A deferred age of zero means "age out the next time a pass sees it". That files the message as
        // a dead letter and keeps it. This is a valid setting.
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

        // The position is bound here, when the subscription is created. Its first read comes later. This gives
        // a starting position its meaning. The time when the subscriber came into existence decides what it can
        // read. The time of the first read by its consumer has no effect.
        var reader = new TopicReader(FileNames.TopicFile(options.RootDirectory, topic), options);
        try
        {
            // If the topic does not exist yet, this subscriber has missed nothing, for each starting position.
            // The position is calculated now, before the file appears. Messages published between now and the
            // first read arrive after this subscriber exists.
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
    /// Where a new position begins. The position starts at the lower bound of the log. With a start at zero,
    /// a subscriber reports the retained window as loss. With a start at the lower bound, a subscriber knows
    /// that records removed before it existed were never its records to read.
    /// </summary>
    private static StartAt StartOf(TopicReader reader, ChimeraOptions options, SubscriptionOptions subscription)
    {
        (long min, long max) = reader.Bounds();

        return subscription.Start == StartPosition.Next
            ? new StartAt(max, reader.DueSoFar(options.Clock.GetUtcNow()))
            : new StartAt(Math.Max(0, min - 1), ScheduledFloor.Start);
    }

    /// <summary>
    /// Binds the position to the log when the file exists. If the file is absent, nobody has published to the
    /// topic yet, and the topic is empty. This method then waits. It does not fail. It does not create the
    /// file, because that makes the subscriber a writer of a log that it only reads.
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
        // This source links to the close and to the caller's token. Every wait that can hold a reader
        // belongs to this loop. Nothing that the close does to the file wakes these waits. This signature
        // permits a token that cannot be cancelled. Without the link, a consumer with such a token waits for
        // ever on the in-flight cap or on an idle poll after the subscription closes.
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _closed.Token);

        while (!cancellationToken.IsCancellationRequested)
        {
            // Take the slot first. This makes the cap real. The loop claims a message only when there is room
            // to deliver it. If a consumer stops settling, it gets no more deliveries.
            try
            {
                await _inFlight.WaitAsync(stop.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw Closed();
            }

            // The claim and the record must happen inside one region that the close can wait for. A claim
            // reaches the file first. The tag reaches _outstanding after that. If a hand-back swept between
            // the two, the delivery stayed in flight with no holder.
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

            // The delivery is recorded, so the close will hand it back. The delivery then goes out again, to the
            // next reader of this subscriber or to a sibling. If this loop also gave it to a handler, two
            // handlers would have one message. So the loop ends here and gives the reason.
            if (_closing)
            {
                _inFlight.Release();
                throw Closed();
            }

            yield return next.Value;
        }
    }

    /// <summary>
    /// Reads the three sources in order of urgency. The set-aside set and the scheduled set are both bounded,
    /// so a read of them first costs nothing. If the log came first, a sustained backlog would starve
    /// scheduled messages. This starvation does not show at development volumes. In production, it makes
    /// scheduled messages late by any amount.
    /// </summary>
    private async ValueTask<EventEnvelope?> NextAsync(CancellationToken ct)
    {
        // The bound applies to the stored set. The stop therefore belongs to the set.
        // This method reads the stop from the set on each poll. An earlier version kept a flag. A subscriber
        // stopped at a full dead set, then closed and opened again. It delivered until the next dead letter
        // set the flag again, because the old process took the flag with it. So each restart gave more
        // deliveries. A later version asked once per instance. That fixed the restart and left the same fault
        // open in a different direction. A second subscription on this subscriber and topic asked while the
        // set had room, and kept that answer for its full life. A set filled by one subscription stopped that
        // subscription and a new one. The older one delivered past the bound with no end. A read on each poll
        // also lets the answer change back. A replay or a discard of the dead letters clears the stop. Then the
        // operator can continue with no restart of the process.
        //
        // The cost is one indexed read of this subscriber's file per poll. Only a subscription with a stop
        // at a bound pays it. If a poll is cancelled before the read answers, that poll has not asked yet. The
        // next poll is in that same state.
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
    /// Takes the next due scheduled message. On the way, it moves the floor of this subscriber over the rows
    /// that it has finished with.
    /// </summary>
    /// <remarks>
    /// A consumed scheduled row stays in the topic file. A subscriber reads that file and cannot delete from
    /// it. With fan-out, it must not delete, because another subscriber can still need the row. Without a
    /// floor, the oldest due rows fill each batch, and a newer message never gets into a batch. The scheduled
    /// table then stops all delivery until retention removes those rows. The floor moves over the settled run
    /// and keeps the window on work. The floor also lets the store delete the entries that record those rows.
    /// Without it, those entries stay for ever.
    /// </remarks>
    private async ValueTask<EventEnvelope?> TakeScheduledAsync(CancellationToken ct)
    {
        DateTimeOffset now = _options.Clock.GetUtcNow();
        ScheduledFloor floor = await _store.ReadScheduledFloorAsync(Topic, ct).ConfigureAwait(false);

        // Read the states before the window, outside it. Then the read happens on a poll that finds nothing,
        // and on a poll that finds work. The cost is one indexed query on this subscriber's file per idle
        // poll. It is the only chance to find an entry whose row has gone. Such an entry stays unnoticed in a
        // topic with nothing due.
        Dictionary<long, EntryState> states =
            await _store.EntryStatesAsync(Topic, EntrySource.Scheduled, ct).ConfigureAwait(false);
        await ForgetRemovedScheduledAsync(states, ct).ConfigureAwait(false);

        while (true)
        {
            List<Candidate> candidates = _reader.Due(floor, now, _subscriptionOptions.ReadBatchSize);
            if (candidates.Count == 0)
                return null;

            // Absorb only the leading run, and only rows that are finished. If this absorbed up to the highest
            // settled row, it would also absorb each in-flight row below it. That message would never come back.
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

            // No candidate here can be taken. Read another batch only if the floor moved past this one. The
            // floor moved past it only when this absorbed every candidate.
            if (absorbed.Count < candidates.Count)
                return null;

            states = await _store.EntryStatesAsync(Topic, EntrySource.Scheduled, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Settles the log entries that are now under the position. Reports the entries still owed.
    /// </summary>
    /// <remarks>
    /// This is the log equivalent of the pass over removed scheduled rows below, and it has the same cause. If
    /// a delivery was out when retention overtook this subscriber, the settle comes after the position moved
    /// past the record. The result then depends on the type of settle. Neither pass can run at the moment the
    /// record goes. At that moment a handler still has the message, and the result is unknown.
    /// <para>
    /// The states that this method already read decide whether the pass runs. Entries under the mark are
    /// rare, and a subscriber that keeps up never has one. So a normal poll costs one comparison over a
    /// dictionary that it reads anyway. It makes no write per read. The write reads the mark again, so a
    /// stale value here can cost only a pass that finds nothing.
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
    /// Removes the entries for scheduled rows that are gone from the topic file.
    /// </summary>
    /// <remarks>
    /// The floor absorbs an entry when it moves over the row that the entry records. The floor moves only over
    /// rows that it has seen as candidates. Retention can remove a row before that happens. Examples are a
    /// settle just before a shutdown, a subscriber offline for longer than the retention window, and a crash
    /// whose redelivery the window outlived. Nothing can absorb the entry that remains. It stays for the life
    /// of the file, and each reader of the file counts it for that time.
    /// <para>
    /// This pass leaves an in-flight delivery alone, also when its row has gone. A handler works on it now. If
    /// this pass removed it, the subscriber would report no work left while a handler runs. The settle that
    /// follows would also write the entry back. After that settle, the next poll removes the entry.
    /// </para>
    /// <para>
    /// One type of entry that this pass removes is a loss, and the pass reports it as a loss. An available
    /// entry is a delivery that failed. The failure told the caller that the message comes again. The row it
    /// comes from has gone. This path has no position to infer a loss from, so a scheduled message that
    /// nobody read gets no report. An entry is a direct record by this subscriber. It shows that the message
    /// reached the subscriber and is still owed.
    /// </para>
    /// </remarks>
    private async ValueTask ForgetRemovedScheduledAsync(Dictionary<long, EntryState> states, CancellationToken ct)
    {
        List<long> settled = states.Where(entry => entry.Value != EntryState.InFlight)
            .Select(entry => entry.Key).ToList();
        if (settled.Count == 0)
            return;

        // If the reader has no file, it gives no answer. No answer does not mean that these rows are gone. If
        // this treated the two as the same, it would delete each entry when a file went missing.
        if (_reader.ExistingScheduled(settled) is not { } present)
            return;

        List<long> gone = settled.Where(id => !present.Contains(id)).ToList();
        if (gone.Count == 0)
            return;

        HashSet<long> owedIfForgotten = gone.Where(id => states[id] == EntryState.Available).ToHashSet();

        // Remove first, then report. This report is the only one. If a cancelled throw left an entry behind,
        // each later poll would count it again.
        //
        // Report only what the write removed. The write refuses an entry that was claimed after these states
        // were read. A report of that message as lost while a handler holds it is the same error in the report
        // that the guard prevents in the file.
        List<long> forgotten = await _store.ForgetScheduledAsync(Topic, gone, ct).ConfigureAwait(false);
        foreach (long id in forgotten)
            states.Remove(id);

        List<long> owed = forgotten.Where(owedIfForgotten.Contains).ToList();
        if (owed.Count > 0)
            throw new MessagesLostException(Topic, Subscriber, owed.Count, owed.Min(), owed.Max(), scheduled: true);
    }

    /// <summary>
    /// The first candidate that this subscriber can take. Its own store records the rows that it holds and the
    /// rows that it has finished. So a batch of mostly those rows costs one read. It makes no write per row.
    /// </summary>
    private async ValueTask<EventEnvelope?> TakeAsync(EntrySource source, List<Candidate> candidates, int from,
        Dictionary<long, EntryState> states, CancellationToken ct)
    {
        for (int i = from; i < candidates.Count; i++)
        {
            Candidate candidate = candidates[i];
            if (states.TryGetValue(candidate.Id, out EntryState state) && state != EntryState.Available)
                continue;   // in flight now, or finished with and waiting to be absorbed

            // Fetch the payload before the claim, and only for the row that goes out next. If the claim came
            // first, an entry could record a delivery of a record that retention had already removed.
            byte[]? payload = _reader.PayloadOf(source, candidate.Id);
            if (payload is null)
                continue;

            // The claim checks the position again inside its own write. For a log row, the position is the mark.
            // For a scheduled row, it is the floor, so the claim also takes the due time. This loop works from
            // states and candidates that earlier operations read. A second reader of the same subscriber can
            // settle and absorb a row between them. If the claim trusted the values in this method, it would
            // insert the entry again below the position. The message then goes out twice on a path with no
            // failure.
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
    /// Retention can remove records that a subscriber has not read. That loss is bounded and attributable, if
    /// the subscriber detects it. Detection compares the position with the lowest remaining sequence. It costs
    /// one query against a log that this method reads anyway.
    /// </summary>
    private async ValueTask<long> DetectLossAsync(long mark, CancellationToken ct)
    {
        (long min, _) = _reader.Bounds();
        if (min <= mark + 1 || min <= _seenMinimum)
            return mark;

        // One write measures the gap and moves the position over it. The report uses the gap that this write
        // measured. It ignores the gap that this method read before. Two readers of the same subscriber can
        // get here at the same time. When the measure and the move were separate operations, both readers
        // measured against the old mark and reported the same loss twice. If the write finds the mark already
        // moved, it measures nothing and reports nothing. So, with any number of readers, only the reader that
        // moved the position over the gap reports the loss.
        (long moved, long lost, long first, long last) =
            await _store.SettleLossAsync(Topic, min, ct).ConfigureAwait(false);

        // Only now does this minimum count as seen. A detection that runs once must be a detection that
        // finished. A consumer can cancel a read at any time. If a cancel above left the flag set with the mark
        // not moved, no later poll would look again, and nobody would report the loss. A cancelled attempt runs
        // the whole detection again. This is safe because detection is idempotent. The mark moves only forward.
        // The second pass measures the gap from the new mark.
        _seenMinimum = min;
        if (lost <= 0)
            return moved;

        throw new MessagesLostException(Topic, Subscriber, lost, first, last);
    }

    private async ValueTask IdleAsync(CancellationToken ct)
    {
        // Wait until the next due time, with no poll for it. The timer is in memory here. After a restart, the
        // reader reads the table again and calculates the time again.
        DateTimeOffset now = _options.Clock.GetUtcNow();
        TimeSpan wait = IdleWait.Until(now, _reader.NextDue(now), _subscriptionOptions.PollInterval);
        if (wait > TimeSpan.Zero)
            await Task.Delay(wait, _options.Clock, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs one settle. It claims the right to settle this delivery, does the work, and frees the in-flight
    /// slot when the work is done. The claim also makes the subscription refuse a second settle of a delivery.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The in-flight slot is why the claim check is necessary. Each settle releases one slot. A message
    /// settled twice releases two slots, and the cap silently grows by one for the life of the subscription.
    /// Or the release throws, from a call that the handler cannot connect to its cause. The second call gets
    /// a clear error that this delivery is already finished.
    /// </para>
    /// <para>
    /// The slot is freed only on success. If the work throws, the delivery becomes outstanding again. A failed
    /// settle has settled nothing, and the entry in the file still shows in flight. The caller must be able to
    /// try again. The slot that it holds stays in use. If the slot were freed anyway, the configured cap would
    /// grow by one for each failed settle. If the delivery were forgotten, it would stay in flight in the file.
    /// Only a new open of the subscription could then settle it.
    /// </para>
    /// <para>
    /// For this reason, one settle runs one durable write and nothing after it. To hand the claim back is
    /// correct for a settle that did nothing. For a settle whose effect committed, it is a false report. This
    /// method cannot tell the two apart. So the settles below have nothing after their write that can throw.
    /// The release under them is part of that rule. For that reason, nothing disposes the semaphore that it
    /// releases. If a close ran while the write was in the queue, a disposed semaphore would turn the last step
    /// of a committed settle into a failure report.
    /// </para>
    /// <para>
    /// The claim is on a delivery. For this reason, the tag carries the attempt. An earlier tag named only the
    /// row, and each redelivery issued that tag again. A failure and its retry then made the tag of a stale
    /// caller valid again. The second settle of the stale caller was accepted, and it settled the retry while
    /// another handler worked on it. That handler's own settle was then refused for an error that it did not
    /// make.
    /// </para>
    /// </remarks>
    private async ValueTask<T> SettlingAsync<T>(EventEnvelope envelope, Func<ValueTask<T>> settle)
    {
        // If a settle starts after the close has sealed, this method refuses it before it does anything. The
        // delivery stays where the hand-back finds it. When such a settle ran, a failing settle stranded its
        // delivery. The tag is out of the dictionary for the length of the write. The catch below files it
        // again. If the hand-back has already gone past, it does not look a second time.
        switch (Take(envelope.Tag))
        {
            case Held.No:
                // Held.No is one answer for three causes: settled already, handed back by the close, or never
                // delivered here. The message names all three. An earlier message said only "already settled".
                // For a settle after the close, that was false. The hand-back took the claim, nothing was
                // written, and the message waits for the next subscription.
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
            // End() comes after the catch. A failed settle then files its delivery again before the close learns
            // that this settle is finished. The purpose of the wait is that each settle it waits for ends, with
            // success or failure, before the wait ends.
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
                    // This route makes the same refusal as the other routes into the dead set. The write decides
                    // it. The write can tell a deferral that arrives in the dead set from a dead letter that is
                    // filed again as a dead letter. After a refusal, nothing was written and the delivery still
                    // belongs to the caller. The caller can complete it deliberately. The caller can also fail it
                    // again when the bound can keep the message.
                    SetAsideOutcome dead = await _store.RefileAsync(envelope.Tag.Id, Topic, SetAsideKind.Dead,
                        reason, _subscriptionOptions.SetAside, cancellationToken).ConfigureAwait(false);
                    if (dead.Refused)
                        throw Unkeepable(envelope.Payload.Length);
                    return false;
                }

                long settled = await _store.SettleSetAsideAsync(envelope.Tag.Id, SetAsideDelivery.Ready,
                    cancellationToken).ConfigureAwait(false);

                // Return what the settle did, as on the log path below. A set-aside row can go while a handler
                // holds it, after a crash between the claim and the settle or after retention on the dead set.
                // If nothing is left to deliver, the answer "delivered again" is false. The caller cannot check
                // that answer itself.
                return settled > 0;
            }

            if (exhausted)
            {
                await SetAsideAsync(SetAsideKind.Dead, envelope, reason, cancellationToken).ConfigureAwait(false);
                return false;
            }

            // Return what the release did. Retention can overtake a subscriber while it holds a delivery. The
            // position then moves over a record that is gone. If no read will offer the message again, the
            // answer "it comes back" is false. The caller cannot check that answer itself.
            long released = await _store.ReleaseAsync(Topic, envelope.Tag.Source, envelope.Tag.Id,
                cancellationToken).ConfigureAwait(false);
            return released > 0 && StillInTheTopic(envelope.Tag);
        });

    /// <summary>
    /// Whether the topic file still holds the record that a handed-back delivery comes from.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The release reports on the entry of this subscriber. The promise of a failure is about the record. The
    /// two differ when retention overtakes a delivery. The entry stays, ready for hand-back, long after the
    /// record has gone. A caller told "it will come again" cannot find out that nothing will offer it.
    /// </para>
    /// <para>
    /// This check runs after the release. If retention removes the record between the two, this subscriber is
    /// still owed it, and the record is reported as lost. That is the correct result. A check before the
    /// release answers about a file that the entry was not yet handed back against. A stale "gone" there is
    /// the same false answer in the other direction. If the reader has no file, it gives no answer. No answer
    /// does not mean "gone".
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
    /// Files a message in this subscriber's store. If the deferred set is full, the deferral becomes a
    /// dead letter. The write that inserts the row makes that decision. So the row joins the same set that the
    /// decision measured.
    /// </summary>
    private async ValueTask SetAsideAsync(SetAsideKind kind, EventEnvelope envelope, string? reason,
        CancellationToken ct)
    {
        if (envelope.Tag.Source == EntrySource.SetAside)
        {
            // The row is already in this subscriber's store, so this changes only its kind, in both
            // directions. If someone replays a dead letter and then defers it, the row is filed as deferred. The
            // call states that result. An operator who works down a dead set asks for it.
            //
            // The write reads each bound against the set that the row joins. This includes the keepability
            // refusal. The decision depends on whether the row joins the dead set or is already in it. The write
            // knows which of the two is true.
            SetAsideOutcome refiled = await _store.RefileAsync(envelope.Tag.Id, Topic, kind,
                reason ?? (kind == SetAsideKind.Dead ? "dead-lettered" : null),
                _subscriptionOptions.SetAside, ct).ConfigureAwait(false);

            if (refiled.Refused)
                throw Unkeepable(envelope.Payload.Length);
            return;
        }

        // The bounds go with the row into the write. If this method checked them here, two handlers that defer
        // at the same time would both read a set with room and both insert into it. The configured bound would
        // then hold only when nothing happened concurrently.
        SetAsideOutcome outcome = await _store.SetAsideAsync(kind, Topic, envelope.Tag.Source, envelope.Tag.Id,
            envelope.MessageId, envelope.EnqueuedAt, envelope.Attempt, reason, envelope.Payload,
            _subscriptionOptions.SetAside, ct).ConfigureAwait(false);

        // The write refuses, and this method throws. Nothing was stored, so this settle did nothing. The delivery
        // still belongs to the caller, the same as after any settle that throws.
        if (outcome.Refused)
            throw Unkeepable(envelope.Payload.Length);
    }

    /// <summary>
    /// Refuses a dead letter that this subscriber cannot keep. If the byte bound is smaller than the message
    /// and the overflow policy drops the oldest, the next trim pass destroys this row. Other rows in the set
    /// have no effect on this. To file the row would store a message only to lose it. The write that would
    /// file the row makes the refusal, because the set that the row joins is the set it is measured against.
    /// This method throws it here, at the caller that caused it. The caller can complete the delivery and
    /// lose the message as a deliberate decision.
    /// </summary>
    private DeadLetterTooLargeException Unkeepable(int size)
        => new(Topic, Subscriber, size, _subscriptionOptions.SetAside.MaxDeadBytes ?? 0);

    /// <summary>Whether a full dead set stops this subscriber.</summary>
    private bool StopsAtBound
        => _subscriptionOptions.SetAside.DeadOverflow == DeadLetterOverflow.StopDelivering
        && (_subscriptionOptions.SetAside.MaxDeadCount is not null
            || _subscriptionOptions.SetAside.MaxDeadBytes is not null);

    /// <summary>
    /// Whether a dead set of this size stops the subscriber. Each bound answers the same question, and either
    /// one is enough. So one method checks both bounds for all three points where a dead letter can arrive.
    /// </summary>
    /// <remarks>
    /// It counts only the dead letters held in the set. It does not count every row filed as dead. Replay is
    /// the remedy that this exception names. A completed replay takes a row out of the set. A replay must be
    /// delivered before it can complete, and a stopped subscriber delivers nothing. With a count of every dead
    /// row, the remedy could not run. Replay changes only the delivery state, so the count did not change. The
    /// subscriber stayed stopped against the one action that could clear the stop. A count of held rows lets
    /// a replay out. If the replay comes back, the stop applies again.
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

            // The other policy needs no flag here. This pass can bring the set to its limit, because a deferral
            // that ages out is a dead letter that arrives. The next poll reads the set that this pass leaves and
            // decides whether to stop. Earlier, the bound held only because this pass also set a flag. A pass
            // cancelled between the ageing and the flag then left a set at its limit that still delivered.
        }

        // Report all counts since the last pass, including the counts from this pass. A dead letter that
        // arrives can cause a drop, the same as a maintenance pass. Both are the same loss for the same reason.
        // A report of one without the other gives a number for one loss and silence for the other. The ageing
        // count comes from the same place, read the same way. If a pass fails after ageing, its count stays for
        // the next pass.
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

        // Count the scheduled side too. The age bound removes from both tables. This call warns before
        // retention overtakes a subscriber, so it must see all that retention reaches. Earlier, a backlog of due
        // scheduled messages showed as no lag.
        DateTimeOffset now = _options.Clock.GetUtcNow();
        ScheduledFloor floor = await _store.ReadScheduledFloorAsync(Topic, cancellationToken)
            .ConfigureAwait(false);
        Dictionary<long, EntryState> states =
            await _store.EntryStatesAsync(Topic, EntrySource.Scheduled, cancellationToken).ConfigureAwait(false);

        // Settled rows that the floor has not reached yet are inside the due window. They are not owed. The
        // reader moves the floor after the settle, so a count of these rows reports work owed for a message
        // already handled. The topic file gives both numbers in this subtraction. So if retention has removed
        // a row since, the row leaves both sides together.
        List<long> absorbable = states.Where(entry => entry.Value == EntryState.Complete)
            .Select(entry => entry.Key).ToList();
        long due = Math.Max(0, _reader.DueCount(floor, now) - _reader.DueAmong(floor, now, absorbable));

        // Report a duration. Retention is set in hours, and a count of messages cannot be compared with it.
        return new SubscriptionLag(Math.Max(0, max - mark),
            oldest is null ? null : now - oldest.Value, due);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            // The close has two phases, because a hand-back is correct only if it reads a complete list. The
            // seal stops new claims and new settles. The wait lets the claims and settles in transit finish
            // filing. If a settle commits during the wait, it keeps its true result. If a settle fails, it puts
            // its delivery back. If a claim lands, it records its tag. After the wait, _outstanding holds all
            // the deliveries of this instance.
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
    /// Hands back each delivery that this subscription still holds. A close then has the same effect on its
    /// deliveries as the end of the process.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Recovery at the next open cannot do this work. Recovery is skipped while another reader in this process
    /// reads the same subscriber and topic. The skip is necessary, because otherwise a second subscription
    /// would reset deliveries that handlers of the first one work on. A subscription can close with work in
    /// hand next to an open sibling. That work then stays in flight in the file with no holder. Nothing offers
    /// it again until each instance has closed. In a host that keeps running, that wait has no end. This
    /// instance knows what it holds, so it hands that back itself.
    /// </para>
    /// <para>
    /// The attempt count does not change, the same as after recovery. A write failure goes to the caller. A
    /// release that did not happen strands the delivery in the same way, with less sign of it.
    /// </para>
    /// <para>
    /// This method runs on a sealed and quiet subscription, so one pass is enough. While claims and settles
    /// still move through this dictionary, a read of it gives a snapshot that can be incomplete. A claim
    /// reaches the file before its tag arrives here. A settle removes its tag for the length of its write.
    /// Earlier reads missed both cases. No other instance can touch a delivery that this instance holds, so a
    /// delivery that nobody hands back is stranded. Recovery is skipped while a sibling on the same subscriber
    /// and topic is open. That case makes the strand last with no end.
    /// </para>
    /// </remarks>
    private async ValueTask ReleaseOutstandingAsync()
    {
        // Take the whole list under the lock that a settle takes to claim one delivery. Then this method and a
        // concurrent settle cannot both act on a delivery or both miss one. After the clear, a later
        // settle gets Held.No. Its message names the hand-back by the close as one of its causes.
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
