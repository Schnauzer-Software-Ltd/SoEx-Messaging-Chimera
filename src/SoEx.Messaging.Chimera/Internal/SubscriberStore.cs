using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// One subscriber's own file: its position in each topic, what it currently has in flight, and what it has set
/// aside. Written by this instance alone, which is why nothing here needs a lease or a visibility timeout —
/// a lock can only ever be held by one subscriber in one process, and if that process is gone so is the lock.
/// </summary>
internal sealed class SubscriberStore : IAsyncDisposable
{
    /// <summary>
    /// How many stores this process currently has reading each subscriber-and-topic pair, so that recovery
    /// can tell deliveries nobody is holding from deliveries a subscription is working on right now.
    /// </summary>
    private static readonly Dictionary<string, int> Reading = new(StringComparer.Ordinal);

    private readonly SingleWriter _writer;
    private readonly TimeProvider _clock;
    private readonly string? _topic;
    private bool _closed;

    private SubscriberStore(string subscriber, string path, string? topic, SingleWriter writer,
        TimeProvider clock)
    {
        Subscriber = subscriber;
        Path = path;
        _topic = topic;
        _writer = writer;
        _clock = clock;
    }

    internal string Subscriber { get; }
    internal string Path { get; }

    /// <summary>
    /// Opens the store, and binds it to a topic while doing so if the caller already knows which log it is
    /// about to read. Binding here rather than on the first read is what makes a starting position mean
    /// "where this subscriber came into existence": a subscriber created before a burst of publishing and
    /// retention should see that as loss, and one created after it should not.
    /// </summary>
    internal static SubscriberStore Open(string subscriber, ChimeraOptions options, string? topic = null,
        Attachment? attach = null)
    {
        Directory.CreateDirectory(options.RootDirectory);
        string path = FileNames.SubscriberFile(options.RootDirectory, subscriber);

        SqliteConnection connection = Sqlite.OpenWriter(path, options);
        bool counted = false;
        try
        {
            SubscriberSchema.Initialise(connection, subscriber);

            if (topic is not null)
            {
                lock (Reading)
                {
                    Reading.TryGetValue(Key(path, topic), out int already);
                    if (already == 0)
                        RecoverInFlight(connection, topic);
                    Reading[Key(path, topic)] = already + 1;
                    counted = true;
                }
            }

            if (attach is { } binding)
            {
                using SqliteTransaction transaction = connection.BeginTransaction();
                Attach(connection, transaction, subscriber, binding);
                transaction.Commit();
            }

            return new SubscriberStore(subscriber, path, topic,
                new SingleWriter(connection, options.MaxAppendBatch), options.Clock);
        }
        catch
        {
            if (counted)
                Release(path, topic!);
            connection.Dispose();
            throw;
        }
    }

    private static string Key(string path, string topic) => path + "\u0000" + topic;

    private static void Release(string path, string topic)
    {
        lock (Reading)
        {
            string key = Key(path, topic);
            if (!Reading.TryGetValue(key, out int open))
                return;
            if (open <= 1)
                Reading.Remove(key);
            else
                Reading[key] = open - 1;
        }
    }

    /// <summary>
    /// Anything in flight when this file is first opened was in flight in a process that is no longer running,
    /// so it becomes available again. This is the entire crash-redelivery mechanism, and the reason there is no
    /// visibility timeout anywhere in this library.
    /// </summary>
    /// <remarks>
    /// <para>
    /// It is scoped to one topic, and it is skipped when this process is already reading that topic as this
    /// subscriber. One subscriber file holds one subscriber's positions across every topic it reads, so a
    /// subscriber reading two topics is two subscriptions over one file — the documented shape, and the
    /// second of them used to reset every delivery the first was holding. Those went straight back out on top
    /// of handlers still running, with the attempt count raised, and both subscriptions then settled the same
    /// message. Nothing written in the file distinguishes "in flight in a process that is gone" from "in
    /// flight in this one", so what this process is reading right now has to.
    /// </para>
    /// <para>
    /// It counts what is being read rather than remembering what has been recovered, so closing a
    /// subscription and opening it again — one subscriber restarting inside a host that keeps running —
    /// recovers exactly as a process restart does, whether or not its siblings stayed open.
    /// </para>
    /// The row is released rather than deleted, so the attempt count survives. Deleting it would let a message
    /// that brings the process down be retried for ever, its counter starting again on every restart — which is
    /// the exact scenario counting attempts exists to stop. Service Bus and JetStream both persist a delivery
    /// count across a redelivery, so resetting it here would also be more forgiving than the places this is
    /// meant to be swapped for.
    /// </remarks>
    private static void RecoverInFlight(SqliteConnection connection, string topic)
    {
        using var transaction = connection.BeginTransaction();

        using (var entries = Sqlite.Command(connection, transaction,
            "UPDATE entry SET state = $available WHERE topic = $t AND state = $inflight;"))
        {
            entries.Parameters.AddWithValue("$t", topic);
            entries.Parameters.AddWithValue("$available", (int)EntryState.Available);
            entries.Parameters.AddWithValue("$inflight", (int)EntryState.InFlight);
            entries.ExecuteNonQuery();
        }

        using (var aside = Sqlite.Command(connection, transaction,
            "UPDATE set_aside SET delivery = $ready WHERE topic = $t AND delivery = $inflight;"))
        {
            aside.Parameters.AddWithValue("$t", topic);
            aside.Parameters.AddWithValue("$ready", (int)SetAsideDelivery.Ready);
            aside.Parameters.AddWithValue("$inflight", (int)SetAsideDelivery.InFlight);
            aside.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    /// <summary>
    /// Binds this subscriber to a topic, checking that the position it holds belongs to the log it is about to
    /// read. A mismatch means a restore has put the two out of step, and carrying on would silently skip
    /// messages as the log reissues sequences the position already treats as done.
    /// </summary>
    internal async ValueTask<long> AttachTopicAsync(string topic, string logUuid, StartAt start = default,
        CancellationToken ct = default)
        => await _writer.EnqueueAsync((connection, transaction) =>
            Attach(connection, transaction, Subscriber,
                new Attachment(topic, logUuid, start)), ct).ConfigureAwait(false);

    /// <summary>
    /// Records this subscriber's position in a topic, or checks the one it already holds. A starting position
    /// only ever applies to a position being created: once there is one it is the truth, and nothing about how
    /// the subscription happens to be configured this time may move it.
    /// </summary>
    private static long Attach(SqliteConnection connection, SqliteTransaction transaction, string subscriber,
        Attachment attach)
    {
        using (var read = Sqlite.Command(connection, transaction,
            "SELECT log_uuid, mark FROM cursor WHERE topic = $t;"))
        {
            read.Parameters.AddWithValue("$t", attach.Topic);
            using SqliteDataReader reader = read.ExecuteReader();
            if (reader.Read())
            {
                string existing = reader.GetString(0);
                long mark = reader.GetInt64(1);
                if (!string.Equals(existing, attach.LogUuid, StringComparison.Ordinal))
                    throw new LogIdentityException(
                        $"Subscriber '{subscriber}' holds a position in topic '{attach.Topic}' belonging to log " +
                        $"'{existing}', but the log now present is '{attach.LogUuid}'. Refusing to continue: the " +
                        "two are out of step and reading on would skip messages without saying so.");
                return mark;
            }
        }

        using var insert = Sqlite.Command(connection, transaction,
            "INSERT INTO cursor (topic, log_uuid, mark, due_floor, id_floor) VALUES ($t, $u, $m, $d, $i);");
        insert.Parameters.AddWithValue("$t", attach.Topic);
        insert.Parameters.AddWithValue("$u", attach.LogUuid);
        insert.Parameters.AddWithValue("$m", attach.Start.Mark);
        insert.Parameters.AddWithValue("$d", attach.Start.Floor.DueAtMs);
        insert.Parameters.AddWithValue("$i", attach.Start.Floor.Id);
        insert.ExecuteNonQuery();
        return attach.Start.Mark;
    }

    internal long MarkOf(SqliteConnection connection, SqliteTransaction transaction, string topic)
    {
        using var command = Sqlite.Command(connection, transaction,
            "SELECT COALESCE((SELECT mark FROM cursor WHERE topic = $t), 0);");
        command.Parameters.AddWithValue("$t", topic);
        return (long)command.ExecuteScalar()!;
    }

    internal ValueTask<long> ReadMarkAsync(string topic, CancellationToken ct = default)
        => _writer.EnqueueAsync((c, t) => MarkOf(c, t, topic), ct);

    /// <summary>Takes a delivery. Returns the attempt number this delivery is.</summary>
    internal ValueTask<long> ClaimAsync(string topic, EntrySource source, long id, CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                """
                INSERT INTO entry (topic, source, id, state, attempts, updated)
                VALUES ($t, $s, $i, $inflight, 1, $now)
                ON CONFLICT(topic, source, id) DO UPDATE
                  SET state = $inflight, attempts = entry.attempts + 1, updated = $now
                RETURNING attempts;
                """);
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$s", (int)source);
            command.Parameters.AddWithValue("$i", id);
            command.Parameters.AddWithValue("$inflight", (int)EntryState.InFlight);
            command.Parameters.AddWithValue("$now", Sqlite.ToUnixMs(_clock.GetUtcNow()));
            return (long)command.ExecuteScalar()!;
        }, ct);

    /// <summary>Finishes a delivery and lets the mark absorb whatever it now can.</summary>
    internal ValueTask<long> CompleteAsync(string topic, EntrySource source, long id, CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            MarkTerminal(connection, transaction, topic, source, id);
            return AdvanceMark(connection, transaction, topic);
        }, ct);

    /// <summary>Records that a delivery is finished with, whichever way it finished.</summary>
    /// <remarks>
    /// It updates the row the claim left and never writes a new one. Every settle follows a claim, so an entry
    /// that is not there is one a position has already passed: retention overtaking a subscriber mid-delivery
    /// moves the mark over the record and drops its entry, and the settle that arrives afterwards used to put
    /// a row back underneath the mark, where nothing looks and nothing can ever delete it.
    /// </remarks>
    private void MarkTerminal(SqliteConnection connection, SqliteTransaction transaction,
        string topic, EntrySource source, long id)
    {
        using var command = Sqlite.Command(connection, transaction,
            "UPDATE entry SET state = $complete, updated = $now WHERE topic = $t AND source = $s AND id = $i;");
        command.Parameters.AddWithValue("$t", topic);
        command.Parameters.AddWithValue("$s", (int)source);
        command.Parameters.AddWithValue("$i", id);
        command.Parameters.AddWithValue("$complete", (int)EntryState.Complete);
        command.Parameters.AddWithValue("$now", Sqlite.ToUnixMs(_clock.GetUtcNow()));
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Moves the mark over contiguous finished log entries, deleting each as it goes.
    /// </summary>
    /// <remarks>
    /// Contiguous is the whole point. Jumping to the highest finished sequence would swallow everything still
    /// in flight below it — the failure looks like nothing at all until a message that was being processed
    /// during a crash never comes back.
    /// </remarks>
    internal static long AdvanceMark(SqliteConnection connection, SqliteTransaction transaction, string topic)
    {
        long mark;
        using (var read = Sqlite.Command(connection, transaction, "SELECT mark FROM cursor WHERE topic = $t;"))
        {
            read.Parameters.AddWithValue("$t", topic);
            mark = (long)(read.ExecuteScalar() ?? 0L);
        }

        using var next = Sqlite.Command(connection, transaction,
            "SELECT state FROM entry WHERE topic = $t AND source = $s AND id = $i;");
        next.Parameters.AddWithValue("$t", topic);
        next.Parameters.AddWithValue("$s", (int)EntrySource.Log);
        SqliteParameter id = next.Parameters.AddWithValue("$i", mark + 1);

        using var delete = Sqlite.Command(connection, transaction,
            "DELETE FROM entry WHERE topic = $t AND source = $s AND id = $i;");
        delete.Parameters.AddWithValue("$t", topic);
        delete.Parameters.AddWithValue("$s", (int)EntrySource.Log);
        SqliteParameter deleteId = delete.Parameters.AddWithValue("$i", 0L);

        while (true)
        {
            id.Value = mark + 1;
            object? state = next.ExecuteScalar();
            if (state is not long s || (EntryState)s != EntryState.Complete)
                break;

            deleteId.Value = mark + 1;
            delete.ExecuteNonQuery();
            mark++;
        }

        using var write = Sqlite.Command(connection, transaction,
            "UPDATE cursor SET mark = $m WHERE topic = $t;");
        write.Parameters.AddWithValue("$m", mark);
        write.Parameters.AddWithValue("$t", topic);
        write.ExecuteNonQuery();
        return mark;
    }

    /// <summary>
    /// Moves a message out of the flow and into this subscriber's own store, payload and all, then lets the
    /// mark move past it. Copying the payload is what makes a set-aside row survive log retention, and what
    /// makes replay mean "deliver this row" rather than "hope the original is still there".
    /// </summary>
    /// <remarks>
    /// The bounds on the deferred set are read here rather than by the caller, inside the transaction that
    /// inserts the row. A check in one write op and an insert in another is not a bound: two handlers
    /// deferring at the same moment both read the set as it stood before either of them wrote, both find room
    /// and both insert, and a set bounded at one ends up holding as many rows as there are deliveries in
    /// flight. Reading the counts where the row is written makes the answer the caller is given the one that
    /// was true when it was written.
    /// </remarks>
    /// <returns>The row that was written, and what it was filed as — which is dead if a deferral did not fit.</returns>
    internal async ValueTask<(long AsideId, SetAsideKind Kind)> SetAsideAsync(SetAsideKind kind, string topic,
        EntrySource source, long id, string messageId, DateTimeOffset enqueuedAt, long attempts, string? reason,
        ReadOnlyMemory<byte> payload, SetAsidePolicy bounds, CancellationToken ct = default)
    {
        byte[] bytes = payload.ToArray();
        string origin = $"{source.ToString().ToLowerInvariant()}:{id}";
        SetAsideKind filedAs = kind;

        long asideId = await _writer.EnqueueAsync((connection, transaction) =>
        {
            filedAs = kind;
            string? why = reason;

            // Nothing is ever dropped to make room: a message that vanished because a bound was reached is
            // exactly the silent loss the rest of this design goes to some length to avoid. A deferral that
            // does not fit becomes a dead letter, and says so.
            if (filedAs == SetAsideKind.Deferred)
            {
                SetAsideSizes size = SetAsideSize(connection, transaction, topic);
                if (bounds.MaxDeferredCount is { } maxDeferred && size.Deferred >= maxDeferred)
                {
                    filedAs = SetAsideKind.Dead;
                    why = $"deferred set full ({maxDeferred})";
                }
                else if (bounds.MaxDeferredBytes is { } maxBytes && size.DeferredBytes + bytes.Length > maxBytes)
                {
                    // Says what happened to it rather than what was full: this row is joining the store it
                    // was measured against, and a reason reading "the store is full" next to a row that just
                    // grew it is the sort of thing an operator has to read the source to understand.
                    filedAs = SetAsideKind.Dead;
                    why = $"deferring would exceed the deferred byte bound ({maxBytes} bytes); filed dead";
                }
            }

            long written;
            using (var insert = Sqlite.Command(connection, transaction,
                """
                INSERT INTO set_aside (kind, topic, origin, msg_id, enqueued_at, aside_at, delivery, attempts,
                                       reason, payload)
                VALUES ($k, $t, $o, $m, $e, $a, $held, $n, $r, $p)
                RETURNING aside_id;
                """))
            {
                insert.Parameters.AddWithValue("$k", (int)filedAs);
                insert.Parameters.AddWithValue("$t", topic);
                insert.Parameters.AddWithValue("$o", origin);
                insert.Parameters.AddWithValue("$m", messageId);
                insert.Parameters.AddWithValue("$e", Sqlite.ToUnixMs(enqueuedAt));

                // The clock is read here, where the row is written, and not where the call was made. An age
                // stamped before the write queued is an age the row never had: a writer with a backlog longer
                // than the deferred age would file a message that ages out the moment it lands.
                insert.Parameters.AddWithValue("$a", Sqlite.ToUnixMs(_clock.GetUtcNow()));
                insert.Parameters.AddWithValue("$held", (int)SetAsideDelivery.Held);
                insert.Parameters.AddWithValue("$n", attempts);
                insert.Parameters.AddWithValue("$r", (object?)why ?? DBNull.Value);
                insert.Parameters.AddWithValue("$p", bytes);
                written = (long)insert.ExecuteScalar()!;
            }

            if (source is EntrySource.Log or EntrySource.Scheduled)
            {
                MarkTerminal(connection, transaction, topic, source, id);
                AdvanceMark(connection, transaction, topic);
            }

            return written;
        }, ct).ConfigureAwait(false);

        return (asideId, filedAs);
    }

    /// <summary>Lists what is set aside. Reads go through the writer, which keeps one connection per file.</summary>
    internal async ValueTask<List<SetAsideEntry>> ListSetAsideAsync(string topic, SetAsideKind kind,
        CancellationToken ct = default)
    {
        List<SetAsideEntry> rows = [];
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                """
                SELECT aside_id, kind, topic, origin, msg_id, enqueued_at, aside_at, attempts, reason, payload
                FROM set_aside WHERE topic = $t AND kind = $k ORDER BY aside_id;
                """);
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$k", (int)kind);
            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                rows.Add(new SetAsideEntry(
                    reader.GetInt64(0),
                    (SetAsideKind)reader.GetInt32(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4),
                    Sqlite.FromUnixMs(reader.GetInt64(5)),
                    Sqlite.FromUnixMs(reader.GetInt64(6)),
                    reader.GetInt32(7),
                    reader.IsDBNull(8) ? null : reader.GetString(8),
                    (byte[])reader[9]));
            }
            return rows.Count;
        }, ct).ConfigureAwait(false);
        return rows;
    }

    /// <summary>Makes a set-aside row deliverable again. The ordinary read loop then picks it up.</summary>
    /// <remarks>
    /// A row a handler is holding is left alone. Re-arming one would hand the same row to a second handler
    /// under the delivery tag the first is still carrying, and a tag can only be settled once: the second
    /// settle is refused for ever and the in-flight slot behind it never comes back. Service Bus will not let
    /// a deferred message be received twice at once either, so refusing is also what a consumer moving there
    /// will meet.
    /// </remarks>
    /// <returns>True if the message is now waiting to be delivered, false if it is gone or in a handler's hands.</returns>
    internal async ValueTask<bool> ReplayAsync(long asideId, CancellationToken ct = default)
        => await _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                "UPDATE set_aside SET delivery = $ready WHERE aside_id = $i AND delivery <> $inflight;");
            command.Parameters.AddWithValue("$ready", (int)SetAsideDelivery.Ready);
            command.Parameters.AddWithValue("$inflight", (int)SetAsideDelivery.InFlight);
            command.Parameters.AddWithValue("$i", asideId);
            return command.ExecuteNonQuery();
        }, ct).ConfigureAwait(false) > 0;

    /// <summary>Destroys a set-aside message. The only place this library throws something away on purpose.</summary>
    /// <remarks>
    /// Like replay, this leaves a row a handler is holding where it is. Deleting one underneath a delivery
    /// makes every answer that follows it untrue: the settle finds nothing to settle, and a failure below the
    /// attempt limit says the message will be delivered again when there is no longer anything to deliver.
    /// </remarks>
    /// <returns>True if the message was destroyed, false if it was gone already or is in a handler's hands.</returns>
    internal async ValueTask<bool> DiscardAsync(long asideId, CancellationToken ct = default)
        => await _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                "DELETE FROM set_aside WHERE aside_id = $i AND delivery <> $inflight;");
            command.Parameters.AddWithValue("$inflight", (int)SetAsideDelivery.InFlight);
            command.Parameters.AddWithValue("$i", asideId);
            return command.ExecuteNonQuery();
        }, ct).ConfigureAwait(false) > 0;

    /// <summary>
    /// Removes the row behind a delivery that has just been completed, which is the one caller entitled to
    /// destroy a row that is in flight: it is the handler holding it, saying it is finished with.
    /// </summary>
    internal ValueTask<long> RemoveSetAsideAsync(long asideId, CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                "DELETE FROM set_aside WHERE aside_id = $i;");
            command.Parameters.AddWithValue("$i", asideId);
            return command.ExecuteNonQuery();
        }, ct);

    /// <summary>
    /// Takes a delivery only if nothing already holds it. An entry above the mark is either in flight now or
    /// finished but not yet absorbed, and neither should be delivered again.
    /// </summary>
    /// <returns>The attempt number, or 0 if this message is already spoken for.</returns>
    internal ValueTask<long> TryClaimAsync(string topic, EntrySource source, long id, CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                """
                INSERT INTO entry (topic, source, id, state, attempts, updated)
                VALUES ($t, $s, $i, $inflight, 1, $now)
                ON CONFLICT(topic, source, id) DO UPDATE
                  SET state = $inflight, attempts = entry.attempts + 1, updated = $now
                  WHERE entry.state = $available
                RETURNING attempts;
                """);
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$s", (int)source);
            command.Parameters.AddWithValue("$i", id);
            command.Parameters.AddWithValue("$inflight", (int)EntryState.InFlight);
            command.Parameters.AddWithValue("$available", (int)EntryState.Available);
            command.Parameters.AddWithValue("$now", Sqlite.ToUnixMs(_clock.GetUtcNow()));
            return command.ExecuteScalar() as long? ?? 0L;
        }, ct);

    /// <summary>Hands a failed delivery back, leaving the attempt count where it is.</summary>
    /// <remarks>
    /// The row stays rather than being deleted. Deleting it would lose the attempt count, and a message that
    /// fails every time would then be retried forever with the counter starting again on each delivery.
    /// </remarks>
    /// <returns>
    /// One if the delivery was handed back, zero if there was no longer an entry to hand back — which means a
    /// position has passed the record and no read will offer it again.
    /// </returns>
    internal ValueTask<long> ReleaseAsync(string topic, EntrySource source, long id, CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                "UPDATE entry SET state = $available, updated = $now WHERE topic = $t AND source = $s AND id = $i;");
            command.Parameters.AddWithValue("$available", (int)EntryState.Available);
            command.Parameters.AddWithValue("$now", Sqlite.ToUnixMs(_clock.GetUtcNow()));
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$s", (int)source);
            command.Parameters.AddWithValue("$i", id);
            return command.ExecuteNonQuery();
        }, ct);

    /// <summary>
    /// Moves the position forward over messages that no longer exist, dropping any entries it passes.
    /// </summary>
    /// <remarks>
    /// Only for the case where retention removed records this subscriber had not read. The mark advances over
    /// contiguous entries, so leaving it below a gap that can never be filled would freeze it there for good and
    /// the exception set would grow without limit. Acknowledging the loss is the only way back to a consistent
    /// position, which is why it is reported at the same time rather than done quietly.
    /// </remarks>
    internal ValueTask<long> ForceMarkAsync(string topic, long mark, CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using (var entries = Sqlite.Command(connection, transaction,
                "DELETE FROM entry WHERE topic = $t AND source = $s AND id <= $m;"))
            {
                entries.Parameters.AddWithValue("$t", topic);
                entries.Parameters.AddWithValue("$s", (int)EntrySource.Log);
                entries.Parameters.AddWithValue("$m", mark);
                entries.ExecuteNonQuery();
            }

            using var command = Sqlite.Command(connection, transaction,
                "UPDATE cursor SET mark = $m WHERE topic = $t AND mark < $m;");
            command.Parameters.AddWithValue("$m", mark);
            command.Parameters.AddWithValue("$t", topic);
            command.ExecuteNonQuery();
            return AdvanceMark(connection, transaction, topic);
        }, ct);

    /// <summary>
    /// What this subscriber knows about the records above a position: what it is holding, and what it has
    /// finished with but not yet absorbed. One read, so a batch of candidates can be sorted out in memory
    /// rather than by trying to claim each one in turn.
    /// </summary>
    internal async ValueTask<Dictionary<long, EntryState>> EntryStatesAsync(string topic, EntrySource source,
        CancellationToken ct = default)
    {
        Dictionary<long, EntryState> states = [];
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                "SELECT id, state FROM entry WHERE topic = $t AND source = $s;");
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$s", (int)source);
            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
                states[reader.GetInt64(0)] = (EntryState)reader.GetInt32(1);
            return states.Count;
        }, ct).ConfigureAwait(false);
        return states;
    }

    /// <summary>This subscriber's place in the scheduled table.</summary>
    internal async ValueTask<ScheduledFloor> ReadScheduledFloorAsync(string topic, CancellationToken ct = default)
    {
        ScheduledFloor floor = ScheduledFloor.Start;
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                "SELECT due_floor, id_floor FROM cursor WHERE topic = $t;");
            command.Parameters.AddWithValue("$t", topic);
            using SqliteDataReader reader = command.ExecuteReader();
            if (reader.Read())
                floor = new ScheduledFloor(reader.GetInt64(0), reader.GetInt64(1));
            return floor.Id;
        }, ct).ConfigureAwait(false);
        return floor;
    }

    /// <summary>
    /// Moves the scheduled floor over a run of settled rows and drops the entries that recorded them.
    /// </summary>
    /// <remarks>
    /// The same reasoning as the log mark, and the same danger: the run has to be contiguous in the order the
    /// reader takes rows in, or the floor swallows something still in flight and it never comes back. The
    /// caller passes the ids it is absorbing rather than a range, so what is deleted is exactly what it
    /// checked. The floor never moves backwards, which is what makes a stale advance harmless.
    /// </remarks>
    internal ValueTask<long> AbsorbScheduledAsync(string topic, ScheduledFloor floor, IReadOnlyList<long> settled,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using (var delete = Sqlite.Command(connection, transaction,
                "DELETE FROM entry WHERE topic = $t AND source = $s AND id = $i;"))
            {
                delete.Parameters.AddWithValue("$t", topic);
                delete.Parameters.AddWithValue("$s", (int)EntrySource.Scheduled);
                SqliteParameter id = delete.Parameters.AddWithValue("$i", 0L);
                foreach (long settledId in settled)
                {
                    id.Value = settledId;
                    delete.ExecuteNonQuery();
                }
            }

            using var move = Sqlite.Command(connection, transaction,
                """
                UPDATE cursor SET due_floor = $d, id_floor = $i
                WHERE topic = $t AND (due_floor < $d OR (due_floor = $d AND id_floor < $i));
                """);
            move.Parameters.AddWithValue("$d", floor.DueAtMs);
            move.Parameters.AddWithValue("$i", floor.Id);
            move.Parameters.AddWithValue("$t", topic);
            return move.ExecuteNonQuery();
        }, ct);

    /// <summary>
    /// Deletes the entries recording scheduled rows that are no longer in the topic file.
    /// </summary>
    /// <remarks>
    /// An entry is normally deleted by the floor absorbing the row it records, and the floor only moves over
    /// rows it saw as candidates. A row retention removed before that happened never appears as a candidate
    /// again, so its entry has nothing left that could absorb it: it stays for good, and anything reading this
    /// file counts it for good. This is the other way an entry ends, and the only one that is not a position
    /// moving.
    /// </remarks>
    internal ValueTask<long> ForgetScheduledAsync(string topic, IReadOnlyList<long> ids,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var delete = Sqlite.Command(connection, transaction,
                "DELETE FROM entry WHERE topic = $t AND source = $s AND id = $i;");
            delete.Parameters.AddWithValue("$t", topic);
            delete.Parameters.AddWithValue("$s", (int)EntrySource.Scheduled);
            SqliteParameter id = delete.Parameters.AddWithValue("$i", 0L);

            long removed = 0;
            foreach (long gone in ids)
            {
                id.Value = gone;
                removed += delete.ExecuteNonQuery();
            }
            return removed;
        }, ct);

    /// <summary>
    /// How many deliveries this subscriber holds for records between a position and a sequence: what it was
    /// handed and has not settled, and what it has settled but the position has not reached.
    /// </summary>
    /// <remarks>
    /// An entry left <see cref="EntryState.Available"/> is neither of those. A delivery that failed and was
    /// handed back is in nobody's hands and is owed another attempt that only the log can make, so once the
    /// record has gone the message is lost as surely as one that was never read at all — and counting it here
    /// would subtract it from the loss and tell nobody. The exemption belongs to the states where something
    /// still speaks for the message: a handler holding it, or a settle that has already happened.
    /// </remarks>
    internal ValueTask<long> HeldBetweenAsync(string topic, EntrySource source, long above, long below,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                """
                SELECT COUNT(*) FROM entry
                WHERE topic = $t AND source = $s AND id > $a AND id < $b AND state <> $available;
                """);
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$s", (int)source);
            command.Parameters.AddWithValue("$a", above);
            command.Parameters.AddWithValue("$b", below);
            command.Parameters.AddWithValue("$available", (int)EntryState.Available);
            return (long)command.ExecuteScalar()!;
        }, ct);

    /// <summary>Takes a set-aside row that replay made ready.</summary>
    internal async ValueTask<SetAsideEntry?> ClaimReadySetAsideAsync(string topic, CancellationToken ct = default)
    {
        SetAsideEntry? entry = null;
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                """
                UPDATE set_aside SET delivery = $inflight, attempts = attempts + 1
                WHERE aside_id = (
                  SELECT aside_id FROM set_aside
                  WHERE topic = $t AND delivery = $ready ORDER BY aside_id LIMIT 1)
                RETURNING aside_id, kind, topic, origin, msg_id, enqueued_at, aside_at, attempts, reason, payload;
                """);
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$ready", (int)SetAsideDelivery.Ready);
            command.Parameters.AddWithValue("$inflight", (int)SetAsideDelivery.InFlight);
            using SqliteDataReader reader = command.ExecuteReader();
            if (reader.Read())
            {
                entry = new SetAsideEntry(
                    reader.GetInt64(0), (SetAsideKind)reader.GetInt32(1), reader.GetString(2), reader.GetString(3),
                    reader.GetString(4), Sqlite.FromUnixMs(reader.GetInt64(5)), Sqlite.FromUnixMs(reader.GetInt64(6)),
                    reader.GetInt32(7), reader.IsDBNull(8) ? null : reader.GetString(8), (byte[])reader[9]);
            }
            return entry is null ? 0 : 1;
        }, ct).ConfigureAwait(false);
        return entry;
    }

    /// <summary>Puts a set-aside delivery back to held, or to ready if it should be tried again.</summary>
    internal ValueTask<long> SettleSetAsideAsync(long asideId, SetAsideDelivery delivery,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                "UPDATE set_aside SET delivery = $d WHERE aside_id = $i;");
            command.Parameters.AddWithValue("$d", (int)delivery);
            command.Parameters.AddWithValue("$i", asideId);
            return command.ExecuteNonQuery();
        }, ct);

    /// <summary>Marks a set-aside row dead after its replay failed too often.</summary>
    internal ValueTask<long> MakeDeadAsync(long asideId, string reason, CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                "UPDATE set_aside SET kind = $k, delivery = $held, reason = $r, aside_at = $now WHERE aside_id = $i;");
            command.Parameters.AddWithValue("$k", (int)SetAsideKind.Dead);
            command.Parameters.AddWithValue("$held", (int)SetAsideDelivery.Held);
            command.Parameters.AddWithValue("$r", reason);
            command.Parameters.AddWithValue("$now", Sqlite.ToUnixMs(_clock.GetUtcNow()));
            command.Parameters.AddWithValue("$i", asideId);
            return command.ExecuteNonQuery();
        }, ct);

    /// <summary>How much this subscriber currently has set aside: a count and a payload total for each kind.</summary>
    /// <remarks>
    /// Kept apart rather than totalled because the two sets are bounded separately, and they are bounded
    /// separately because only one of them has somewhere to pass a message that does not fit.
    /// </remarks>
    internal async ValueTask<SetAsideSizes> SetAsideSizeAsync(string topic, CancellationToken ct = default)
    {
        SetAsideSizes size = default;
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            size = SetAsideSize(connection, transaction, topic);
            return size.Deferred + size.Dead;
        }, ct).ConfigureAwait(false);
        return size;
    }

    private static SetAsideSizes SetAsideSize(SqliteConnection connection, SqliteTransaction transaction,
        string topic)
    {
        using var command = Sqlite.Command(connection, transaction,
            """
            SELECT
              COALESCE(SUM(CASE WHEN kind = $deferred THEN 1 ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN kind = $dead THEN 1 ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN kind = $deferred THEN LENGTH(payload) ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN kind = $dead THEN LENGTH(payload) ELSE 0 END), 0)
            FROM set_aside WHERE topic = $t;
            """);
        command.Parameters.AddWithValue("$t", topic);
        command.Parameters.AddWithValue("$deferred", (int)SetAsideKind.Deferred);
        command.Parameters.AddWithValue("$dead", (int)SetAsideKind.Dead);
        using SqliteDataReader reader = command.ExecuteReader();
        return reader.Read()
            ? new SetAsideSizes(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3))
            : default;
    }

    /// <summary>
    /// Turns deferred messages that have sat too long into dead letters. They are not destroyed: a deferred
    /// message that quietly evaporated would be the silent loss this whole design exists to avoid.
    /// </summary>
    /// <remarks>
    /// A row a handler is holding has not sat anywhere; it is being worked on now, and the settle that follows
    /// would put a dead letter straight back to ready. Its age is measured again on the next pass, from
    /// wherever that settle leaves it.
    /// </remarks>
    internal ValueTask<long> AgeOutDeferredAsync(string topic, DateTimeOffset cutoff, string reason,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                """
                UPDATE set_aside SET kind = $dead, reason = $r, delivery = $held
                WHERE topic = $t AND kind = $deferred AND aside_at < $cutoff AND delivery <> $inflight;
                """);
            command.Parameters.AddWithValue("$dead", (int)SetAsideKind.Dead);
            command.Parameters.AddWithValue("$deferred", (int)SetAsideKind.Deferred);
            command.Parameters.AddWithValue("$held", (int)SetAsideDelivery.Held);
            command.Parameters.AddWithValue("$inflight", (int)SetAsideDelivery.InFlight);
            command.Parameters.AddWithValue("$r", reason);
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$cutoff", Sqlite.ToUnixMs(cutoff));
            return command.ExecuteNonQuery();
        }, ct);

    /// <summary>Destroys the oldest dead letters down to a limit. Real loss, and the only path here that causes any.</summary>
    /// <remarks>
    /// <para>
    /// It is given the limit and works out the excess itself, inside the one write that does the deleting. A
    /// caller that reads the size and then asks for that many rows to be destroyed is deciding how many to
    /// destroy against a set that other settles are trimming at the same time: each of them measures the same
    /// backlog, each is told to remove it, and between them they empty a set that only had to lose its oldest
    /// few. Same rule as the insert side, on the side that does the destroying.
    /// </para>
    /// <para>
    /// A dead letter someone has replayed and is handling now is not dropped, for the same reason a discard
    /// leaves one alone: the handler is about to be told what happened to a row that would no longer be there.
    /// It still counts towards the limit, so the set can sit one or two above itself while those deliveries
    /// finish, and the next pass trims what they leave behind.
    /// </para>
    /// </remarks>
    internal ValueTask<long> TrimDeadToAsync(string topic, long? count, long? bytes,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            long dropped;
            // Numbered and totalled newest first, so what a bound keeps is the newest of the set and what it
            // drops is everything the newest of the set has pushed out. A row being delivered is numbered and
            // totalled like any other — it still occupies the set — but it is never the row deleted.
            using var command = Sqlite.Command(connection, transaction,
                """
                DELETE FROM set_aside WHERE aside_id IN (
                  SELECT aside_id FROM (
                    SELECT aside_id, delivery,
                           ROW_NUMBER() OVER (ORDER BY aside_id DESC) AS place,
                           SUM(LENGTH(payload)) OVER (ORDER BY aside_id DESC
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
                    FROM set_aside WHERE topic = $t AND kind = $dead)
                  WHERE delivery <> $inflight
                    AND (($count IS NOT NULL AND place > $count)
                      OR ($bytes IS NOT NULL AND running > $bytes)));
                """);
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$dead", (int)SetAsideKind.Dead);
            command.Parameters.AddWithValue("$inflight", (int)SetAsideDelivery.InFlight);
            command.Parameters.AddWithValue("$count", (object?)count ?? DBNull.Value);
            command.Parameters.AddWithValue("$bytes", (object?)bytes ?? DBNull.Value);
            dropped = command.ExecuteNonQuery();

            // Recorded in the same write that destroyed them. A count held in memory is a count a close or a
            // crash takes with it, and what it was counting is loss nobody can reconstruct afterwards: the
            // rows are gone. Kept in the file, it belongs to the subscriber rather than to whichever object
            // happened to be settling at the time, and the next pass reports it whoever runs it.
            if (dropped > 0)
            {
                using var record = Sqlite.Command(connection, transaction,
                    """
                    INSERT INTO meta (key, value) VALUES ($k, $n)
                    ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(value AS INTEGER) + $n AS TEXT);
                    """);
                record.Parameters.AddWithValue("$k", DroppedKey(topic));
                record.Parameters.AddWithValue("$n", dropped.ToString());
                record.ExecuteNonQuery();
            }

            return dropped;
        }, ct);

    /// <summary>How many dead letters have been destroyed for this topic since anyone last asked.</summary>
    /// <remarks>
    /// Read and reset in one write, so two passes cannot both report the same drops and a pass that reports
    /// them cannot leave them behind to be reported again.
    /// </remarks>
    internal ValueTask<long> TakeDroppedAsync(string topic, CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            using var command = Sqlite.Command(connection, transaction,
                "DELETE FROM meta WHERE key = $k RETURNING value;");
            command.Parameters.AddWithValue("$k", DroppedKey(topic));
            return command.ExecuteScalar() is string value && long.TryParse(value, out long dropped)
                ? dropped
                : 0;
        }, ct);

    private static string DroppedKey(string topic) => $"dead_dropped:{topic}";

    /// <summary>Everything this subscriber knows about a topic, for checking the invariant in one go.</summary>
    internal async ValueTask<StoreSnapshot> SnapshotAsync(string topic, CancellationToken ct = default)
    {
        long mark = 0;
        Dictionary<long, EntryState> entries = [];
        List<string> origins = [];

        await _writer.EnqueueAsync((connection, transaction) =>
        {
            mark = MarkOf(connection, transaction, topic);

            using (var read = Sqlite.Command(connection, transaction,
                "SELECT id, state FROM entry WHERE topic = $t AND source = $s;"))
            {
                read.Parameters.AddWithValue("$t", topic);
                read.Parameters.AddWithValue("$s", (int)EntrySource.Log);
                using SqliteDataReader reader = read.ExecuteReader();
                while (reader.Read())
                    entries[reader.GetInt64(0)] = (EntryState)reader.GetInt32(1);
            }

            using (var aside = Sqlite.Command(connection, transaction,
                "SELECT origin FROM set_aside WHERE topic = $t;"))
            {
                aside.Parameters.AddWithValue("$t", topic);
                using SqliteDataReader reader = aside.ExecuteReader();
                while (reader.Read())
                    origins.Add(reader.GetString(0));
            }

            return mark;
        }, ct).ConfigureAwait(false);

        return new StoreSnapshot(mark, entries, origins);
    }

    public async ValueTask DisposeAsync()
    {
        await _writer.DisposeAsync().ConfigureAwait(false);

        if (_closed || _topic is null)
            return;
        _closed = true;
        Release(Path, _topic);
    }
}

/// <summary>Where a subscriber that has never read a topic before begins in it.</summary>
internal readonly record struct StartAt(long Mark, ScheduledFloor Floor);

/// <summary>A subscriber binding itself to one topic, with the position to take if it has none yet.</summary>
internal readonly record struct Attachment(string Topic, string LogUuid, StartAt Start);

/// <summary>A subscriber's whole view of one topic at a moment in time.</summary>
internal sealed record StoreSnapshot(long Mark, Dictionary<long, EntryState> Entries, List<string> SetAsideOrigins);

/// <summary>What one subscriber has set aside: a count and a payload total for each of the two sets.</summary>
internal readonly record struct SetAsideSizes(long Deferred, long Dead, long DeferredBytes, long DeadBytes);
