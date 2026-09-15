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

    /// <summary>The identity each path spelling this process has opened resolves to. See <see cref="Key"/>.</summary>
    private static readonly Dictionary<string, string> Identities = new(StringComparer.Ordinal);

    private readonly SingleWriter _writer;
    private readonly MonotonicClock _clock;
    private readonly string? _topic;
    private int _closed;

    private SubscriberStore(string subscriber, string path, string? topic, SingleWriter writer,
        MonotonicClock clock)
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
                // Worked out before the lock is taken: naming the file asks the filesystem a question the
                // first time it sees a path, and every other store in this process opening any subscriber
                // would be waiting behind it.
                string key = Key(path, topic);
                lock (Reading)
                {
                    Reading.TryGetValue(key, out int already);
                    if (already == 0)
                        RecoverInFlight(connection, topic);
                    Reading[key] = already + 1;
                    counted = true;
                }
            }

            if (attach is { } binding)
            {
                using SqliteTransaction transaction = connection.BeginTransaction();
                Attach(connection, transaction, subscriber, binding);
                transaction.Commit();
            }

            // Seeded from this file's own stamps, for the reason the topic file is: a deferral aged out from
            // a stamp older than the event it describes can reach destruction under a dead bound.
            var stamps = new MonotonicClock(options.Clock, NewestStamp(connection));

            return new SubscriberStore(subscriber, path, topic,
                new SingleWriter(connection, options.MaxAppendBatch), stamps);
        }
        catch
        {
            if (counted)
                Release(path, topic!);
            connection.Dispose();
            throw;
        }
    }

    /// <summary>
    /// How the file and topic a store is reading are named to the guard above. The path is folded to one
    /// identity where the filesystem holding it ignores case, so two components configured with roots that
    /// differ only in spelling find each other rather than each believing the other's deliveries are stale.
    /// Asked once per path and remembered: the answer cannot change while the file is open, and a store
    /// releasing its count has to name what it registered even if the file has since been deleted.
    /// </summary>
    private static string Key(string path, string topic)
    {
        lock (Identities)
        {
            if (!Identities.TryGetValue(path, out string? identity))
                Identities[path] = identity = FileNames.Identity(path);
            return identity + "\u0000" + topic;
        }
    }

    /// <summary>
    /// The newest moment this file has already been stamped with. Only the two stamps this store writes are
    /// consulted: a set-aside row also carries the moment the topic published it, which belongs to another
    /// file's clock.
    /// </summary>
    private static long NewestStamp(SqliteConnection connection)
    {
        using SqliteCommand command = Sqlite.Command(connection, null,
            """
            SELECT MAX(newest) FROM (
              SELECT COALESCE(MAX(updated), 0) AS newest FROM entry
              UNION ALL
              SELECT COALESCE(MAX(aside_at), 0) FROM set_aside
            );
            """);
        return command.ExecuteScalar() is long value ? value : 0;
    }

    private static void Release(string path, string topic)
    {
        string key = Key(path, topic);
        lock (Reading)
        {
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
        command.Parameters.AddWithValue("$now", _clock.NowMs());
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
    /// <returns>
    /// The row that was written and what it was filed as — which is dead if a deferral did not fit — or a
    /// refusal, when what it would have been filed as is something this subscriber cannot keep.
    /// </returns>
    /// <remarks>
    /// A refusal is returned rather than thrown from inside the write. The exception belongs to the caller
    /// that caused it, and throwing here would roll the whole batch back: everyone else's work would fail
    /// with somebody else's refusal, and a bound being reached is an ordinary thing that happens under load
    /// rather than a fault. The decision is still made inside the transaction, against the set the row would
    /// have joined.
    /// </remarks>
    internal async ValueTask<SetAsideOutcome> SetAsideAsync(SetAsideKind kind, string topic,
        EntrySource source, long id, string messageId, DateTimeOffset enqueuedAt, long attempts, string? reason,
        ReadOnlyMemory<byte> payload, SetAsidePolicy bounds, CancellationToken ct = default)
    {
        byte[] bytes = payload.ToArray();
        string origin = $"{source.ToString().ToLowerInvariant()}:{id}";
        SetAsideKind filedAs = kind;
        bool refused = false;
        SetAsideSizes sizes = default;

        long asideId = await _writer.EnqueueAsync((connection, transaction) =>
        {
            filedAs = kind;
            refused = false;
            sizes = default;
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

            // Filed dead and too big to be kept: this subscriber destroys the oldest to stay inside its byte
            // bound, and this row is over that bound on its own, so the trim takes it whatever room the set
            // has. Writing it would be storing the message in order to lose it, so nothing is written and the
            // caller is told. What it would cost the rest of the set is nothing either way — an unkeepable
            // row is charged to nothing — so the refusal is about this message and not about the others.
            if (filedAs == SetAsideKind.Dead
                && bounds.DeadOverflow == DeadLetterOverflow.DropOldest
                && bounds.MaxDeadBytes is { } maxDead && bytes.Length > maxDead)
            {
                refused = true;
                return 0;
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
                insert.Parameters.AddWithValue("$a", _clock.NowMs());
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

            sizes = EnforceDead(connection, transaction, topic, filedAs, bounds, written);
            return written;
        }, ct).ConfigureAwait(false);

        return new SetAsideOutcome(asideId, filedAs, refused, sizes);
    }

    /// <summary>
    /// Holds the dead bound to the set inside the write that just changed it, and answers with the sizes the
    /// caller needs to decide whether the subscriber should stop.
    /// </summary>
    /// <remarks>
    /// This runs here, in the transaction that filed the row, rather than in a second write op afterwards. A
    /// settle made of two ops can commit the first and then fail the second — a cancelled token is checked at
    /// the door of every op, so the ordinary way to hit it is to cancel a settle whose insert is already
    /// queued — and the caller is then told the settle failed over an effect that is durable. It retries, and
    /// the same message is filed dead twice. One op cannot half-happen, so there is nothing left to fail.
    /// </remarks>
    private static SetAsideSizes EnforceDead(SqliteConnection connection, SqliteTransaction transaction,
        string topic, SetAsideKind filedAs, SetAsidePolicy bounds, long filed)
    {
        if (filedAs != SetAsideKind.Dead || (bounds.MaxDeadCount is null && bounds.MaxDeadBytes is null))
            return default;

        // A limit is a maximum, so dropping trims back to it and stopping happens on reaching it — a
        // subscriber that stops has no way to take another one anyway.
        if (bounds.DeadOverflow == DeadLetterOverflow.DropOldest)
        {
            TrimDeadTo(connection, transaction, topic, bounds.MaxDeadCount, bounds.MaxDeadBytes, filed);
            return default;
        }

        return SetAsideSize(connection, transaction, topic);
    }

    /// <summary>
    /// Re-files a row already in this store as something else, and holds the bounds to what that leaves.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One write op, for the reason <see cref="EnforceDead"/> gives: the settles that reach here are the ones
    /// that give a delivery back when they throw, so anything that can throw after the row has moved is a
    /// failure reported over a durable effect.
    /// </para>
    /// <para>
    /// The deferred bounds are consulted only when the row is joining the deferred set from outside it. A row
    /// re-filed as what it already is adds neither a row nor a byte, and measuring it against a set it is
    /// already inside is how a message replayed and deferred again at the bound was dead-lettered for a set
    /// that was not growing. A row moving the other way — dead to deferred — is genuinely joining, so it is
    /// measured, and a deferral that does not fit stays dead and says so, exactly as one arriving fresh does.
    /// </para>
    /// <para>
    /// The keepability refusal is made here as well, and on the same terms as everywhere else: a row is
    /// refused when it is joining the dead set and is larger than the whole byte bound. Knowing what the row
    /// already is, is the reason the decision belongs in this write rather than at the call site. A caller
    /// cannot tell a deferral being downgraded — which is arriving in the dead set, and meets the refusal a
    /// fresh arrival meets — from a dead letter being re-filed as what it already is, which joins nothing:
    /// refusing that one gives a reason untrue of it, since the row is already held and keeping it costs the
    /// set nothing, and it leaves a replayed dead letter no way back into the store it came out of.
    /// </para>
    /// </remarks>
    internal async ValueTask<SetAsideOutcome> RefileAsync(long asideId, string topic, SetAsideKind kind,
        string? reason, SetAsidePolicy bounds, CancellationToken ct = default)
    {
        SetAsideKind filedAs = kind;
        SetAsideSizes sizes = default;
        bool refused = false;

        await _writer.EnqueueAsync((connection, transaction) =>
        {
            filedAs = kind;
            sizes = default;
            refused = false;
            string? why = reason;

            SetAsideKind was;
            long length;
            using (var current = Sqlite.Command(connection, transaction,
                "SELECT kind, LENGTH(payload) FROM set_aside WHERE aside_id = $i;"))
            {
                current.Parameters.AddWithValue("$i", asideId);
                using SqliteDataReader reader = current.ExecuteReader();
                if (!reader.Read())
                    return 0;   // discarded, or destroyed by a bound, while the handler held it
                was = (SetAsideKind)reader.GetInt32(0);
                length = reader.GetInt64(1);
            }

            if (filedAs == SetAsideKind.Deferred && was != SetAsideKind.Deferred)
            {
                SetAsideSizes size = SetAsideSize(connection, transaction, topic);
                if (bounds.MaxDeferredCount is { } maxDeferred && size.Deferred >= maxDeferred)
                {
                    filedAs = SetAsideKind.Dead;
                    why = $"deferred set full ({maxDeferred})";
                }
                else if (bounds.MaxDeferredBytes is { } maxBytes && size.DeferredBytes + length > maxBytes)
                {
                    filedAs = SetAsideKind.Dead;
                    why = $"deferring would exceed the deferred byte bound ({maxBytes} bytes); left dead";
                }
            }

            // Joining the dead set and too big for this subscriber to hold: nothing is written, for the
            // reason the fresh arrival is turned away. A row already filed dead is not joining it, and is
            // re-filed as what it already is without complaint — the set it is in is the set it stays in.
            if (filedAs == SetAsideKind.Dead && was != SetAsideKind.Dead
                && bounds.DeadOverflow == DeadLetterOverflow.DropOldest
                && bounds.MaxDeadBytes is { } maxDead && length > maxDead)
            {
                refused = true;
                return 0;
            }

            using (var command = Sqlite.Command(connection, transaction,
                """
                UPDATE set_aside SET kind = $k, delivery = $held, reason = $r, aside_at = $now
                WHERE aside_id = $i;
                """))
            {
                command.Parameters.AddWithValue("$k", (int)filedAs);
                command.Parameters.AddWithValue("$held", (int)SetAsideDelivery.Held);
                command.Parameters.AddWithValue("$r", (object?)why ?? DBNull.Value);

                // Re-stamped, because the age bounds measure how long a row has sat as what it now is: a
                // message deferred a moment ago has not been sitting there since it was first set aside.
                command.Parameters.AddWithValue("$now", _clock.NowMs());
                command.Parameters.AddWithValue("$i", asideId);
                command.ExecuteNonQuery();
            }

            sizes = EnforceDead(connection, transaction, topic, filedAs, bounds, asideId);
            return 1;
        }, ct).ConfigureAwait(false);

        return new SetAsideOutcome(asideId, filedAs, refused, sizes);
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
    /// Takes a delivery only if nothing already holds it and the position has not passed it. An entry above
    /// the mark is either in flight now or finished but not yet absorbed, and neither should be delivered
    /// again.
    /// </summary>
    /// <remarks>
    /// The position is checked inside this statement and not by the caller, because the caller's reading of it
    /// is stale by the time the claim runs. Choosing a candidate and claiming it are separate write ops, and a
    /// second reader of the same subscriber — a sibling instance, or a second enumerator — can complete the
    /// same record in between, whereupon the mark absorbs it and <em>deletes its entry</em>. An unguarded
    /// upsert then reads that absence as "never delivered" and inserts a fresh claim below the mark: the
    /// message goes to a second handler on a path where nothing failed, and the row it leaves behind sits
    /// where no position will ever absorb it — counted by every poll and every inspection for the life of the
    /// file, and answered over by <c>ReleaseAsync</c> as if a redelivery were coming that no read will offer.
    /// Selecting from the cursor row makes the claim and the check one statement: a log claim is valid only
    /// above the mark, a scheduled one only above the floor, which needs the row's due time because the floor
    /// orders by due time and then id — the same comparison <c>TopicReader.Due</c> makes.
    /// </remarks>
    /// <returns>The attempt number, or 0 if this message is already spoken for or already absorbed.</returns>
    internal ValueTask<long> TryClaimAsync(string topic, EntrySource source, long id, long dueAtMs,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            string above = source == EntrySource.Scheduled
                ? "(due_floor < $d OR (due_floor = $d AND id_floor < $i))"
                : "mark < $i";
            using var command = Sqlite.Command(connection, transaction,
                $"""
                INSERT INTO entry (topic, source, id, state, attempts, updated)
                SELECT $t, $s, $i, $inflight, 1, $now FROM cursor
                WHERE topic = $t AND {above}
                ON CONFLICT(topic, source, id) DO UPDATE
                  SET state = $inflight, attempts = entry.attempts + 1, updated = $now
                  WHERE entry.state = $available
                RETURNING attempts;
                """);
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$s", (int)source);
            command.Parameters.AddWithValue("$i", id);
            if (source == EntrySource.Scheduled)
                command.Parameters.AddWithValue("$d", dueAtMs);
            command.Parameters.AddWithValue("$inflight", (int)EntryState.InFlight);
            command.Parameters.AddWithValue("$available", (int)EntryState.Available);
            command.Parameters.AddWithValue("$now", _clock.NowMs());
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
            command.Parameters.AddWithValue("$now", _clock.NowMs());
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$s", (int)source);
            command.Parameters.AddWithValue("$i", id);
            return command.ExecuteNonQuery();
        }, ct);

    /// <summary>
    /// Measures the loss below a surviving minimum, moves the position over it, and drops the entries it
    /// passes — in the one write, so the gap that is measured is the gap that is closed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Only for the case where retention removed records this subscriber had not read. The mark advances over
    /// contiguous entries, so leaving it below a gap that can never be filled would freeze it there for good
    /// and the exception set would grow without limit. Acknowledging the loss is the only way back to a
    /// consistent position, which is why it is reported at the same time rather than done quietly.
    /// </para>
    /// <para>
    /// Measuring and moving used to be separate ops, and two readers of the same subscriber — a sibling
    /// instance, or a second enumerator — interleaved them: both read the un-moved mark, both measured the
    /// same gap, and both reported it, doubling the one number the loss contract says an operator can act on.
    /// In one op, whichever call finds the mark already moved measures a gap of nothing and reports nothing:
    /// the report belongs to the call that moved the position. The opening no-op write is part of that — it
    /// takes the file's write lock before the mark is read, so a sibling's move lands before this
    /// measurement rather than between it and the write behind it.
    /// </para>
    /// </remarks>
    /// <returns>The moved mark, how many records were lost, and the bounds of the gap they were lost in.</returns>
    internal async ValueTask<(long Mark, long Lost, long First, long Last)> SettleLossAsync(string topic,
        long min, CancellationToken ct = default)
    {
        long moved = 0, lost = 0, first = 0, last = 0;
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            moved = 0; lost = 0; first = 0; last = 0;

            using (var touch = Sqlite.Command(connection, transaction,
                "UPDATE cursor SET mark = mark WHERE topic = $t;"))
            {
                touch.Parameters.AddWithValue("$t", topic);
                touch.ExecuteNonQuery();
            }

            long mark = MarkOf(connection, transaction, topic);
            if (min <= mark + 1)
            {
                moved = mark;
                return 0;
            }

            // Records a handler is holding, or has already settled, are not lost to this subscriber however
            // they left the log: counting them would name messages someone has in their hands as ones that
            // never arrived. An entry left Available is neither — a delivery that failed and was handed back
            // is owed another attempt only the log can make. Read before the delete below takes them, and
            // read as ids rather than as a number so the gap this reports is the gap it counted.
            var held = new HashSet<long>();
            using (var rows = Sqlite.Command(connection, transaction,
                """
                SELECT id FROM entry
                WHERE topic = $t AND source = $s AND id > $a AND id < $b AND state <> $available;
                """))
            {
                rows.Parameters.AddWithValue("$t", topic);
                rows.Parameters.AddWithValue("$s", (int)EntrySource.Log);
                rows.Parameters.AddWithValue("$a", mark);
                rows.Parameters.AddWithValue("$b", min);
                rows.Parameters.AddWithValue("$available", (int)EntryState.Available);
                using SqliteDataReader reader = rows.ExecuteReader();
                while (reader.Read())
                    held.Add(reader.GetInt64(0));
            }

            // Everything in the gap except the deliveries that are still out. Those are left where they are
            // deliberately: the mark is about to move past them, and an entry is then the only remaining
            // record that this subscriber was handed the message at all. Deleting it — which is what used to
            // happen — meant the settle that followed found nothing to hand back, so the failure was answered
            // honestly and the message was counted by nobody: not here, because a handler held it when this
            // ran, and not afterwards, because nothing was left to notice. What is left instead is swept by
            // the pass over entries below the mark, which reports the ones still owed and is where the second
            // ending for these lives. Bounded below by the mark as well, so an entry a previous gap left
            // behind is that pass's to report and is not quietly taken from underneath it here.
            using (var entries = Sqlite.Command(connection, transaction,
                """
                DELETE FROM entry
                WHERE topic = $t AND source = $s AND id > $a AND id <= $m AND state <> $inflight;
                """))
            {
                entries.Parameters.AddWithValue("$t", topic);
                entries.Parameters.AddWithValue("$s", (int)EntrySource.Log);
                entries.Parameters.AddWithValue("$a", mark);
                entries.Parameters.AddWithValue("$m", min - 1);
                entries.Parameters.AddWithValue("$inflight", (int)EntryState.InFlight);
                entries.ExecuteNonQuery();
            }

            using (var move = Sqlite.Command(connection, transaction,
                "UPDATE cursor SET mark = $m WHERE topic = $t AND mark < $m;"))
            {
                move.Parameters.AddWithValue("$m", min - 1);
                move.Parameters.AddWithValue("$t", topic);
                move.ExecuteNonQuery();
            }

            moved = AdvanceMark(connection, transaction, topic);
            lost = Math.Max(0, min - mark - 1 - held.Count);

            // The bounds name records this report counted, and not the span it looked at. An operator given a
            // range that takes in messages the subscriber is holding or has already dealt with goes looking
            // for messages that are not missing.
            first = mark + 1;
            while (first < min && held.Contains(first))
                first++;
            last = min - 1;
            while (last > mark && held.Contains(last))
                last--;
            return lost;
        }, ct).ConfigureAwait(false);
        return (moved, lost, first, last);
    }

    /// <summary>
    /// Clears the log entries the position has already passed, and says which of them were still owed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The mark only moves over an entry once there is nothing left to do about it, with one exception: the
    /// settle above moves it over a gap retention left, and it leaves the deliveries that were out at the
    /// time to be settled afterwards. Those entries come to rest underneath the mark, where no claim can
    /// reach them — the claim guard is what makes that true — and nothing else will ever absorb them. This is
    /// their ending, and which ending it is depends on how the delivery finished. Finished with is finished
    /// with, however the record left the log. Handed back is a redelivery this subscriber was promised and
    /// the log can no longer make, which is the definition of the loss it reports. A crash reaches the same
    /// place: recovery hands its in-flight deliveries back, and the ones under the mark are owed.
    /// </para>
    /// <para>
    /// The mark is read inside the delete and the rows are named by what the delete removed, so the report
    /// belongs to the caller that removed them. A sibling, or a second enumerator, running this at the same
    /// moment takes no rows and reports nothing rather than reporting the same loss over again. A delivery
    /// claimed since the caller looked is left alone by the state clause for the same reason it is left alone
    /// by the settle above: naming a message as lost while a handler holds it is the mistake in the report
    /// that the guard exists to stop in the file.
    /// </para>
    /// </remarks>
    internal async ValueTask<List<long>> ForgetStrandedAsync(string topic, CancellationToken ct = default)
    {
        List<long> owed = [];
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            owed.Clear();
            using var command = Sqlite.Command(connection, transaction,
                """
                DELETE FROM entry
                WHERE topic = $t AND source = $s AND state <> $inflight
                  AND id <= (SELECT mark FROM cursor WHERE topic = $t)
                RETURNING id, state;
                """);
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$s", (int)EntrySource.Log);
            command.Parameters.AddWithValue("$inflight", (int)EntryState.InFlight);
            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                if ((EntryState)reader.GetInt32(1) == EntryState.Available)
                    owed.Add(reader.GetInt64(0));
            }
            return owed.Count;
        }, ct).ConfigureAwait(false);
        return owed;
    }

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
    /// <returns>The ids whose entry this actually removed, which is not always the ids it was given.</returns>
    internal async ValueTask<List<long>> ForgetScheduledAsync(string topic, IReadOnlyList<long> ids,
        CancellationToken ct = default)
    {
        List<long> forgotten = [];
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            forgotten.Clear();

            // The in-flight guard belongs in the statement and not only in the caller's reading of the states
            // it last saw. What to forget is decided from a set read a moment earlier, and a delivery claimed
            // since — by a sibling on the same subscriber and topic — would otherwise have its entry deleted
            // underneath it and be named as owed-and-lost while a handler was holding it. Same rule as
            // everywhere else here: nothing acts on a row while a handler has it.
            using var delete = Sqlite.Command(connection, transaction,
                "DELETE FROM entry WHERE topic = $t AND source = $s AND id = $i AND state <> $inflight;");
            delete.Parameters.AddWithValue("$t", topic);
            delete.Parameters.AddWithValue("$s", (int)EntrySource.Scheduled);
            delete.Parameters.AddWithValue("$inflight", (int)EntryState.InFlight);
            SqliteParameter id = delete.Parameters.AddWithValue("$i", 0L);

            foreach (long gone in ids)
            {
                id.Value = gone;
                if (delete.ExecuteNonQuery() > 0)
                    forgotten.Add(gone);
            }
            return forgotten.Count;
        }, ct).ConfigureAwait(false);
        return forgotten;
    }

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
              COALESCE(SUM(CASE WHEN kind = $dead THEN LENGTH(payload) ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN kind = $dead AND delivery = $held THEN 1 ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN kind = $dead AND delivery = $held THEN LENGTH(payload) ELSE 0 END), 0)
            FROM set_aside WHERE topic = $t;
            """);
        command.Parameters.AddWithValue("$t", topic);
        command.Parameters.AddWithValue("$deferred", (int)SetAsideKind.Deferred);
        command.Parameters.AddWithValue("$dead", (int)SetAsideKind.Dead);
        command.Parameters.AddWithValue("$held", (int)SetAsideDelivery.Held);
        using SqliteDataReader reader = command.ExecuteReader();
        return reader.Read()
            ? new SetAsideSizes(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3),
                reader.GetInt64(4), reader.GetInt64(5))
            : default;
    }

    /// <summary>
    /// Turns deferred messages that have sat too long into dead letters. They are not destroyed: a deferred
    /// message that quietly evaporated would be the silent loss this whole design exists to avoid.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A row a handler is holding is left alone. It has not sat anywhere; it is being worked on now, and the
    /// settle that follows would put a dead letter straight back to ready. Its age is measured again on the
    /// next pass, from wherever that settle leaves it.
    /// </para>
    /// <para>
    /// Everything else keeps whatever delivery state it had. The pass used to park each row it changed, which
    /// silently cancelled a replay: a deferral an operator had asked to see again was turned into a held dead
    /// letter and the request evaporated without a word. Only the kind changes now, so a row on its way out is
    /// still on its way out and the next read delivers it — as a dead letter, which is what replaying one has
    /// always meant. Leaving the row alone instead would have been the other way to stop cancelling the
    /// replay, and it is the worse one: a delivery interrupted by a restart comes back as ready, so every
    /// deferral a redeploy caught mid-flight would have been exempt from the age bound for as long as nobody
    /// read the subscriber — which, for a subscriber stopped at a full dead set, is indefinitely.
    /// </para>
    /// </remarks>
    internal ValueTask<long> AgeOutDeferredAsync(string topic, DateTimeOffset cutoff, string reason,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            long aged;
            using (var command = Sqlite.Command(connection, transaction,
                """
                UPDATE set_aside SET kind = $dead, reason = $r
                WHERE topic = $t AND kind = $deferred AND aside_at < $cutoff AND delivery <> $inflight;
                """))
            {
                command.Parameters.AddWithValue("$dead", (int)SetAsideKind.Dead);
                command.Parameters.AddWithValue("$deferred", (int)SetAsideKind.Deferred);
                command.Parameters.AddWithValue("$inflight", (int)SetAsideDelivery.InFlight);
                command.Parameters.AddWithValue("$r", reason);
                command.Parameters.AddWithValue("$t", topic);
                command.Parameters.AddWithValue("$cutoff", Sqlite.ToUnixMs(cutoff));
                aged = command.ExecuteNonQuery();
            }

            // Recorded in the file, in the write that did it, for the reason the drop count is. A pass is
            // three writes, and one that failed after this one — a cancelled token is checked at the door of
            // every op, so a host handing its shutdown token to a maintenance pass is the ordinary way there
            // — used to report nothing at all, and the next pass had nothing left to count: the deferrals
            // were already dead by then and no arithmetic could recover the number.
            if (aged > 0)
                Bump(connection, transaction, AgedKey(topic), aged);

            return aged;
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
    /// <para>
    /// A row that has been replayed but not yet handed out is not spared, and that is a decision rather than
    /// an oversight. Nothing has been told anything about it that this could falsify — the answer replay gave
    /// was about the store as it stood, the same kind of answer an append gives about a record retention may
    /// later reach. And the exemption cannot be widened to cover it safely: deliveries in flight are capped by
    /// <c>MaxInFlight</c>, whereas any number of rows can be made ready at once, so sparing them would let a
    /// caller who replayed a whole set suspend the bound entirely. A bound that can be switched off is worse
    /// than loss that is counted, which is what this option exists to provide.
    /// </para>
    /// </remarks>
    internal ValueTask<long> TrimDeadToAsync(string topic, long? count, long? bytes,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction)
            => TrimDeadTo(connection, transaction, topic, count, bytes, filed: null), ct);

    /// <inheritdoc cref="TrimDeadToAsync"/>
    /// <remarks>
    /// Given the connection rather than the writer, so that the settle which filed the row can trim inside the
    /// write that filed it. What a bound destroys and what put the set over it belong in one transaction.
    /// </remarks>
    private static long TrimDeadTo(SqliteConnection connection, SqliteTransaction transaction, string topic,
        long? count, long? bytes, long? filed)
    {
        long dropped;
        // Numbered and totalled newest first, so what a bound keeps is the newest of the set and what it
        // drops is everything the newest of the set has pushed out. A row being delivered is numbered and
        // totalled like any other — it still occupies the set — but it is never the row deleted.
        //
        // The row this write just filed is the newest arrival whatever its aside_id says, which is the whole
        // of $filed. A re-filed row keeps the id it was given when it was first set aside, so ordering by id
        // alone made a replayed message settled dead the oldest row in the set by construction: the trim its
        // own settle triggers destroyed it, and the settle answered "filed" about a message that had gone.
        // Sorting it to the front is what fixes that — numbered first and totalled first, it is kept and what
        // it pushes out is the oldest of the rest, exactly as a fresh insert has always behaved.
        //
        // Excluding it from the delete as well makes "a settle never destroys the row it is filing" true of
        // this statement rather than true only while every route into the set turns away a row it could not
        // keep. It is here because that would otherwise be a guarantee assembled out of call sites and a
        // validation rule, and the defect it was written for is what that looks like when one of them is a
        // bound out of step with the one that fires.
        //
        // A row whose own payload is larger than the whole byte bound is charged to nothing and destroyed
        // wherever it sits. Totalling it like any other row made one unkeepable row push the running sum
        // past the bound for every row older than it, so a set that was comfortably inside the bound was
        // emptied by a single arrival it could never have kept — the age-out pass files exactly such a row,
        // and the maintenance trim that followed took the rest of the set with it. Both bounds now count
        // only what this subscriber can keep, so the collateral is gone and what is destroyed is the row
        // that cannot be held, counted as the loss it is. The delete says so directly rather than relying on
        // the sum, because a row too big for the bound is over it alone and stays over it however the set
        // around it shrinks; the in-flight and $filed exclusions still come first, since a handler about to
        // be told what happened to a row, and the row a write is filing, are spared whatever their size.
        using (var command = Sqlite.Command(connection, transaction,
            """
            DELETE FROM set_aside WHERE aside_id IN (
              SELECT aside_id FROM (
                SELECT aside_id, delivery, unkeepable,
                       SUM(CASE WHEN unkeepable THEN 0 ELSE 1 END)
                         OVER (ORDER BY aside_id = $filed DESC, aside_id DESC
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS place,
                       SUM(CASE WHEN unkeepable THEN 0 ELSE size END)
                         OVER (ORDER BY aside_id = $filed DESC, aside_id DESC
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
                FROM (SELECT aside_id, delivery, LENGTH(payload) AS size,
                             ($bytes IS NOT NULL AND LENGTH(payload) > $bytes) AS unkeepable
                      FROM set_aside WHERE topic = $t AND kind = $dead))
              WHERE delivery <> $inflight
                AND ($filed IS NULL OR aside_id <> $filed)
                AND (unkeepable
                  OR ($count IS NOT NULL AND place > $count)
                  OR ($bytes IS NOT NULL AND running > $bytes)));
            """))
        {
            command.Parameters.AddWithValue("$t", topic);
            command.Parameters.AddWithValue("$dead", (int)SetAsideKind.Dead);
            command.Parameters.AddWithValue("$inflight", (int)SetAsideDelivery.InFlight);
            command.Parameters.AddWithValue("$count", (object?)count ?? DBNull.Value);
            command.Parameters.AddWithValue("$bytes", (object?)bytes ?? DBNull.Value);
            command.Parameters.AddWithValue("$filed", (object?)filed ?? DBNull.Value);
            dropped = command.ExecuteNonQuery();
        }

        // Recorded in the same write that destroyed them. A count held in memory is a count a close or a
        // crash takes with it, and what it was counting is loss nobody can reconstruct afterwards: the
        // rows are gone. Kept in the file, it belongs to the subscriber rather than to whichever object
        // happened to be settling at the time, and the next pass reports it whoever runs it.
        if (dropped > 0)
            Bump(connection, transaction, DroppedKey(topic), dropped);

        return dropped;
    }

    /// <summary>
    /// What has happened to this subscriber's set-aside store since anyone last asked: deferrals aged into
    /// dead letters, and dead letters destroyed by a bound.
    /// </summary>
    /// <remarks>
    /// Read and reset in one write, so two passes cannot both report the same numbers and a pass that reports
    /// them cannot leave them behind to be reported again. The two are taken together because both are now
    /// counted the same way — by whatever did the work, in the write that did it — and a pass that took one
    /// from the file and the other from a local variable lost the local one every time it failed part-way.
    /// </remarks>
    internal async ValueTask<(long AgedOut, long Dropped)> TakeCountersAsync(string topic,
        CancellationToken ct = default)
    {
        long aged = 0, dropped = 0;
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            aged = Take(connection, transaction, AgedKey(topic));
            dropped = Take(connection, transaction, DroppedKey(topic));
            return aged + dropped;
        }, ct).ConfigureAwait(false);
        return (aged, dropped);
    }

    private static long Take(SqliteConnection connection, SqliteTransaction transaction, string key)
    {
        using var command = Sqlite.Command(connection, transaction,
            "DELETE FROM meta WHERE key = $k RETURNING value;");
        command.Parameters.AddWithValue("$k", key);
        return command.ExecuteScalar() is string value && long.TryParse(value, out long count) ? count : 0;
    }

    private static void Bump(SqliteConnection connection, SqliteTransaction transaction, string key, long by)
    {
        using var command = Sqlite.Command(connection, transaction,
            """
            INSERT INTO meta (key, value) VALUES ($k, $n)
            ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(value AS INTEGER) + $n AS TEXT);
            """);
        command.Parameters.AddWithValue("$k", key);
        command.Parameters.AddWithValue("$n", by.ToString());
        command.ExecuteNonQuery();
    }

    private static string DroppedKey(string topic) => $"dead_dropped:{topic}";

    private static string AgedKey(string topic) => $"deferred_aged:{topic}";

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

    /// <summary>
    /// Closes the file and gives back the count this store holds against it.
    /// </summary>
    /// <remarks>
    /// Given back once, whoever gets there first. Two calls that both read a plain flag before either set it
    /// would each give the same reader back, and the count for this file and topic would reach nothing while a
    /// sibling subscription was still holding deliveries — which is the one condition that lets the next open
    /// run crash recovery, handing that sibling's in-flight work to somebody else. Closing twice is a thing
    /// callers are entitled to do, so it is made safe here rather than asked for.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        await _writer.DisposeAsync().ConfigureAwait(false);

        if (_topic is null || Interlocked.Exchange(ref _closed, 1) != 0)
            return;
        Release(Path, _topic);
    }
}

/// <summary>Where a subscriber that has never read a topic before begins in it.</summary>
internal readonly record struct StartAt(long Mark, ScheduledFloor Floor);

/// <summary>A subscriber binding itself to one topic, with the position to take if it has none yet.</summary>
internal readonly record struct Attachment(string Topic, string LogUuid, StartAt Start);

/// <summary>A subscriber's whole view of one topic at a moment in time.</summary>
internal sealed record StoreSnapshot(long Mark, Dictionary<long, EntryState> Entries, List<string> SetAsideOrigins);

/// <summary>What became of a row this subscriber tried to set aside.</summary>
/// <param name="Refused">Nothing was written: what it would have been filed as is something this subscriber
/// cannot keep, so the caller is told instead of being answered with a row that is already gone.</param>
/// <param name="Sizes">
/// What the dead set held once the write had finished with it, for a subscriber that stops at a bound rather
/// than trimming to one. Read in the same transaction that filed the row, so the answer the caller acts on is
/// the one that was true when it landed. Empty for the other overflow policy, which needs no answer.
/// </param>
internal readonly record struct SetAsideOutcome(long AsideId, SetAsideKind Kind, bool Refused,
    SetAsideSizes Sizes);

/// <summary>What one subscriber has set aside: a count and a payload total for each of the two sets.</summary>
/// <param name="DeadHeld">
/// Dead letters nobody is working on — the ones parked in the set rather than on their way out of it. A
/// subscriber that stops at a full dead set counts these, not every dead row: replay is the documented way to
/// clear the stop, and a replayed row that still counted would leave the subscriber stopped against the only
/// remedy it has.
/// </param>
/// <param name="DeadHeldBytes">Payload bytes those parked dead letters hold between them.</param>
internal readonly record struct SetAsideSizes(long Deferred, long Dead, long DeferredBytes, long DeadBytes,
    long DeadHeld, long DeadHeldBytes);
