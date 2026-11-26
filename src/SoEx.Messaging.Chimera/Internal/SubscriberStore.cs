using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// The subscriber file. It holds the subscriber's position in each topic, its in-flight deliveries and
/// its set-aside messages. Only this instance writes the file. Only one subscriber in one process can hold a
/// lock. When that process stops, its lock ends. Thus the store needs no lease and no visibility timeout.
/// </summary>
internal sealed class SubscriberStore : IAsyncDisposable
{
    /// <summary>
    /// The number of stores in this process that read each subscriber-and-topic pair. Recovery uses it to tell
    /// deliveries that nobody holds from deliveries that a subscription has in hand now.
    /// </summary>
    private static readonly Dictionary<string, int> Reading = new(StringComparer.Ordinal);

    /// <summary>The identity of each path spelling that this process opened. See <see cref="Key"/>.</summary>
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
    /// Opens the store. If the caller already knows which log it will read, the open also binds the store to
    /// that topic. A bind at open, before the first read, makes a starting position mean "where this
    /// subscriber came into existence". A subscriber created before a burst of publishing and retention sees
    /// that burst as loss. A subscriber created after the burst does not.
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
                // The key is computed before the lock is taken. The first time Key sees a path, it queries
                // the filesystem. Inside the lock, every other store in this process that opens any
                // subscriber would wait behind that query.
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

            // The clock starts from the stamps in this file, for the same reason as the topic file clock. A
            // deferral aged out from a stamp older than the event that it describes can be destroyed under a
            // dead bound.
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
    /// The name of the file and topic that a store reads, for the guard above. If the filesystem ignores case,
    /// the path folds to one identity. Two components with roots that differ only in spelling then find each
    /// other. Otherwise, each component would treat the deliveries of the other as stale.
    /// The identity is computed once per path and kept. It cannot change while the file is open. A store that
    /// releases its count must name the key that it registered, even if the file was deleted after that.
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
    /// The newest stamp in this file. The query reads only the two stamps that this store writes. A set-aside
    /// row also holds the time that the topic published it. That time comes from the clock of another file.
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
    /// When this file is first opened, each in-flight delivery belongs to a process that stopped. This method
    /// makes those deliveries available again. It is the full crash-redelivery mechanism. Because of it, this
    /// library has no visibility timeout.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Recovery applies to one topic. It is skipped if this process already reads that topic as this
    /// subscriber. One subscriber file holds the positions of one subscriber in all the topics that it reads.
    /// Thus a subscriber that reads two topics is two subscriptions over one file. This is the documented shape.
    /// The second subscription used to reset every delivery that the first held. Those deliveries went out
    /// again while their handlers still ran, with the attempt count raised. Both subscriptions then settled the
    /// same message.
    /// </para>
    /// <para>
    /// The file records "in flight in a process that stopped" and "in flight in this process" in the same way.
    /// The count of the stores that this process has open for reading now tells the two apart.
    /// </para>
    /// <para>
    /// The guard counts the current readers. It keeps no record of past recoveries. Thus a subscription that
    /// closes and opens again, for example a restart of one subscriber in a host that continues to run,
    /// recovers in the same way as a process restart. This is true whether or not its siblings stayed open.
    /// </para>
    /// <para>
    /// Recovery releases the row and keeps it, so the attempt count survives. If recovery deleted the row, a
    /// message that stops the process would be retried for ever, because its count would start again on each
    /// restart. The attempt count exists to stop that scenario. Service Bus and JetStream both keep a delivery
    /// count across a redelivery. A reset here would also be more lenient than the brokers that this library
    /// is meant to be swapped for.
    /// </para>
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
    /// Binds this subscriber to a topic. The bind checks that the stored position belongs to the log that the
    /// subscriber will read. A mismatch means that a restore put the two out of step. If the read continued,
    /// the log would reissue sequences that the position already treats as done. The subscriber would skip
    /// those messages silently.
    /// </summary>
    internal async ValueTask<long> AttachTopicAsync(string topic, string logUuid, StartAt start = default,
        CancellationToken ct = default)
        => await _writer.EnqueueAsync((connection, transaction) =>
            Attach(connection, transaction, Subscriber,
                new Attachment(topic, logUuid, start)), ct).ConfigureAwait(false);

    /// <summary>
    /// Records this subscriber's position in a topic, or checks the stored position. A starting position
    /// applies only when the position is created. After that, the stored position is the truth. The
    /// configuration of a later subscription cannot move it.
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

    /// <summary>Finishes a delivery. The mark then absorbs all the entries that it can.</summary>
    internal ValueTask<long> CompleteAsync(string topic, EntrySource source, long id, CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction) =>
        {
            MarkTerminal(connection, transaction, topic, source, id);
            return AdvanceMark(connection, transaction, topic);
        }, ct);

    /// <summary>Marks a delivery as finished, for every way that a delivery can finish.</summary>
    /// <remarks>
    /// The method updates the row that the claim wrote. It never writes a new row. Every settle follows a
    /// claim. Thus, if the entry is missing, a position has already passed it. When retention overtakes a
    /// subscriber during a delivery, the mark moves over the record and its entry is dropped. The settle that
    /// arrived after that used to put a row back below the mark. No read looks there, and nothing can delete
    /// that row.
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
    /// Moves the mark over contiguous finished log entries. Each entry that the mark passes is deleted.
    /// </summary>
    /// <remarks>
    /// The entries must be contiguous. A jump to the highest finished sequence would pass over every in-flight
    /// entry below it. That failure shows no symptom until a crash. A message that was in process during the
    /// crash then never comes back.
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
    /// Moves a message, with its payload, out of the flow into this subscriber's store. The mark then
    /// moves past it. The copy of the payload lets a set-aside row survive log retention. Replay thus delivers
    /// this row. It needs no original record in the log.
    /// </summary>
    /// <remarks>
    /// This method reads the bounds on the deferred set inside the transaction that inserts the row. A check
    /// in one write op and an insert in another op do not hold a bound. Two handlers that defer at the same
    /// moment both read the set before either of them writes. Both find room, and both insert. A set with a
    /// bound of one then holds as many rows as there are in-flight deliveries. When the counts are read where
    /// the row is written, the caller gets the answer that was true at the write.
    /// </remarks>
    /// <returns>
    /// The written row and the kind that it was filed as, or a refusal. A deferral that does not fit is filed
    /// dead. A refusal means that this subscriber cannot keep a row of the kind that it would be filed as.
    /// </returns>
    /// <remarks>
    /// The write returns a refusal as a value. The exception belongs to the caller that caused it. A throw
    /// inside the write would roll back the whole batch. Every other op in the batch would then fail with a
    /// refusal that belongs to another caller. A bound reached under load is an ordinary event. The decision
    /// is still made inside the transaction, against the set that the row would join.
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

            // A full deferred set keeps every message it holds. A message lost because a bound was reached is
            // silent loss. The design avoids silent loss. A deferral that does not fit becomes a dead letter,
            // with a reason that says so.
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
                    // The reason states what happened to the row. This row joins the store that it was
                    // measured against. An operator who sees the reason "the store is full" next to a row that
                    // made the store larger must read the source code to understand it.
                    filedAs = SetAsideKind.Dead;
                    why = $"deferring would exceed the deferred byte bound ({maxBytes} bytes); filed dead";
                }
            }

            // A row filed dead and too big to keep. This subscriber destroys the oldest rows to stay inside
            // its byte bound. This row alone is over that bound, so the trim takes it, whatever room the set
            // has. A write would store the message only to lose it. Thus nothing is written. The caller gets
            // a refusal. An unkeepable row is charged to nothing, so it costs the rest of the set nothing in
            // either case. The refusal concerns this message only.
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

                // The clock is read here, where the row is written. A stamp taken before the write queued
                // gives the row an age that it never had. A writer with a backlog longer than the deferred age
                // would then file a message that ages out when it lands.
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
    /// Applies the dead bound to the set inside the write that changed the set. Returns the sizes that the
    /// caller uses to decide if the subscriber stops.
    /// </summary>
    /// <remarks>
    /// This method runs in the transaction that filed the row, in the same write op. A settle made of two ops
    /// can commit the first op and then fail the second. Each op checks for a cancelled token before it
    /// starts. Thus the usual cause is a cancel of a settle whose insert is already queued. The caller is then
    /// told that the settle failed, but the effect of the first op is durable. The caller retries, and the
    /// same message is filed dead twice. One op commits fully or not at all, so no part is left to fail.
    /// </remarks>
    private static SetAsideSizes EnforceDead(SqliteConnection connection, SqliteTransaction transaction,
        string topic, SetAsideKind filedAs, SetAsidePolicy bounds, long filed)
    {
        if (filedAs != SetAsideKind.Dead || (bounds.MaxDeadCount is null && bounds.MaxDeadBytes is null))
            return default;

        // A limit is a maximum. A drop trims the set back to the limit. A stop occurs when the set reaches
        // the limit, because a stopped subscriber cannot take another dead letter.
        if (bounds.DeadOverflow == DeadLetterOverflow.DropOldest)
        {
            TrimDeadTo(connection, transaction, topic, bounds.MaxDeadCount, bounds.MaxDeadBytes, filed);
            return default;
        }

        return SetAsideSize(connection, transaction, topic);
    }

    /// <summary>
    /// Re-files a row that is already in this store as a different kind. Then applies the bounds to the set
    /// that results.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The method is one write op, for the reason that <see cref="EnforceDead"/> gives. The settles that call
    /// this method give a delivery back when they throw. If anything throws after the row moved, the failure
    /// is reported over a durable effect.
    /// </para>
    /// <para>
    /// The deferred bounds apply only when the row joins the deferred set from outside it. A row re-filed as
    /// its current kind adds zero rows and zero bytes. A measure of such a row against the set that already
    /// holds it caused a defect. A message replayed and deferred again at the bound was dead-lettered, although
    /// the set did not grow. A row that moves from dead to deferred joins the set, so it is measured. A
    /// deferral that does not fit stays dead, with a reason that says so. A fresh arrival behaves the same way.
    /// </para>
    /// <para>
    /// This method also makes the keepability refusal, on the same terms as all other paths. A row is refused
    /// when it joins the dead set and is larger than the whole byte bound. The decision needs the current kind
    /// of the row. Thus the decision belongs in this write, and not at the call site.
    /// </para>
    /// <para>
    /// The caller cannot tell two cases apart. A downgraded deferral arrives in the dead set, and gets the
    /// same refusal as a fresh arrival. A dead letter re-filed as dead joins no set. A refusal of that row
    /// gives a false reason, because the set already holds the row and its stay costs the set nothing. That
    /// refusal also leaves a replayed dead letter no way back into the store that it came from.
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

            // A row that joins the dead set and is too big for this subscriber to hold: nothing is written,
            // for the same reason as for a fresh arrival. A row already filed dead does not join the set. It
            // is re-filed as dead with no refusal, and it stays in the same set.
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

                // The row gets a new stamp. The age bounds measure how long a row has held its current kind.
                // The age of a message deferred a moment ago starts at that deferral, and not at its first
                // set-aside.
                command.Parameters.AddWithValue("$now", _clock.NowMs());
                command.Parameters.AddWithValue("$i", asideId);
                command.ExecuteNonQuery();
            }

            sizes = EnforceDead(connection, transaction, topic, filedAs, bounds, asideId);
            return 1;
        }, ct).ConfigureAwait(false);

        return new SetAsideOutcome(asideId, filedAs, refused, sizes);
    }

    /// <summary>Lists the set-aside rows. Reads go through the writer, with one connection per file.</summary>
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

    /// <summary>Makes a set-aside row deliverable again. The ordinary read loop then takes it.</summary>
    /// <remarks>
    /// Replay skips a row that a handler holds. A re-arm of that row would give it to a second handler, under
    /// the delivery tag that the first handler still holds. A tag can be settled once only. The second settle
    /// is then refused for ever. The in-flight slot behind it never comes back. Service Bus also refuses to
    /// let a deferred message be received twice at the same time. A consumer that moves to Service Bus gets
    /// the same refusal.
    /// </remarks>
    /// <returns>True if the message now waits for delivery. False if it is gone or a handler holds it.</returns>
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

    /// <summary>
    /// Destroys a set-aside message at the caller's request. This is the only operation that destroys a message
    /// because a caller asked for it.
    /// </summary>
    /// <remarks>
    /// Like replay, discard skips a row that a handler holds. A delete under a delivery makes every later
    /// answer false. The settle finds nothing to settle. A failure below the attempt limit reports a
    /// redelivery, but no message remains to deliver.
    /// </remarks>
    /// <returns>True if the message was destroyed. False if it was already gone or a handler holds it.</returns>
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
    /// Removes the row behind a completed delivery. This is the one caller that can destroy an in-flight row.
    /// The caller is the handler that holds the row, and the call says that the handler is finished with it.
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
    /// Takes a delivery if nothing holds it and the position is below it. An entry above the mark is in
    /// flight now, or it is finished and not yet absorbed. Neither type of entry is delivered again.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This statement checks the position. The reading of the position by the caller is stale by the time the
    /// claim runs. The choice of a candidate and its claim are separate write ops. Between them, a second
    /// reader of the same subscriber can complete the same record. That reader is a sibling instance or a
    /// second enumerator. The mark then absorbs the record and <em>deletes its entry</em>.
    /// </para>
    /// <para>
    /// An unguarded upsert then reads the missing entry as "never delivered" and inserts a fresh claim below
    /// the mark. The message goes to a second handler on a path where nothing failed. The row that the claim
    /// leaves sits where no position will absorb it. Every poll and every inspection counts that row for the
    /// life of the file. <c>ReleaseAsync</c> answers for it as if a redelivery will come, but no read will
    /// offer one.
    /// </para>
    /// <para>
    /// A select from the cursor row makes the claim and the check one statement. A log claim is valid only
    /// above the mark. A scheduled claim is valid only above the floor. That check needs the due time of the
    /// row, because the floor orders by due time and then by id. <c>TopicReader.Due</c> makes the same
    /// comparison.
    /// </para>
    /// </remarks>
    /// <returns>The attempt number, or 0 if this message is already claimed or already absorbed.</returns>
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

    /// <summary>Gives a failed delivery back. The attempt count stays the same.</summary>
    /// <remarks>
    /// The row stays. A delete would lose the attempt count. A message that fails every time would then be
    /// retried forever, because its count would start again on each delivery.
    /// </remarks>
    /// <returns>
    /// One if the delivery was given back. Zero if the entry is gone. A missing entry means that a position
    /// has passed the record. No read will offer the record again.
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
    /// Measures the loss below a surviving minimum, moves the position over the loss and drops the entries
    /// that it passes. One write does all three, so the measured gap is the closed gap.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this method only when retention removed records that this subscriber had not read. The mark
    /// advances over contiguous entries. A mark below a gap that can never fill would stay there for good. The
    /// exception set would then grow without limit. An acknowledgement of the loss is the only way back to a
    /// consistent position. Thus the method reports the loss at the same time as it moves the position.
    /// </para>
    /// <para>
    /// The measure and the move used to be separate ops. Two readers of the same subscriber interleaved them.
    /// Each reader was a sibling instance or a second enumerator. Both read the mark before it moved, both
    /// measured the same gap, and both reported it. That doubled the one number that, by the loss contract, an
    /// operator can act on.
    /// </para>
    /// <para>
    /// In one op, a call that finds the mark already moved measures an empty gap and reports zero. The report
    /// belongs to the call that moved the position. The no-op write at the start is part of this. It takes the
    /// write lock of the file before the mark is read. A move by a sibling thus lands before this measurement,
    /// and never between the measurement and the write after it.
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

            // Records that a handler holds, or has already settled, are not lost to this subscriber, however
            // they left the log. A count of them would report messages that a handler has as messages that
            // never arrived. An entry left Available is neither held nor settled. A delivery that failed and
            // was given back is owed another attempt, and only the log can make that attempt. The ids are read
            // before the delete below removes them. They are read as a set of ids, so the gap that this
            // reports is the gap that it counted.
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

            // The delete takes every entry in the gap except the in-flight deliveries. Those stay on purpose.
            // The mark will move past them. After that, the entry is the only record that this subscriber
            // received the message. This code used to delete it. The settle that followed then found nothing
            // to give back. The failure was answered honestly, but no count included the message. This pass
            // did not count it, because a handler held it at the time. No later pass counted it, because
            // nothing was left to see.
            //
            // The pass over entries below the mark sweeps the entries that stay. That pass reports the
            // entries that are still owed. It is the second ending for these entries. The mark is also the
            // lower bound of the delete. An entry that a previous gap left is thus for that pass to report.
            // This delete does not take it silently.
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

            // The bounds name the first and last records that this report counted. If the range included
            // messages that the subscriber holds or has already finished, an operator would look for messages
            // that are present.
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
    /// Clears the log entries that the position has already passed. Returns the ids of the entries that were
    /// still owed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The mark moves over an entry only when no work on it remains, with one exception. The loss settle above
    /// moves the mark over a gap that retention left. It leaves the deliveries that were in flight at that time
    /// to be settled later. Those entries come to rest below the mark. The claim guard makes sure that no claim
    /// can reach them there. Nothing else will absorb them.
    /// </para>
    /// <para>
    /// This method is their ending. The type of ending depends on how the delivery finished. A finished
    /// delivery is finished, however the record left the log. A delivery that was given back is a redelivery
    /// that this subscriber was promised and that the log can no longer make. That is the definition of the
    /// loss that this method reports. A crash reaches the same state. Recovery gives its in-flight deliveries
    /// back, and the ones below the mark are owed.
    /// </para>
    /// <para>
    /// The delete reads the mark, and the report names the rows that the delete removed. Thus the report
    /// belongs to the caller that removed them. A sibling or a second enumerator that runs this at the same
    /// moment takes no rows and reports zero. It does not report the same loss again. The state clause skips
    /// a delivery claimed after the caller looked, for the same reason as the loss settle above. A report that
    /// names a message as lost while a handler holds it makes the same mistake that the guard stops in the
    /// file.
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
    /// The state of every entry row for one topic and source. Entry rows exist only for deliveries that the
    /// position has not yet absorbed: the deliveries that this subscriber holds, and the finished deliveries
    /// waiting to be absorbed. One read gives all of them. The caller
    /// thus sorts a batch of candidates in memory, with no claim attempt for each one in turn.
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

    /// <summary>This subscriber's position in the scheduled table.</summary>
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
    /// Moves the scheduled floor over a run of settled rows. Drops the entries that recorded those rows.
    /// </summary>
    /// <remarks>
    /// The log mark has the same rule and the same risk. The run must be contiguous in the order in which the
    /// reader takes rows. Otherwise, the floor passes over an in-flight row, and that row never comes back.
    /// The caller passes each id that it absorbs, as a list of ids. Thus the delete removes only the rows that
    /// the caller checked. The floor never moves backwards. Because of this, a stale advance causes no harm.
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
    /// Deletes the entries for scheduled rows that the topic file no longer holds.
    /// </summary>
    /// <remarks>
    /// Usually, an entry is deleted when the floor absorbs the row that it records. The floor moves only over
    /// rows that it saw as candidates. If retention removed a row before that, the row never appears as a
    /// candidate again. Nothing can then absorb its entry. The entry stays for good, and each reader of this
    /// file counts it for good. This method is the other way that an entry ends. It is the only ending that is
    /// not a move of a position.
    /// </remarks>
    /// <returns>The ids whose entries this method removed. They can differ from the ids that it got.</returns>
    internal async ValueTask<List<long>> ForgetScheduledAsync(string topic, IReadOnlyList<long> ids,
        CancellationToken ct = default)
    {
        List<long> forgotten = [];
        await _writer.EnqueueAsync((connection, transaction) =>
        {
            forgotten.Clear();

            // The statement holds the in-flight guard, in addition to the check on the states that the caller
            // last saw. The caller decides what to forget from a set that it read a moment earlier. A sibling
            // on the same subscriber and topic can claim a delivery after that read. Without the guard, the
            // delete would remove the entry of that delivery. The report would then name the delivery as owed
            // and lost while a handler held it. The rule here is the same as everywhere in this file: nothing
            // acts on a row that a handler holds.
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

    /// <summary>Sets a set-aside delivery back to held, or to ready for another attempt.</summary>
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

    /// <summary>The size of the set-aside store: a count and a payload total for each kind.</summary>
    /// <remarks>
    /// The sizes are kept per set because each set has a separate bound. The bounds are separate because only
    /// one of the sets has a place to pass on a message that does not fit.
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
    /// Changes deferred messages that have waited too long into dead letters. The pass keeps each message. A
    /// deferred message lost without a report would be silent loss, and this design exists to avoid silent
    /// loss.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The pass skips a row that a handler holds. That row has not waited. A handler works on it now. The
    /// settle that follows would put a dead letter back to ready at once. The next pass measures its age
    /// again, from the state that the settle leaves.
    /// </para>
    /// <para>
    /// Every other row keeps its delivery state. The pass used to park each row that it changed. That
    /// cancelled a replay silently. A deferral that an operator had asked to see again became a held dead
    /// letter, and the request was lost with no report. Now only the kind changes. A row on its way out stays
    /// on its way out, and the next read delivers it as a dead letter. A replay of a dead letter has always
    /// meant this.
    /// </para>
    /// <para>
    /// The other way to stop the cancel was to leave such a row unchanged. That way is worse. A delivery
    /// interrupted by a restart comes back as ready. Every deferral that a redeploy caught in flight would
    /// then escape the age bound for as long as no reader read the subscriber. For a subscriber stopped at a
    /// full dead set, that time has no end.
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

            // The count is recorded in the file, in the write that aged the rows, for the same reason as the
            // drop count. A pass is three writes. Each op checks for a cancelled token before it starts. Thus
            // a host that gives its shutdown token to a maintenance pass is the usual cause of a failure after
            // this write. A pass that failed there used to report nothing. The next pass then had nothing to
            // count, because the deferrals were already dead. No calculation could recover the number.
            if (aged > 0)
                Bump(connection, transaction, AgedKey(topic), aged);

            return aged;
        }, ct);

    /// <summary>
    /// Destroys the oldest dead letters down to a limit, under <see cref="DeadLetterOverflow.DropOldest"/>. This is
    /// real loss. It is the only path in this file where a bound destroys messages.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The method gets the limit and calculates the excess, inside the one write that deletes. A caller can
    /// read the size and then ask for that number of rows to be destroyed. Other settles can trim the same set
    /// at the same time. Each of them measures the same backlog and is told to remove it. Together they empty
    /// a set that had to lose only its oldest few rows. This is the same rule as on the insert side, applied
    /// to the side that destroys.
    /// </para>
    /// <para>
    /// The trim keeps a dead letter that was replayed and that a handler holds now. The reason is the same as
    /// for discard. The handler will be told what happened to the row. After a trim, the row would be gone.
    /// The row still counts towards the limit. Thus the set can be one or two rows above its limit while those
    /// deliveries finish. The next pass trims what they leave.
    /// </para>
    /// <para>
    /// The trim can destroy a row that was replayed and not yet given to a handler. This is a decision. No
    /// answer about that row becomes false. The answer from replay was about the store at that time. An
    /// append gives the same kind of answer about a record that retention can later reach.
    /// </para>
    /// <para>
    /// The exemption cannot safely cover these rows. <c>MaxInFlight</c> limits the in-flight deliveries, but
    /// any number of rows can be made ready at once. If the trim spared ready rows, a caller that replayed a
    /// whole set would suspend the bound. A bound that a caller can switch off is worse than counted loss.
    /// Counted loss is the purpose of this option.
    /// </para>
    /// </remarks>
    internal ValueTask<long> TrimDeadToAsync(string topic, long? count, long? bytes,
        CancellationToken ct = default)
        => _writer.EnqueueAsync((connection, transaction)
            => TrimDeadTo(connection, transaction, topic, count, bytes, filed: null), ct);

    /// <inheritdoc cref="TrimDeadToAsync"/>
    /// <remarks>
    /// This method takes the connection. The settle that filed the row can then trim inside the write that
    /// filed it. The rows that a bound destroys and the row that put the set over the bound belong in one
    /// transaction.
    /// </remarks>
    private static long TrimDeadTo(SqliteConnection connection, SqliteTransaction transaction, string topic,
        long? count, long? bytes, long? filed)
    {
        long dropped;
        // The rows are numbered and totalled newest first. A bound thus keeps the newest rows of the set and
        // drops every row that the newest rows pushed out. An in-flight row is numbered and totalled like any
        // other row, because it still occupies the set. The delete never takes it.
        //
        // The row that this write filed is the newest arrival, whatever its aside_id is. That is the full
        // meaning of $filed. A re-filed row keeps the id from its first set-aside. With an order by id alone,
        // a replayed message settled dead was always the oldest row in the set. The trim that its own settle
        // started destroyed it. The settle then answered "filed" about a message that was gone. The sort puts
        // this row first. It is numbered first and totalled first, so the trim keeps it and pushes out the
        // oldest of the other rows. A fresh insert has always behaved the same way.
        //
        // The delete also excludes this row. Thus this statement itself makes this rule true: "a settle never
        // destroys the row that it files". Without the exclusion, the rule holds only while every route into
        // the set refuses a row that it cannot keep. The guarantee would then depend on call sites and a
        // validation rule. The defect that this exclusion was written for shows that failure: one of those
        // parts was a bound out of step with the bound that fires.
        //
        // A row with a payload larger than the whole byte bound is charged to nothing. The delete destroys it
        // wherever it sits. When such a row was totalled like other rows, it pushed the running sum past the
        // bound for every older row. A single arrival that the set could never keep then emptied a set that
        // was well inside the bound. The age-out pass files such a row, and the maintenance trim after it took
        // the rest of the set. Both bounds now count only the rows that this subscriber can keep. The trim
        // destroys only the row that it cannot hold, and counts that row as loss.
        //
        // The delete names unkeepable rows directly, and does not rely on the sum. A row too big for the bound
        // is over the bound alone, and stays over it however much the set shrinks. The in-flight and $filed
        // exclusions still apply first. The trim spares, whatever their size, a row that a handler holds and
        // the row that a write files. That handler will be told what happened to its row.
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

        // The count is recorded in the write that destroyed the rows. A close or a crash loses a count held
        // in memory. Nobody can reconstruct that loss afterwards, because the rows are gone. In the file, the
        // count belongs to the subscriber, whatever object settled at the time. The next pass reports it,
        // whoever runs that pass.
        if (dropped > 0)
            Bump(connection, transaction, DroppedKey(topic), dropped);

        return dropped;
    }

    /// <summary>
    /// The changes to this subscriber's set-aside store since the last call: deferrals aged into dead
    /// letters, and dead letters destroyed by a bound.
    /// </summary>
    /// <remarks>
    /// One write reads and resets both counters. Thus two passes cannot report the same numbers. A pass that
    /// reports the numbers also clears them, so they are not reported again. The two counters are taken
    /// together because both are now counted the same way: by the code that did the work, in the write that
    /// did it. A pass that took one count from the file and the other from a local variable lost the local
    /// count each time it failed part-way.
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

    /// <summary>All that this subscriber knows about a topic, read in one write op to check the invariant.</summary>
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
    /// Closes the file. Gives back the count that this store holds against the file.
    /// </summary>
    /// <remarks>
    /// The first call gives the count back, once. If two calls both read a plain flag before either set it,
    /// each would give the same reader back. The count for this file and topic would then reach zero while a
    /// sibling subscription still held deliveries. A count of zero is the one condition that lets the next
    /// open run crash recovery. That recovery gives the in-flight work of the sibling to another reader.
    /// Callers are permitted to close twice, so this method makes a second close safe.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        await _writer.DisposeAsync().ConfigureAwait(false);

        if (_topic is null || Interlocked.Exchange(ref _closed, 1) != 0)
            return;
        Release(Path, _topic);
    }
}

/// <summary>The start position in a topic for a subscriber with no position in that topic yet.</summary>
internal readonly record struct StartAt(long Mark, ScheduledFloor Floor);

/// <summary>The bind of a subscriber to one topic, with the start position to use if it has no position yet.</summary>
internal readonly record struct Attachment(string Topic, string LogUuid, StartAt Start);

/// <summary>The full view that a subscriber has of one topic at one moment.</summary>
internal sealed record StoreSnapshot(long Mark, Dictionary<long, EntryState> Entries, List<string> SetAsideOrigins);

/// <summary>The result when this subscriber tries to set a row aside.</summary>
/// <param name="AsideId">The identifier of the set-aside row that was written.</param>
/// <param name="Kind">The kind that the row was filed as.</param>
/// <param name="Refused">Nothing was written. This subscriber cannot keep a row of the kind that it would have
/// been filed as. The caller gets this refusal, and no answer about a row that is already gone.</param>
/// <param name="Sizes">
/// The contents of the dead set after the write, for a subscriber that stops at a bound. The trim policy
/// leaves this empty, because it needs no answer. The sizes are read in the transaction that filed the row.
/// Thus the caller acts on the answer that was true when the row landed.
/// </param>
internal readonly record struct SetAsideOutcome(long AsideId, SetAsideKind Kind, bool Refused,
    SetAsideSizes Sizes);

/// <summary>The set-aside store of one subscriber: a count and a payload total for each of the two sets.</summary>
/// <param name="Deferred">Deferred messages held.</param>
/// <param name="Dead">Dead letters held, including ones on their way out after a replay.</param>
/// <param name="DeferredBytes">The total payload bytes of the deferred messages.</param>
/// <param name="DeadBytes">The total payload bytes of the dead letters.</param>
/// <param name="DeadHeld">
/// Dead letters that are parked in the set, with no work on them. A replayed dead letter is on its way out,
/// and this count excludes it. A subscriber that stops at a full dead set counts only these rows. Replay is
/// the documented way to clear the stop. If a replayed row still counted, the subscriber would stay stopped
/// after its only remedy.
/// </param>
/// <param name="DeadHeldBytes">The total payload bytes of those parked dead letters.</param>
internal readonly record struct SetAsideSizes(long Deferred, long Dead, long DeferredBytes, long DeadBytes,
    long DeadHeld, long DeadHeldBytes);
