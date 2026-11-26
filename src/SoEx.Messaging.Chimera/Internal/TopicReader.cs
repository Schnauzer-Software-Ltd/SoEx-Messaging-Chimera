using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// A candidate delivery read from a topic file, without its payload. The choice of what to deliver needs only the
/// identity and the times. Most rows in a batch of candidates are rows that this subscriber has already processed.
/// A read of their payloads, which the reader then ignores, is the one avoidable cost on this path.
/// </summary>
internal readonly record struct Candidate(
    long Id, string MessageId, DateTimeOffset EnqueuedAt, DateTimeOffset EligibleAt);

/// <summary>
/// The position of a subscriber in the scheduled table. Every row at or below it is settled, and the reader does
/// not offer it again. The scheduled table is in order of due time and then id, so a position in it needs both
/// values.
/// </summary>
internal readonly record struct ScheduledFloor(long DueAtMs, long Id)
{
    internal static ScheduledFloor Start => new(0, 0);
}

/// <summary>
/// The read-only side of a topic file. The connection opens with the read-only flag. This flag stops the connection
/// from a change to database content. SQLite still creates the -wal and -shm sidecars, so the process needs write
/// permission on the directory.
/// </summary>
/// <remarks>
/// <para>
/// Every statement, and the close, run under one lock. A <see cref="SqliteConnection"/> is not thread-safe. This is
/// the one object in a subscription that more than one caller reaches: the read loop uses it continuously,
/// <c>LagAsync</c> is documented for calls beside that loop, a second enumerator also uses it, and the close
/// destroys it. All other objects that a subscription touches go through the single writer, which serialises by
/// construction. This object had no serialisation. Without the lock, the losing caller got a <c>SQLITE_MISUSE</c>,
/// an <c>ObjectDisposedException</c> on a statement handle, or a connection so broken that its close stopped the
/// process from inside <c>DisposeAsync</c>.
/// </para>
/// <para>
/// The lock is held for one statement and never across an await. Thus a caller waits only for a query that has
/// already started. This is the purpose of the lock: the close waits for the current read of the loop, and then
/// releases the connection.
/// </para>
/// </remarks>
internal sealed class TopicReader : IDisposable
{
    /// <summary>Due, and above the floor of a subscriber. One definition, for every query that uses it.</summary>
    private const string DueAboveFloor =
        "due_at <= $now AND (due_at > $fd OR (due_at = $fd AND id > $fi))";

    private readonly object _gate = new();
    private readonly string _path;
    private readonly ChimeraOptions _options;
    private SqliteConnection? _connection;
    private bool _closed;

    internal TopicReader(string path, ChimeraOptions options)
    {
        _path = path;
        _options = options;
    }

    /// <summary>
    /// True when the file exists and is open. A topic with no publish yet has no file. The reader reports an empty
    /// topic for it. If the reader created the file, the subscriber would become a writer of the log, and the
    /// design forbids that.
    /// </summary>
    internal bool IsOpen
    {
        get { lock (_gate) return _connection is not null; }
    }

    internal string? LogUuid { get; private set; }

    /// <summary>
    /// The connection to read from, or null when the file does not exist yet. Every method below calls it under the
    /// lock.
    /// </summary>
    /// <remarks>
    /// A closed reader throws an exception that says it is closed. An empty topic and a closed reader are different
    /// states. If the reader gave the same answer for both, a loop could continue to poll a subscription that a
    /// caller closed: no rows, no error, and nothing to tell it from a topic with no publishes.
    /// </remarks>
    private SqliteConnection? Live()
    {
        if (_closed)
            throw new ChimeraException("The store is closed.");
        return _connection;
    }

    internal bool TryOpen()
    {
        lock (_gate)
        {
            if (_closed)
                throw new ChimeraException("The store is closed.");
            if (_connection is not null)
                return true;
            if (!File.Exists(_path))
                return false;

            SqliteConnection connection = Sqlite.OpenReader(_path, _options);
            try
            {
                if (TopicSchema.TryReadIdentity(connection) is not { } identity)
                {
                    // The file exists, but the publisher has not committed the schema yet. Return false so the
                    // caller tries again.
                    connection.Dispose();
                    return false;
                }

                if (identity.SchemaVersion > TopicSchema.Version)
                    throw new ChimeraException(
                        $"The topic file was written by a newer version of this library " +
                        $"(schema {identity.SchemaVersion}).");

                LogUuid = identity.Uuid;
                _connection = connection;
                return true;
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }
    }

    /// <summary>
    /// Log records after a position. The query is one short statement. A read transaction held open across dispatch
    /// blocks WAL checkpointing, and the file grows for as long as the transaction stays open.
    /// </summary>
    internal List<Candidate> After(long mark, int limit)
    {
        lock (_gate)
        {
            var records = new List<Candidate>();
            if (Live() is not { } connection)
                return records;

            using SqliteCommand command = Sqlite.Command(connection, null,
                "SELECT seq, msg_id, enqueued_at FROM log WHERE seq > $m ORDER BY seq LIMIT $n;");
            command.Parameters.AddWithValue("$m", mark);
            command.Parameters.AddWithValue("$n", limit);
            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                DateTimeOffset enqueued = Sqlite.FromUnixMs(reader.GetInt64(2));
                records.Add(new Candidate(reader.GetInt64(0), reader.GetString(1), enqueued, enqueued));
            }
            return records;
        }
    }

    /// <summary>
    /// Scheduled messages that are due and above the floor of a subscriber. These messages stay in the scheduled
    /// table, and the reader picks them up there.
    /// </summary>
    /// <remarks>
    /// The floor prevents starvation here. A consumed scheduled row stays in the table. The subscriber only reads
    /// the file and cannot delete rows. Fan-out also forbids a delete, because a different subscriber can still
    /// need the row. Without a floor, a batch of the oldest due rows fills with rows already processed, and a newer
    /// message never reaches the window. The order by due time and then id makes the processed rows a prefix. A
    /// position can record a prefix.
    /// </remarks>
    internal List<Candidate> Due(ScheduledFloor floor, DateTimeOffset now, int limit)
    {
        lock (_gate)
        {
            var records = new List<Candidate>();
            if (Live() is not { } connection)
                return records;

            using SqliteCommand command = Sqlite.Command(connection, null,
                $"SELECT id, msg_id, enqueued_at, due_at FROM scheduled WHERE {DueAboveFloor} " +
                "ORDER BY due_at, id LIMIT $n;");
            Bind(command, floor, now);
            command.Parameters.AddWithValue("$n", limit);
            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                records.Add(new Candidate(reader.GetInt64(0), reader.GetString(1),
                    Sqlite.FromUnixMs(reader.GetInt64(2)), Sqlite.FromUnixMs(reader.GetInt64(3))));
            }
            return records;
        }
    }

    /// <summary>
    /// The payload of one record, or null if the record has gone. The reader reads it after it chooses a delivery
    /// and before it claims the delivery. Thus the reader passes over a record that retention removed in between,
    /// before any claim.
    /// </summary>
    internal byte[]? PayloadOf(EntrySource source, long id)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return null;

            string sql = source == EntrySource.Scheduled
                ? "SELECT payload FROM scheduled WHERE id = $i;"
                : "SELECT payload FROM log WHERE seq = $i;";

            using SqliteCommand command = Sqlite.Command(connection, null, sql);
            command.Parameters.AddWithValue("$i", id);
            return command.ExecuteScalar() as byte[];
        }
    }

    /// <summary>
    /// Scheduled rows that a subscriber at this floor must still deliver. This count uses the same predicate as
    /// <see cref="Due"/>, and sits beside it, so the two always agree about what is "still to do".
    /// </summary>
    internal long DueCount(ScheduledFloor floor, DateTimeOffset now)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return 0;

            using SqliteCommand command = Sqlite.Command(connection, null,
                $"SELECT COUNT(*) FROM scheduled WHERE {DueAboveFloor};");
            Bind(command, floor, now);
            return (long)command.ExecuteScalar()!;
        }
    }

    /// <summary>
    /// How many of these ids name a scheduled row that is due and above the floor. This is the count from
    /// <see cref="DueCount"/>, narrowed to a set of ids. The query asks for the narrowed count directly, and this makes
    /// the two counts comparable. A subtraction of a count from a different query gives counts that do not compare.
    /// With this query, an id whose row was removed is missing from both sides, and not from one side only.
    /// </summary>
    internal long DueAmong(ScheduledFloor floor, DateTimeOffset now, IReadOnlyList<long> ids)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return 0;

            long total = 0;
            foreach ((string list, int start, int count) in Chunks(ids))
            {
                using SqliteCommand command = Sqlite.Command(connection, null,
                    $"SELECT COUNT(*) FROM scheduled WHERE {DueAboveFloor} AND id IN ({list});");
                Bind(command, floor, now);
                BindIds(command, ids, start, count);
                total += (long)command.ExecuteScalar()!;
            }
            return total;
        }
    }

    /// <summary>
    /// Whether one sequence still names a record in the log. Null when this reader has no file to read from. Null
    /// is a different answer from "it has gone", and callers must keep the two apart.
    /// </summary>
    /// <remarks>
    /// The method asks about the one record. It does not infer the answer from the lowest sequence still present.
    /// Retention removes the oldest records first, so the two answers agree today. But a settle that answers a
    /// caller about the message in its hands must not depend on how a different component chooses what to remove.
    /// </remarks>
    internal bool? ExistingLog(long seq)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return null;

            using SqliteCommand command = Sqlite.Command(connection, null, "SELECT 1 FROM log WHERE seq = $s;");
            command.Parameters.AddWithValue("$s", seq);
            return command.ExecuteScalar() is not null;
        }
    }

    /// <summary>
    /// Which of these ids still name a row in the scheduled table. Null when this reader has no file to read from.
    /// Null is a different answer from "none of them", and callers must keep the two apart.
    /// </summary>
    internal HashSet<long>? ExistingScheduled(IReadOnlyList<long> ids)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return null;

            var present = new HashSet<long>();
            foreach ((string list, int start, int count) in Chunks(ids))
            {
                using SqliteCommand command = Sqlite.Command(connection, null,
                    $"SELECT id FROM scheduled WHERE id IN ({list});");
                BindIds(command, ids, start, count);
                using SqliteDataReader reader = command.ExecuteReader();
                while (reader.Read())
                    present.Add(reader.GetInt64(0));
            }
            return present;
        }
    }

    /// <summary>
    /// Splits a set of ids into statements that SQLite accepts. The set can be as large as the entry table of a
    /// subscriber, which has no fixed bound. A parameter list has a bound.
    /// </summary>
    private static IEnumerable<(string List, int Start, int Count)> Chunks(IReadOnlyList<long> ids)
    {
        const int size = 400;
        for (int start = 0; start < ids.Count; start += size)
        {
            int count = Math.Min(size, ids.Count - start);
            yield return (string.Join(", ", Enumerable.Range(0, count).Select(i => $"$x{i}")), start, count);
        }
    }

    private static void BindIds(SqliteCommand command, IReadOnlyList<long> ids, int start, int count)
    {
        for (int i = 0; i < count; i++)
            command.Parameters.AddWithValue($"$x{i}", ids[start + i]);
    }

    /// <summary>Finds the table that holds a message id, and the row id in it. Null if this topic has no such
    /// message.</summary>
    /// <remarks>
    /// Callers identify a message by its message id. The row ids come from two number spaces and only look
    /// orderable, so callers do not use them. Thus every query about one message uses the message id, and this
    /// method translates it.
    /// </remarks>
    internal (EntrySource Source, long Id)? Locate(string messageId)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return null;

            using (var log = Sqlite.Command(connection, null, "SELECT seq FROM log WHERE msg_id = $m;"))
            {
                log.Parameters.AddWithValue("$m", messageId);
                if (log.ExecuteScalar() is long seq)
                    return (EntrySource.Log, seq);
            }

            using var scheduled = Sqlite.Command(connection, null, "SELECT id FROM scheduled WHERE msg_id = $m;");
            scheduled.Parameters.AddWithValue("$m", messageId);
            return scheduled.ExecuteScalar() is long id ? (EntrySource.Scheduled, id) : null;
        }
    }

    /// <summary>The due time of one scheduled row, for placing it against a floor.</summary>
    internal DateTimeOffset? DueAt(long id)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return null;

            using SqliteCommand command = Sqlite.Command(connection, null,
                "SELECT due_at FROM scheduled WHERE id = $i;");
            command.Parameters.AddWithValue("$i", id);
            return command.ExecuteScalar() is long due ? Sqlite.FromUnixMs(due) : null;
        }
    }

    private static void Bind(SqliteCommand command, ScheduledFloor floor, DateTimeOffset now)
    {
        command.Parameters.AddWithValue("$now", Sqlite.ToUnixMs(now));
        command.Parameters.AddWithValue("$fd", floor.DueAtMs);
        command.Parameters.AddWithValue("$fi", floor.Id);
    }

    /// <summary>
    /// The due time of the next scheduled message, so a reader can wait until then without a poll. The timer is in
    /// memory only. After a restart, the reader reads this value again and calculates the time again.
    /// </summary>
    internal DateTimeOffset? NextDue(DateTimeOffset after)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return null;

            using SqliteCommand command = Sqlite.Command(connection, null,
                "SELECT MIN(due_at) FROM scheduled WHERE due_at > $now;");
            command.Parameters.AddWithValue("$now", Sqlite.ToUnixMs(after));
            return command.ExecuteScalar() is long due ? Sqlite.FromUnixMs(due) : null;
        }
    }

    /// <summary>
    /// The floor that puts a subscriber above every message already due. Messages not yet due arrive normally. A
    /// subscriber is entitled to a message scheduled before the subscriber existed but due after it started.
    /// Service Bus treats a scheduled message the same way.
    /// </summary>
    internal ScheduledFloor DueSoFar(DateTimeOffset now)
    {
        lock (_gate)
        {
            long nowMs = Sqlite.ToUnixMs(now);
            if (Live() is not { } connection)
                return new ScheduledFloor(nowMs, 0);

            using SqliteCommand command = Sqlite.Command(connection, null,
                "SELECT COALESCE(MAX(id), 0) FROM scheduled WHERE due_at <= $now;");
            command.Parameters.AddWithValue("$now", nowMs);
            return new ScheduledFloor(nowMs, (long)command.ExecuteScalar()!);
        }
    }

    /// <summary>
    /// The statement that <see cref="Bounds"/> runs. Each aggregate has a separate scalar subquery, and this is
    /// required for performance. SQLite answers a minimum or a maximum from the index in a single descent, but only
    /// for a query with one aggregate. If one SELECT asks for both, SQLite scans the whole log. The
    /// delivery path runs this statement once for each message, so a full scan makes the cost of a delivery grow
    /// with the size of the file. In a subquery, each aggregate gets the index path again. The statement stays one
    /// statement, so the three values come from a single snapshot.
    /// </summary>
    internal const string BoundsSql =
        """
        SELECT (SELECT COALESCE(MIN(seq), 0) FROM log),
               (SELECT COALESCE(MAX(seq), 0) FROM log),
               COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'log'), 0);
        """;

    /// <summary>
    /// The lowest and the highest sequence still in the log. Used to find records that retention removed before a
    /// read, and to measure how far behind a position is.
    /// </summary>
    /// <remarks>
    /// If retention empties the log, the log has no lowest or highest surviving sequence. An answer of (0, 0) makes
    /// every use of those two numbers report the opposite of the facts: nothing above the position, so nothing lost
    /// and nothing behind. A subscriber then shows as settled over messages that were destroyed unread. The
    /// high-water mark gives the answer. It is durable and exact. <c>seq</c> is AUTOINCREMENT so that a sequence is
    /// never issued again. SQLite thus keeps the highest sequence ever assigned, with or without surviving rows.
    /// The high-water mark puts the bounds where an empty log leaves them: the next record is one above it, and all
    /// records up to it are gone. A log with no appends reads as (1, 0), the same empty window as before.
    /// </remarks>
    internal (long Min, long Max) Bounds()
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return (0, 0);

            using SqliteCommand command = Sqlite.Command(connection, null, BoundsSql);
            using SqliteDataReader reader = command.ExecuteReader();
            if (!reader.Read())
                return (0, 0);

            long min = reader.GetInt64(0);
            long max = reader.GetInt64(1);
            long highWater = reader.GetInt64(2);
            return max == 0 ? (highWater + 1, highWater) : (min, max);
        }
    }

    /// <summary>
    /// The publish time of the oldest record above a position, or null when nothing is above it.
    /// </summary>
    /// <remarks>
    /// The query reads the first record above the mark, and not the minimum over all records above it. The two give
    /// the same answer for one reason: the writer thread takes the stamp of a record in the order that it issues
    /// sequences, so stamps never fall as sequences rise. A minimum is a single aggregate over a column with no
    /// index. SQLite must then examine every record above the mark: the index takes it to the start point, and then
    /// it reads to the end of the log. The first-row query stops at the first row. On one hundred thousand records,
    /// the minimum takes twenty-one milliseconds and the first row takes eight microseconds. The query plan is
    /// identical for both, so only a measurement or this comment shows the difference.
    /// <para>
    /// This order is an existing assumption. Age-based retention already selects a cutoff by time and then deletes
    /// by sequence. That removes every record below the cutoff, whatever its stamp. If the stamps of a log fell out
    /// of order, retention would lose unexpired records long before this query gave a wrong answer. This comment
    /// states the dependency in one more place so that it stays visible.
    /// </para>
    /// </remarks>
    internal DateTimeOffset? OldestAfter(long mark)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return null;

            using SqliteCommand command = Sqlite.Command(connection, null,
                "SELECT enqueued_at FROM log WHERE seq > $m ORDER BY seq LIMIT 1;");
            command.Parameters.AddWithValue("$m", mark);

            // If nothing is above the mark, the query returns no row. A null row reaches this test the same way.
            // Neither is a long, and both mean that there is nothing to be behind on.
            return command.ExecuteScalar() is long ms ? Sqlite.FromUnixMs(ms) : null;
        }
    }

    /// <summary>
    /// Closes the connection. The close waits for any current statement on it.
    /// </summary>
    /// <remarks>
    /// A second close is safe and does nothing. The close clears the field, so the field does not point to a
    /// disposed object. A closed reader answers every later call with an exception that says it is closed.
    /// <see cref="Live"/> gives that answer.
    /// </remarks>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_closed)
                return;
            _closed = true;
            _connection?.Dispose();
            _connection = null;
        }
    }
}
