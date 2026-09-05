using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// A candidate delivery read from a topic file, without its payload. Choosing what to deliver needs the
/// identity and the times and nothing else, and a batch of candidates is mostly rows this subscriber has
/// already dealt with — reading their payloads to throw them away is the one avoidable cost on this path.
/// </summary>
internal readonly record struct Candidate(
    long Id, string MessageId, DateTimeOffset EnqueuedAt, DateTimeOffset EligibleAt);

/// <summary>
/// A subscriber's place in the scheduled table: every row at or below it is settled and will not be offered
/// again. The scheduled table is ordered by due time and then id, so a position in it needs both.
/// </summary>
internal readonly record struct ScheduledFloor(long DueAtMs, long Id)
{
    internal static ScheduledFloor Start => new(0, 0);
}

/// <summary>
/// The read-only side of a topic file. Opened with the read-only flag, which stops this connection changing
/// database content — though not SQLite creating the -wal and -shm sidecars, so the process still needs write
/// permission on the directory.
/// </summary>
/// <remarks>
/// <para>
/// Every statement, and the close, run under one lock. A <see cref="SqliteConnection"/> is not thread-safe,
/// and this is the one object in a subscription that more than one caller reaches: the read loop is on it
/// continuously, <c>LagAsync</c> is documented to be called beside that loop, a second enumerator would be on
/// it too, and the close destroys it. Everything else a subscription touches goes through the single writer,
/// which serialises by construction; this had nothing. Unsynchronised, the losing party got a
/// <c>SQLITE_MISUSE</c>, an <c>ObjectDisposedException</c> on a statement handle, or a connection left broken
/// enough that closing it took the process down from inside <c>DisposeAsync</c>.
/// </para>
/// <para>
/// The lock is held across one statement and never across an await, so the only thing it makes anybody wait
/// for is a query already in progress. That is the point: the close waits for the loop's current read rather
/// than pulling the connection out from under it.
/// </para>
/// </remarks>
internal sealed class TopicReader : IDisposable
{
    /// <summary>Due, and past a subscriber's floor. One definition, used by everything that asks.</summary>
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
    /// True once the file exists and is open. A topic nobody has published to yet has no file at all, and that
    /// reads as an empty topic rather than an error — creating it here would make a subscriber a writer of the
    /// log, which is the one thing the design does not allow.
    /// </summary>
    internal bool IsOpen
    {
        get { lock (_gate) return _connection is not null; }
    }

    internal string? LogUuid { get; private set; }

    /// <summary>
    /// The connection to read from, or null when there is no file yet. Called under the lock by everything
    /// below.
    /// </summary>
    /// <remarks>
    /// A reader that has been closed says so rather than answering as though the topic were empty. The two
    /// are not the same thing and reading them as the same is how a loop carries on polling a subscription
    /// somebody closed: no rows, no error, and nothing to tell it apart from a topic nobody has published to.
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
                    // The file is there but the publisher has not committed the schema yet. Come back.
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
    /// Log records after a position. Kept to one short statement: a read transaction held open across dispatch
    /// pins WAL checkpointing, and the file grows for as long as it is held.
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
    /// Scheduled messages that are due and above a subscriber's floor. Nothing promotes these into the log;
    /// the reader picks them up.
    /// </summary>
    /// <remarks>
    /// The floor is what stops this starving. A consumed scheduled row stays in the table — the subscriber
    /// reads the file and cannot delete anything, and fan-out means it must not, since another subscriber may
    /// not have taken it yet — so without a floor a batch of the oldest due rows fills with rows already dealt
    /// with and a newer message never reaches the window. Ordering by due time and then id makes "already dealt
    /// with" a prefix, which is the thing a position can be kept for.
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
    /// The payload of one record, or null if it is no longer there. Read after a delivery has been chosen and
    /// before it is claimed, so a record retention removed in between is simply passed over rather than
    /// claimed and then found to be missing.
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
    /// Scheduled rows a subscriber at this floor still owes delivery of. The same predicate as
    /// <see cref="Due"/>, kept beside it so the two can never disagree about what "still to do" means.
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
    /// How many of these ids name a scheduled row that is due and above the floor: the count
    /// <see cref="DueCount"/> answers, narrowed to a set of ids. Asking it this way rather than subtracting a
    /// number counted somewhere else is what makes the two comparable — an id whose row has been removed is
    /// missing from both sides instead of from one.
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
    /// Whether one sequence still names a record in the log. Null when this reader has no file to answer
    /// from, which is not the same answer as "it has gone" and must not be mistaken for it.
    /// </summary>
    /// <remarks>
    /// Asked about the one record rather than inferred from the lowest sequence still present. Retention
    /// takes the oldest records first, so the two agree today — but a settle answering a caller about the
    /// message in its hands is the wrong place to depend on how another component chooses what to remove.
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
    /// Which of these ids still name a row in the scheduled table. Null when this reader has no file to
    /// answer from, which is not the same answer as "none of them" and must not be mistaken for it.
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
    /// Splits a set of ids into statements SQLite will accept. The set is as large as a subscriber's entry
    /// table, which has no fixed bound, and a parameter list does.
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

    /// <summary>Finds which table a message id is in, and the row it is. Null if this topic has no such message.</summary>
    /// <remarks>
    /// Message ids are the identity a caller is told to use; the row ids are not, because they come from two
    /// number spaces and only look orderable. So anything asking about one message asks by id and translates
    /// here.
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
    /// When the next scheduled message falls due, so a reader can wait until then instead of polling. The timer
    /// is in memory and nothing depends on it surviving: a restart reads this again and works it out afresh.
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
    /// The floor that puts a subscriber past everything already due, leaving what is not yet due to arrive
    /// normally. A message scheduled before a subscriber existed but due after it started is one the
    /// subscriber is entitled to, which is what Service Bus does with a scheduled message too.
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
    /// The statement <see cref="Bounds"/> runs. Each aggregate is in a scalar subquery of its own, which is
    /// not a matter of taste: SQLite answers a minimum or a maximum from the index in a single descent, but
    /// only for a query holding exactly one aggregate. Asking for both in the same SELECT loses that and
    /// scans the whole log — on a delivery path that runs this per message, which turns the cost of a
    /// delivery into the size of the file. As subqueries each qualifies again, and it stays one statement so
    /// the three values are still read from a single snapshot.
    /// </summary>
    internal const string BoundsSql =
        """
        SELECT (SELECT COALESCE(MIN(seq), 0) FROM log),
               (SELECT COALESCE(MAX(seq), 0) FROM log),
               COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'log'), 0);
        """;

    /// <summary>
    /// Lowest sequence still in the log and highest, for spotting what retention removed before it was read
    /// and for measuring how far behind a position is.
    /// </summary>
    /// <remarks>
    /// A log retention has emptied has no lowest surviving sequence and no highest, and answering (0, 0) makes
    /// every reading of those two numbers say the opposite of what happened: nothing above the position, so
    /// nothing lost, nothing behind, and a subscriber reported as settled over messages that were destroyed
    /// unread. The high-water mark answers instead. It is durable and exact — <c>seq</c> is AUTOINCREMENT
    /// precisely so a sequence is never reissued, so SQLite keeps the highest ever assigned whether or not any
    /// row survives — and it puts the bounds where an empty log genuinely leaves them: the next record will be
    /// one above it, and everything up to it is gone. A log nobody has ever appended to reads as (1, 0), which
    /// is the same empty window it always was.
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

    internal DateTimeOffset? OldestAfter(long mark)
    {
        lock (_gate)
        {
            if (Live() is not { } connection)
                return null;

            using SqliteCommand command = Sqlite.Command(connection, null,
                "SELECT MIN(enqueued_at) FROM log WHERE seq > $m;");
            command.Parameters.AddWithValue("$m", mark);
            return command.ExecuteScalar() is long ms ? Sqlite.FromUnixMs(ms) : null;
        }
    }

    /// <summary>
    /// Closes the connection, waiting for whatever statement is on it rather than freeing it underneath one.
    /// </summary>
    /// <remarks>
    /// Closing twice is safe and does nothing the second time, and the field is cleared rather than left
    /// pointing at a disposed object: a reader that has been closed answers every later call by saying so,
    /// which is what <see cref="Live"/> is for.
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
