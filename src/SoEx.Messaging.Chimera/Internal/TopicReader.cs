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
internal sealed class TopicReader : IDisposable
{
    /// <summary>Due, and past a subscriber's floor. One definition, used by everything that asks.</summary>
    private const string DueAboveFloor =
        "due_at <= $now AND (due_at > $fd OR (due_at = $fd AND id > $fi))";

    private readonly string _path;
    private readonly ChimeraOptions _options;
    private SqliteConnection? _connection;

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
    internal bool IsOpen => _connection is not null;

    internal string? LogUuid { get; private set; }

    internal bool TryOpen()
    {
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

    /// <summary>
    /// Log records after a position. Kept to one short statement: a read transaction held open across dispatch
    /// pins WAL checkpointing, and the file grows for as long as it is held.
    /// </summary>
    internal List<Candidate> After(long mark, int limit)
    {
        var records = new List<Candidate>();
        if (_connection is null)
            return records;

        using SqliteCommand command = Sqlite.Command(_connection, null,
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
        var records = new List<Candidate>();
        if (_connection is null)
            return records;

        using SqliteCommand command = Sqlite.Command(_connection, null,
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

    /// <summary>
    /// The payload of one record, or null if it is no longer there. Read after a delivery has been chosen and
    /// before it is claimed, so a record retention removed in between is simply passed over rather than
    /// claimed and then found to be missing.
    /// </summary>
    internal byte[]? PayloadOf(EntrySource source, long id)
    {
        if (_connection is null)
            return null;

        string sql = source == EntrySource.Scheduled
            ? "SELECT payload FROM scheduled WHERE id = $i;"
            : "SELECT payload FROM log WHERE seq = $i;";

        using SqliteCommand command = Sqlite.Command(_connection, null, sql);
        command.Parameters.AddWithValue("$i", id);
        return command.ExecuteScalar() as byte[];
    }

    /// <summary>
    /// Scheduled rows a subscriber at this floor still owes delivery of. The same predicate as
    /// <see cref="Due"/>, kept beside it so the two can never disagree about what "still to do" means.
    /// </summary>
    internal long DueCount(ScheduledFloor floor, DateTimeOffset now)
    {
        if (_connection is null)
            return 0;

        using SqliteCommand command = Sqlite.Command(_connection, null,
            $"SELECT COUNT(*) FROM scheduled WHERE {DueAboveFloor};");
        Bind(command, floor, now);
        return (long)command.ExecuteScalar()!;
    }

    /// <summary>
    /// How many of these ids name a scheduled row that is due and above the floor: the count
    /// <see cref="DueCount"/> answers, narrowed to a set of ids. Asking it this way rather than subtracting a
    /// number counted somewhere else is what makes the two comparable — an id whose row has been removed is
    /// missing from both sides instead of from one.
    /// </summary>
    internal long DueAmong(ScheduledFloor floor, DateTimeOffset now, IReadOnlyList<long> ids)
    {
        if (_connection is null)
            return 0;

        long total = 0;
        foreach ((string list, int start, int count) in Chunks(ids))
        {
            using SqliteCommand command = Sqlite.Command(_connection, null,
                $"SELECT COUNT(*) FROM scheduled WHERE {DueAboveFloor} AND id IN ({list});");
            Bind(command, floor, now);
            BindIds(command, ids, start, count);
            total += (long)command.ExecuteScalar()!;
        }
        return total;
    }

    /// <summary>
    /// Which of these ids still name a row in the scheduled table. Null when this reader has no file to
    /// answer from, which is not the same answer as "none of them" and must not be mistaken for it.
    /// </summary>
    internal HashSet<long>? ExistingScheduled(IReadOnlyList<long> ids)
    {
        if (_connection is null)
            return null;

        var present = new HashSet<long>();
        foreach ((string list, int start, int count) in Chunks(ids))
        {
            using SqliteCommand command = Sqlite.Command(_connection, null,
                $"SELECT id FROM scheduled WHERE id IN ({list});");
            BindIds(command, ids, start, count);
            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
                present.Add(reader.GetInt64(0));
        }
        return present;
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
        if (_connection is null)
            return null;

        using (var log = Sqlite.Command(_connection, null, "SELECT seq FROM log WHERE msg_id = $m;"))
        {
            log.Parameters.AddWithValue("$m", messageId);
            if (log.ExecuteScalar() is long seq)
                return (EntrySource.Log, seq);
        }

        using var scheduled = Sqlite.Command(_connection, null, "SELECT id FROM scheduled WHERE msg_id = $m;");
        scheduled.Parameters.AddWithValue("$m", messageId);
        return scheduled.ExecuteScalar() is long id ? (EntrySource.Scheduled, id) : null;
    }

    /// <summary>The due time of one scheduled row, for placing it against a floor.</summary>
    internal DateTimeOffset? DueAt(long id)
    {
        if (_connection is null)
            return null;

        using SqliteCommand command = Sqlite.Command(_connection, null,
            "SELECT due_at FROM scheduled WHERE id = $i;");
        command.Parameters.AddWithValue("$i", id);
        return command.ExecuteScalar() is long due ? Sqlite.FromUnixMs(due) : null;
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
        if (_connection is null)
            return null;

        using SqliteCommand command = Sqlite.Command(_connection, null,
            "SELECT MIN(due_at) FROM scheduled WHERE due_at > $now;");
        command.Parameters.AddWithValue("$now", Sqlite.ToUnixMs(after));
        return command.ExecuteScalar() is long due ? Sqlite.FromUnixMs(due) : null;
    }

    /// <summary>
    /// The floor that puts a subscriber past everything already due, leaving what is not yet due to arrive
    /// normally. A message scheduled before a subscriber existed but due after it started is one the
    /// subscriber is entitled to, which is what Service Bus does with a scheduled message too.
    /// </summary>
    internal ScheduledFloor DueSoFar(DateTimeOffset now)
    {
        long nowMs = Sqlite.ToUnixMs(now);
        if (_connection is null)
            return new ScheduledFloor(nowMs, 0);

        using SqliteCommand command = Sqlite.Command(_connection, null,
            "SELECT COALESCE(MAX(id), 0) FROM scheduled WHERE due_at <= $now;");
        command.Parameters.AddWithValue("$now", nowMs);
        return new ScheduledFloor(nowMs, (long)command.ExecuteScalar()!);
    }

    /// <summary>Lowest sequence still in the log, for spotting what retention removed before it was read.</summary>
    internal (long Min, long Max) Bounds()
    {
        if (_connection is null)
            return (0, 0);

        using SqliteCommand command = Sqlite.Command(_connection, null,
            "SELECT COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0) FROM log;");
        using SqliteDataReader reader = command.ExecuteReader();
        return reader.Read() ? (reader.GetInt64(0), reader.GetInt64(1)) : (0, 0);
    }

    internal DateTimeOffset? OldestAfter(long mark)
    {
        if (_connection is null)
            return null;

        using SqliteCommand command = Sqlite.Command(_connection, null,
            "SELECT MIN(enqueued_at) FROM log WHERE seq > $m;");
        command.Parameters.AddWithValue("$m", mark);
        return command.ExecuteScalar() is long ms ? Sqlite.FromUnixMs(ms) : null;
    }

    public void Dispose() => _connection?.Dispose();
}
