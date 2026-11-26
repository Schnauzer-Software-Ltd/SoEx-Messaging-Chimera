using Microsoft.Data.Sqlite;
using SoEx.Messaging.Chimera.Internal;

namespace SoEx.Messaging.Chimera.Testing;

/// <summary>
/// Reads the state of each subscriber under a root directory from the subscriber files. Each file opens
/// read-only.
/// </summary>
/// <remarks>
/// <para>
/// Use it in a test over several connected components. The components set how many messages flow. The test
/// asserts that the system came to rest, and then that the correct results occurred. Both are questions about
/// the state on disk. Each file opens read-only while its writer continues to write.
/// </para>
/// <para>
/// The inspector only reads. It cannot advance a position, settle a delivery or create a file. Use it on a
/// stuck system and on a system under test. Reading needs write permission on the directory, because SQLite
/// creates its sidecar files for a read-only connection too.
/// </para>
/// </remarks>
public static class ChimeraInspector
{
    /// <summary>Reads the state of each subscriber under the root directory at the time of the call.</summary>
    /// <remarks>
    /// An unreadable subscriber file causes an exception. Retention skips an unreadable file, because retention
    /// only reports on subscribers. In a test, a subscriber missing from the answer lets the test pass with no
    /// check of that subscriber.
    /// </remarks>
    public static ChimeraOverview Overview(ChimeraOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var activities = new List<SubscriberActivity>();
        if (!Directory.Exists(options.RootDirectory))
            return new ChimeraOverview(activities);

        DateTimeOffset now = options.Clock.GetUtcNow();
        var topics = new Dictionary<string, TopicReader>(StringComparer.Ordinal);
        try
        {
            IEnumerable<string> files = Directory
                .EnumerateFiles(options.RootDirectory, "*.subscriber.db")
                .OrderBy(path => path, StringComparer.Ordinal);

            foreach (string path in files)
                activities.AddRange(Read(path, options, topics, now));
        }
        finally
        {
            foreach (TopicReader reader in topics.Values)
                reader.Dispose();
        }

        return new ChimeraOverview(activities);
    }

    /// <summary>Gets the state of one message for one subscriber.</summary>
    /// <remarks>
    /// <para>
    /// Identify the message by its message id. Do not use the sequence on an envelope. Sequences come from two
    /// number spaces in the topic file, and values from different spaces have no order. This method converts
    /// the message id to a sequence.
    /// </para>
    /// <para>
    /// This method reads the files as they are now. A destroyed message and an absent message give the same
    /// result. For that reason, <see cref="MessageState.Completed"/> and <see cref="MessageState.Unknown"/>
    /// both include a loss. The remarks on <see cref="MessageState"/> tell which loss each includes. Use this
    /// method to assert an expected state. It cannot show that no message was lost.
    /// </para>
    /// </remarks>
    public static MessageStatus StatusOf(ChimeraOptions options, string topic, string subscriber,
        string messageId)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriber);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

        using var reader = new TopicReader(FileNames.TopicFile(options.RootDirectory, topic), options);
        reader.TryOpen();

        if (reader.Locate(messageId) is not { } row)
            return Status(messageId, topic, subscriber, MessageState.Unknown, 0, null, null);

        string path = FileNames.SubscriberFile(options.RootDirectory, subscriber);
        if (!File.Exists(path))
            return Status(messageId, topic, subscriber, Undelivered(reader, row, ScheduledFloor.Start, 0,
                options.Clock.GetUtcNow()), 0, null, null);

        using SqliteConnection connection = Sqlite.OpenReader(path, options);
        SubscriberSchema.EnsureNotNewer(connection, null, subscriber);

        // Read the set-aside rows first. The exception set also marks a deferred or dead-lettered message as
        // terminal. A read of the exception set first gives "completed" for a message with a held copy.
        if (SetAside(connection, topic, messageId) is { } aside)
        {
            // A replayed row that a handler holds now is in flight, whatever its kind. A read of the kind only
            // gives "deferred" for that row. The overview shows a subscriber as unsettled while a replay runs.
            // The two results must agree. If they disagree, a test waits for quiet on one number while the
            // other number shows parked work.
            MessageState state = aside.Delivery == SetAsideDelivery.InFlight
                ? MessageState.InFlight
                : aside.Kind == SetAsideKind.Dead
                    ? MessageState.DeadLettered
                    : MessageState.Deferred;
            return Status(messageId, topic, subscriber, state, aside.Attempts, aside.Reason, aside.AsideAt);
        }

        if (Entry(connection, topic, row.Source, row.Id) is { } entry)
        {
            MessageState state = entry.State switch
            {
                EntryState.InFlight => MessageState.InFlight,
                EntryState.Available => MessageState.AwaitingRetry,
                _ => MessageState.Completed,
            };
            return Status(messageId, topic, subscriber, state, entry.Attempts, null, entry.Updated);
        }

        // The exception set has no record of the message. The position passed it, or the message is not
        // delivered yet. A passed position usually means that this subscriber completed the message. It can
        // also mean that the dead-letter bound destroyed a filed row. The bound deletes the set-aside row.
        // The entry for that row was terminal and absorbed long before. The result is a position past the
        // record, with no other trace. A completion leaves the same state. This method cannot tell the two
        // apart, so it answers Completed for both. The MessageState documentation discloses this. The
        // subscriber file counts the drop, and ApplyRetentionAsync reports it.
        (long mark, ScheduledFloor floor) = Position(connection, topic);
        return Status(messageId, topic, subscriber,
            Undelivered(reader, row, floor, mark, options.Clock.GetUtcNow()), 0, null, null);
    }

    private static MessageState Undelivered(TopicReader reader, (EntrySource Source, long Id) row,
        ScheduledFloor floor, long mark, DateTimeOffset now)
    {
        if (row.Source == EntrySource.Log)
            return row.Id <= mark ? MessageState.Completed : MessageState.Pending;

        if (reader.DueAt(row.Id) is not { } due)
            return MessageState.Unknown;
        if (due > now)
            return MessageState.NotYetDue;

        long dueMs = due.ToUnixTimeMilliseconds();
        bool passed = dueMs < floor.DueAtMs || (dueMs == floor.DueAtMs && row.Id <= floor.Id);
        return passed ? MessageState.Completed : MessageState.Pending;
    }

    private static MessageStatus Status(string messageId, string topic, string subscriber, MessageState state,
        long attempts, string? reason, DateTimeOffset? updatedAt)
        => new(messageId, topic, subscriber, state, attempts, reason, updatedAt);

    private static List<SubscriberActivity> Read(string path, ChimeraOptions options,
        Dictionary<string, TopicReader> topics, DateTimeOffset now)
    {
        var activities = new List<SubscriberActivity>();

        using SqliteConnection connection = Sqlite.OpenReader(path, options);
        string name = Meta(connection, "subscriber") ?? Path.GetFileNameWithoutExtension(path);
        SubscriberSchema.EnsureNotNewer(connection, null, name);

        foreach ((string topic, string logUuid, long mark, ScheduledFloor floor) in Cursors(connection))
        {
            TopicReader reader = ReaderFor(topics, options, topic);

            // A position from a different incarnation of the log gives no information about the current log.
            // The subscriber refuses to read it. A report of zero behind would look settled.
            bool stranded = reader.IsOpen && !string.Equals(reader.LogUuid, logUuid, StringComparison.Ordinal);

            (_, long max) = reader.Bounds();
            Dictionary<(EntrySource, EntryState), long> entries = EntryStates(connection, topic);
            (long deferred, long dead, long replaying) = SetAsideCounts(connection, topic);

            // A scheduled row that is settled and not yet absorbed stays inside the due window. The reader
            // moves the floor when it polls. If this count included the row, it would show work owed for a
            // handled message. A test that waits for quiet would then wait for the next poll. The log needs no
            // correction, because settling advances its mark.
            //
            // The topic file selects which settled entries to discount. The two numbers then come from one
            // predicate over one table. An entry whose row is removed drops out of both numbers. An earlier
            // version subtracted such an entry from a window that no longer held it. That arithmetic reported
            // an owed message as settled.
            List<long> settledScheduled = SettledScheduled(connection, topic);

            activities.Add(new SubscriberActivity(
                Subscriber: name,
                Topic: topic,
                Behind: stranded ? 0 : Math.Max(0, max - mark),
                DueScheduled: stranded
                    ? 0
                    : reader.DueCount(floor, now) - reader.DueAmong(floor, now, settledScheduled),
                InFlight: Count(entries, EntryState.InFlight),
                AwaitingRetry: Count(entries, EntryState.Available),
                AwaitingReplay: replaying,
                Deferred: deferred,
                DeadLettered: dead,
                Stranded: stranded));
        }

        return activities;
    }

    private static TopicReader ReaderFor(Dictionary<string, TopicReader> topics, ChimeraOptions options,
        string topic)
    {
        if (topics.TryGetValue(topic, out TopicReader? reader))
            return reader;

        reader = new TopicReader(FileNames.TopicFile(options.RootDirectory, topic), options);
        reader.TryOpen();   // a topic with no file reads as empty, as a subscriber sees it
        topics[topic] = reader;
        return reader;
    }

    private static List<(string Topic, string LogUuid, long Mark, ScheduledFloor Floor)> Cursors(
        SqliteConnection connection)
    {
        var cursors = new List<(string, string, long, ScheduledFloor)>();
        using SqliteCommand command = Sqlite.Command(connection, null,
            "SELECT topic, log_uuid, mark, due_floor, id_floor FROM cursor ORDER BY topic;");
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            cursors.Add((reader.GetString(0), reader.GetString(1), reader.GetInt64(2),
                new ScheduledFloor(reader.GetInt64(3), reader.GetInt64(4))));
        }
        return cursors;
    }

    private static Dictionary<(EntrySource, EntryState), long> EntryStates(SqliteConnection connection,
        string topic)
    {
        var states = new Dictionary<(EntrySource, EntryState), long>();
        using SqliteCommand command = Sqlite.Command(connection, null,
            "SELECT source, state, COUNT(*) FROM entry WHERE topic = $t GROUP BY source, state;");
        command.Parameters.AddWithValue("$t", topic);
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
            states[((EntrySource)reader.GetInt32(0), (EntryState)reader.GetInt32(1))] = reader.GetInt64(2);
        return states;
    }

    /// <summary>Gets the scheduled rows that this subscriber completed and that the floor has not reached.</summary>
    private static List<long> SettledScheduled(SqliteConnection connection, string topic)
    {
        var ids = new List<long>();
        using SqliteCommand command = Sqlite.Command(connection, null,
            "SELECT id FROM entry WHERE topic = $t AND source = $s AND state = $c;");
        command.Parameters.AddWithValue("$t", topic);
        command.Parameters.AddWithValue("$s", (int)EntrySource.Scheduled);
        command.Parameters.AddWithValue("$c", (int)EntryState.Complete);
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
            ids.Add(reader.GetInt64(0));
        return ids;
    }

    private static long Count(Dictionary<(EntrySource, EntryState), long> states, EntryState state)
        => states.Where(pair => pair.Key.Item2 == state).Sum(pair => pair.Value);

    private static (long Deferred, long Dead, long Replaying) SetAsideCounts(SqliteConnection connection,
        string topic)
    {
        long deferred = 0, dead = 0, replaying = 0;
        using SqliteCommand command = Sqlite.Command(connection, null,
            "SELECT kind, delivery, COUNT(*) FROM set_aside WHERE topic = $t GROUP BY kind, delivery;");
        command.Parameters.AddWithValue("$t", topic);
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            var kind = (SetAsideKind)reader.GetInt32(0);
            var delivery = (SetAsideDelivery)reader.GetInt32(1);
            long count = reader.GetInt64(2);

            if (kind == SetAsideKind.Dead)
                dead += count;
            else
                deferred += count;

            // Held means parked. Each other delivery state means that the message goes out, so it is work to do.
            if (delivery != SetAsideDelivery.Held)
                replaying += count;
        }
        return (deferred, dead, replaying);
    }

    private static (long Mark, ScheduledFloor Floor) Position(SqliteConnection connection, string topic)
    {
        using SqliteCommand command = Sqlite.Command(connection, null,
            "SELECT mark, due_floor, id_floor FROM cursor WHERE topic = $t;");
        command.Parameters.AddWithValue("$t", topic);
        using SqliteDataReader reader = command.ExecuteReader();
        return reader.Read()
            ? (reader.GetInt64(0), new ScheduledFloor(reader.GetInt64(1), reader.GetInt64(2)))
            : (0, ScheduledFloor.Start);
    }

    private static (SetAsideKind Kind, SetAsideDelivery Delivery, long Attempts, string? Reason,
        DateTimeOffset AsideAt)? SetAside(SqliteConnection connection, string topic, string messageId)
    {
        // Match on the message id in the row. The origin column records provenance for a person who reads the
        // file. If this code parsed the column, the column would become load-bearing.
        using SqliteCommand command = Sqlite.Command(connection, null,
            "SELECT kind, delivery, attempts, reason, aside_at FROM set_aside " +
            "WHERE topic = $t AND msg_id = $m ORDER BY aside_id DESC LIMIT 1;");
        command.Parameters.AddWithValue("$t", topic);
        command.Parameters.AddWithValue("$m", messageId);
        using SqliteDataReader reader = command.ExecuteReader();
        if (!reader.Read())
            return null;

        return ((SetAsideKind)reader.GetInt32(0), (SetAsideDelivery)reader.GetInt32(1), reader.GetInt64(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(4)));
    }

    private static (EntryState State, long Attempts, DateTimeOffset Updated)? Entry(SqliteConnection connection,
        string topic, EntrySource source, long id)
    {
        using SqliteCommand command = Sqlite.Command(connection, null,
            "SELECT state, attempts, updated FROM entry WHERE topic = $t AND source = $s AND id = $i;");
        command.Parameters.AddWithValue("$t", topic);
        command.Parameters.AddWithValue("$s", (int)source);
        command.Parameters.AddWithValue("$i", id);
        using SqliteDataReader reader = command.ExecuteReader();
        if (!reader.Read())
            return null;

        return ((EntryState)reader.GetInt32(0), reader.GetInt64(1),
            DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(2)));
    }

    private static string? Meta(SqliteConnection connection, string key)
    {
        using SqliteCommand command = Sqlite.Command(connection, null,
            "SELECT value FROM meta WHERE key = $k;");
        command.Parameters.AddWithValue("$k", key);
        return command.ExecuteScalar() as string;
    }
}
