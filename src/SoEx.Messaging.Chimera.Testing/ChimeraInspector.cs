using Microsoft.Data.Sqlite;
using SoEx.Messaging.Chimera.Internal;

namespace SoEx.Messaging.Chimera.Testing;

/// <summary>
/// Reads what every subscriber under a root is doing, from their own files, without disturbing any of them.
/// </summary>
/// <remarks>
/// <para>
/// This exists because a test over several components wired together cannot assert on a message count: how
/// many messages flow is the components' business, not the test's. What it can assert is that the system came
/// to rest, and then that the right things happened. Both are questions about state on disk, and every file
/// here can be opened read-only while its writer carries on.
/// </para>
/// <para>
/// It is as useful against a system that is stuck as against one under test, which is why it reads rather than
/// controls: there is nothing here that can advance a position, settle a delivery, or create a file. Reading
/// still needs write permission on the directory, because SQLite creates its sidecars even for a read-only
/// connection.
/// </para>
/// </remarks>
public static class ChimeraInspector
{
    /// <summary>What every subscriber under this root is doing, at the moment it is read.</summary>
    /// <remarks>
    /// A file that cannot be read is not skipped. Retention skips one rather than fail over a subscriber it
    /// only wanted to report on; here a subscriber quietly missing from the answer is a test going green
    /// because nobody looked, so the failure is raised instead.
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

    /// <summary>What became of one message, as far as one subscriber is concerned.</summary>
    /// <remarks>
    /// Ask by message id, never by the sequence on an envelope: sequences come from two number spaces in the
    /// topic file and only look orderable. The translation from one to the other happens here.
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

        // Set aside first. A deferred or dead-lettered message is also terminal in the exception set, so
        // asking that first would answer "completed" for a message somebody is still holding a copy of.
        if (SetAside(connection, topic, messageId) is { } aside)
        {
            MessageState state = aside.Kind == SetAsideKind.Dead
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

        // No record of it in the exception set. Either the position has passed it, which for a row still in
        // the topic can only mean this subscriber finished with it, or it has not been handed out yet.
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

            // A position belonging to a different incarnation of the log says nothing about the log now
            // present, and the subscriber will refuse to read it. Reporting zero behind would read as settled.
            bool stranded = reader.IsOpen && !string.Equals(reader.LogUuid, logUuid, StringComparison.Ordinal);

            (_, long max) = reader.Bounds();
            Dictionary<(EntrySource, EntryState), long> entries = EntryStates(connection, topic);
            (long deferred, long dead, long replaying) = SetAsideCounts(connection, topic);

            // A scheduled row settled but not yet absorbed is still inside the due window, because the floor
            // moves in the reader rather than at the moment of settling. Counting it would say work is owed
            // for a message already handled, and a test waiting for quiet would wait for a poll that has not
            // happened yet. The log needs no such correction: its mark advances as part of settling.
            //
            // Which of those settled entries to discount is asked of the topic file rather than counted here.
            // The two numbers then come from one predicate over one table, so an entry whose row has been
            // removed drops out of both instead of being subtracted from a window it is no longer in — the
            // arithmetic that made this report a message as settled while it was genuinely owed.
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
        reader.TryOpen();   // a topic with no file yet reads as empty, exactly as a subscriber sees it
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

    /// <summary>Scheduled rows this subscriber has finished with and whose entry the floor has not reached.</summary>
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

            // Held is parked. Anything else is on its way out, so it is work still to happen.
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

    private static (SetAsideKind Kind, long Attempts, string? Reason, DateTimeOffset AsideAt)? SetAside(
        SqliteConnection connection, string topic, string messageId)
    {
        // Matched on the message id the row already carries. The origin column is provenance for a human
        // reading the file, and parsing it here would quietly make it load-bearing.
        using SqliteCommand command = Sqlite.Command(connection, null,
            "SELECT kind, attempts, reason, aside_at FROM set_aside " +
            "WHERE topic = $t AND msg_id = $m ORDER BY aside_id DESC LIMIT 1;");
        command.Parameters.AddWithValue("$t", topic);
        command.Parameters.AddWithValue("$m", messageId);
        using SqliteDataReader reader = command.ExecuteReader();
        if (!reader.Read())
            return null;

        return ((SetAsideKind)reader.GetInt32(0), reader.GetInt64(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(3)));
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
