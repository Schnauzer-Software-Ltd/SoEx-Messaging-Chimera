using Microsoft.Data.Sqlite;
using SoEx.Messaging.Chimera.Internal;

namespace SoEx.Messaging.Chimera;

/// <summary>
/// A topic stored as one SQLite file, written by this instance alone.
/// </summary>
public sealed class SqliteTopicLog : ITopicLog
{
    private readonly ChimeraOptions _options;
    private readonly TopicOptions _topicOptions;
    private readonly SingleWriter _writer;

    private SqliteTopicLog(string topic, string path, string logUuid, ChimeraOptions options,
        TopicOptions topicOptions, SingleWriter writer)
    {
        Topic = topic;
        Path = path;
        LogUuid = logUuid;
        _options = options;
        _topicOptions = topicOptions;
        _writer = writer;
    }

    public string Topic { get; }

    /// <summary>The file backing this topic. Open it read-only with any tool; it will not disturb anything.</summary>
    public string Path { get; }

    /// <summary>
    /// Identifies this log across restores. A subscriber records it, and refuses to carry on against a
    /// different one rather than skipping messages a restored log has reissued sequences for.
    /// </summary>
    public string LogUuid { get; }

    public static SqliteTopicLog Open(string topic, ChimeraOptions options, TopicOptions? topicOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(options);

        topicOptions ??= new TopicOptions();
        if (topicOptions.MaxPayloadBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(topicOptions),
                topicOptions.MaxPayloadBytes, "The payload limit must be greater than zero.");

        Directory.CreateDirectory(options.RootDirectory);
        string path = FileNames.TopicFile(options.RootDirectory, topic);

        SqliteConnection connection = Sqlite.OpenWriter(path, options);
        try
        {
            string uuid = TopicSchema.Initialise(connection, topic);
            return new SqliteTopicLog(topic, path, uuid, options, topicOptions,
                new SingleWriter(connection, options.MaxAppendBatch));
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    public ValueTask<long> AppendAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        EnsureWithinLimit(payload);
        byte[] bytes = payload.ToArray();
        string messageId = Sqlite.NewMessageId();

        return _writer.EnqueueAsync((connection, transaction) =>
        {
            // Stamped on the writer thread, for the same reason the scheduled path reads the clock there: a
            // stamp taken at the call and a sequence taken at the commit can disagree about which of two
            // records came first, and retention removes by sequence after choosing a cutoff by time.
            long enqueuedAt = Sqlite.ToUnixMs(_options.Clock.GetUtcNow());

            using SqliteCommand command = Sqlite.Command(connection, transaction,
                "INSERT INTO log (msg_id, enqueued_at, size, payload) VALUES ($m, $e, $s, $p) RETURNING seq;");
            command.Parameters.AddWithValue("$m", messageId);
            command.Parameters.AddWithValue("$e", enqueuedAt);
            command.Parameters.AddWithValue("$s", bytes.Length);
            command.Parameters.AddWithValue("$p", bytes);
            return (long)command.ExecuteScalar()!;
        }, cancellationToken);
    }

    public ValueTask<long> ScheduleAsync(ReadOnlyMemory<byte> payload, DateTimeOffset dueAt,
        CancellationToken cancellationToken = default)
    {
        EnsureWithinLimit(payload);
        byte[] bytes = payload.ToArray();
        string messageId = Sqlite.NewMessageId();

        return _writer.EnqueueAsync((connection, transaction) =>
        {
            // The clock is read here, on the writer thread, and not where this was called. A row becomes
            // visible when its batch commits, and a busy writer puts an arbitrary distance between the two: a
            // due time read at the call can have passed by the time the row lands, below a position another
            // delivery has moved past in the meantime, where nothing will ever offer it. Assigning due times
            // in commit order closes that. A row's due time is then at or after the moment it became visible,
            // so it is at or above any position set before that moment, and a position set after it has the
            // row in its window and stops at it rather than passing over it.
            DateTimeOffset now = _options.Clock.GetUtcNow();
            long enqueuedAt = Sqlite.ToUnixMs(now);

            // A due time already gone means due now, and is stored as now. Service Bus reads a scheduled
            // enqueue time in the past the same way. It also has to be stored that way here: a subscriber's
            // place in the scheduled table is a due time, so a row arriving behind one already delivered would
            // sit below that position and never be offered.
            long due = Sqlite.ToUnixMs(dueAt < now ? now : dueAt);

            using SqliteCommand command = Sqlite.Command(connection, transaction,
                "INSERT INTO scheduled (msg_id, enqueued_at, due_at, size, payload) " +
                "VALUES ($m, $e, $d, $s, $p) RETURNING id;");
            command.Parameters.AddWithValue("$m", messageId);
            command.Parameters.AddWithValue("$e", enqueuedAt);
            command.Parameters.AddWithValue("$d", due);
            command.Parameters.AddWithValue("$s", bytes.Length);
            command.Parameters.AddWithValue("$p", bytes);
            return (long)command.ExecuteScalar()!;
        }, cancellationToken);
    }

    public async ValueTask<bool> CancelScheduledAsync(long scheduledId, CancellationToken cancellationToken = default)
    {
        long removed = await _writer.EnqueueAsync((connection, transaction) =>
        {
            using SqliteCommand command = Sqlite.Command(connection, transaction,
                "DELETE FROM scheduled WHERE id = $i;");
            command.Parameters.AddWithValue("$i", scheduledId);
            return command.ExecuteNonQuery();
        }, cancellationToken).ConfigureAwait(false);

        return removed > 0;
    }

    public async ValueTask<IReadOnlyList<SubscriberPosition>> InspectSubscribersAsync(
        CancellationToken cancellationToken = default)
    {
        long highest = await _writer.EnqueueAsync((connection, transaction) =>
        {
            using SqliteCommand command = Sqlite.Command(connection, transaction,
                "SELECT COALESCE(MAX(seq), 0) FROM log;");
            return (long)command.ExecuteScalar()!;
        }, cancellationToken).ConfigureAwait(false);

        return SubscriberScan.Positions(_options, Topic, LogUuid, highest);
    }

    public async ValueTask<RetentionReport> ApplyRetentionAsync(CancellationToken cancellationToken = default)
    {
        RetentionPolicy policy = _topicOptions.Retention;
        long now = Sqlite.ToUnixMs(_options.Clock.GetUtcNow());
        long logRemovedTo = 0;
        long highest = 0;
        RetentionReport report = default;

        await _writer.EnqueueAsync((connection, transaction) =>
        {
            long cutoff = 0;   // highest log sequence to remove; 0 removes nothing

            if (policy.MaxAge is { } maxAge)
                cutoff = Math.Max(cutoff, HighestSequenceWhere(connection, transaction,
                    "enqueued_at < $v", now - (long)maxAge.TotalMilliseconds));

            if (policy.MaxCount is { } maxCount)
                cutoff = Math.Max(cutoff, Scalar(connection, transaction,
                    "SELECT COALESCE(MAX(seq), 0) - $v FROM log;", maxCount));

            if (policy.MaxBytes is { } maxBytes)
                cutoff = Math.Max(cutoff, Scalar(connection, transaction,
                    """
                    SELECT COALESCE(MAX(seq), 0) FROM (
                      SELECT seq, SUM(size) OVER (ORDER BY seq DESC) AS running FROM log
                    ) WHERE running > $v;
                    """, maxBytes));

            long logRemoved = 0;
            if (cutoff > 0)
            {
                using SqliteCommand delete = Sqlite.Command(connection, transaction,
                    "DELETE FROM log WHERE seq <= $v;");
                delete.Parameters.AddWithValue("$v", cutoff);
                logRemoved = delete.ExecuteNonQuery();
            }

            long scheduledRemoved = 0;
            if (policy.MaxAge is { } scheduledAge)
            {
                using SqliteCommand delete = Sqlite.Command(connection, transaction,
                    "DELETE FROM scheduled WHERE due_at < $v;");
                delete.Parameters.AddWithValue("$v", now - (long)scheduledAge.TotalMilliseconds);
                scheduledRemoved = delete.ExecuteNonQuery();
            }

            logRemovedTo = logRemoved > 0 ? cutoff : 0;
            using (var max = Sqlite.Command(connection, transaction, "SELECT COALESCE(MAX(seq), 0) FROM log;"))
                highest = (long)max.ExecuteScalar()!;

            report = new RetentionReport(logRemoved, logRemovedTo, scheduledRemoved, []);
            return logRemoved + scheduledRemoved;
        }, cancellationToken).ConfigureAwait(false);

        // Scanning after the delete gives the same answer as scanning before it — removing log records does not
        // move anyone's position, since those live in their own files — and it keeps the writer free meanwhile.
        IReadOnlyList<SubscriberOvertaken> overtaken = logRemovedTo == 0
            ? []
            : SubscriberScan.Positions(_options, Topic, LogUuid, highest)
                .Where(p => p.Mark < logRemovedTo)
                .Select(p => new SubscriberOvertaken(p.Subscriber, p.Mark, logRemovedTo - p.Mark))
                .ToList();

        return report with { Overtaken = overtaken };
    }

    /// <summary>Checked here, before anything is queued or copied, so a rejected message never reaches the file.</summary>
    private void EnsureWithinLimit(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length > _topicOptions.MaxPayloadBytes)
            throw new PayloadTooLargeException(Topic, payload.Length, _topicOptions.MaxPayloadBytes);
    }

    private static long HighestSequenceWhere(SqliteConnection connection, SqliteTransaction transaction,
        string predicate, long value)
    {
        using SqliteCommand command = Sqlite.Command(connection, transaction,
            $"SELECT COALESCE(MAX(seq), 0) FROM log WHERE {predicate};");
        command.Parameters.AddWithValue("$v", value);
        return (long)command.ExecuteScalar()!;
    }

    private static long Scalar(SqliteConnection connection, SqliteTransaction transaction, string sql, long value)
    {
        using SqliteCommand command = Sqlite.Command(connection, transaction, sql);
        command.Parameters.AddWithValue("$v", value);
        object? result = command.ExecuteScalar();
        return result is long l ? l : 0;
    }

    public ValueTask DisposeAsync() => _writer.DisposeAsync();
}
