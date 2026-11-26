using Microsoft.Data.Sqlite;
using SoEx.Messaging.Chimera.Internal;

namespace SoEx.Messaging.Chimera;

/// <summary>
/// A topic stored in one SQLite file. This instance is the only writer of the file.
/// </summary>
public sealed class SqliteTopicLog : ITopicLog
{
    private readonly ChimeraOptions _options;
    private readonly TopicOptions _topicOptions;
    private readonly SingleWriter _writer;
    private readonly MonotonicClock _stamps;

    private SqliteTopicLog(string topic, string path, string logUuid, ChimeraOptions options,
        TopicOptions topicOptions, SingleWriter writer, MonotonicClock stamps)
    {
        Topic = topic;
        Path = path;
        LogUuid = logUuid;
        _options = options;
        _topicOptions = topicOptions;
        _writer = writer;
        _stamps = stamps;
    }

    /// <inheritdoc/>
    public string Topic { get; }

    /// <summary>The file that holds this topic. Any tool can open it read-only with no effect on the system.</summary>
    public string Path { get; }

    /// <summary>
    /// The identity of this log across restores. A subscriber records it. If the identity changes, the
    /// subscriber refuses to continue. A restored log can reissue sequences, and the refusal prevents skipped
    /// messages.
    /// </summary>
    public string LogUuid { get; }

    /// <summary>Opens the writer for <paramref name="topic"/>. The first open creates the topic file.</summary>
    /// <remarks>
    /// Share one instance per topic per process. Each file has one writer. A second writer on the same topic
    /// file causes lock contention.
    /// </remarks>
    /// <param name="topic">The name of the topic.</param>
    /// <param name="options">The root directory, and the settings for all topics and subscribers under it.</param>
    /// <param name="topicOptions">The settings for this topic. If null, the defaults apply.</param>
    /// <returns>The open log. Dispose it to close it.</returns>
    public static SqliteTopicLog Open(string topic, ChimeraOptions options, TopicOptions? topicOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(options);

        topicOptions ??= new TopicOptions();
        if (topicOptions.MaxPayloadBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(topicOptions),
                topicOptions.MaxPayloadBytes, "The payload limit must be greater than zero.");

        // A null retention setting means no bound, so the checks apply to supplied values only. A bad value
        // fails silently and destroys records. An age of zero or less gives a cutoff at or after the current
        // time. That cutoff removes every record in the log, including unread records published a moment ago. A
        // size or count of zero or less has the same effect.
        RetentionPolicy retention = topicOptions.Retention;
        if (retention.MaxAge is { } maxAge && maxAge <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(topicOptions), maxAge,
                "A retention age must be greater than zero. Use null for no age bound.");
        if (retention.MaxBytes is { } maxBytes && maxBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(topicOptions), maxBytes,
                "A retention size must be greater than zero. Use null for no size bound.");
        if (retention.MaxCount is { } maxCount && maxCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(topicOptions), maxCount,
                "A retention count must be greater than zero. Use null for no count bound.");

        // A byte bound below the payload limit cannot keep a message at that limit. The topic accepts and
        // writes the message. The next retention pass removes it as the record that put the log over its
        // size, possibly before a subscriber reads it. Open refuses the setting as a configuration error. At
        // open, no message is at risk. At the append, a caller already has the message. The dead set has the
        // same rule: it refuses a dead letter that it cannot keep. This check applies that rule to the log,
        // the part of the file that keeps history.
        if (retention.MaxBytes is { } bound && bound < topicOptions.MaxPayloadBytes)
            throw new ArgumentOutOfRangeException(nameof(topicOptions), bound,
                $"A retention size of {bound} bytes is below the topic's payload limit of " +
                $"{topicOptions.MaxPayloadBytes} bytes, so a message at that limit would be removed by the " +
                "next retention pass, unread. Raise MaxBytes to at least the payload limit, or lower " +
                "MaxPayloadBytes to what retention can keep.");

        Directory.CreateDirectory(options.RootDirectory);
        string path = FileNames.TopicFile(options.RootDirectory, topic);

        SqliteConnection connection = Sqlite.OpenWriter(path, options);
        try
        {
            string uuid = TopicSchema.Initialise(connection, topic);
            // The high-water is a field. Its seed comes from the file, so it survives a restart. A process
            // that restarts after a backwards clock step then continues to distrust the stepped clock.
            var stamps = new MonotonicClock(options.Clock, NewestStamp(connection));
            return new SqliteTopicLog(topic, path, uuid, options, topicOptions,
                new SingleWriter(connection, options.MaxAppendBatch), stamps);
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public ValueTask<long> AppendAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        EnsureWithinLimit(payload);
        byte[] bytes = payload.ToArray();
        string messageId = Sqlite.NewMessageId();

        return _writer.EnqueueAsync((connection, transaction) =>
        {
            // The stamp comes from the writer thread. The scheduled path reads the clock there for the same
            // reason. A stamp from the call and a sequence from the commit can disagree on which of two
            // records came first. Retention selects a cutoff by time and then removes by sequence.
            long enqueuedAt = _stamps.NowMs();

            using SqliteCommand command = Sqlite.Command(connection, transaction,
                "INSERT INTO log (msg_id, enqueued_at, size, payload) VALUES ($m, $e, $s, $p) RETURNING seq;");
            command.Parameters.AddWithValue("$m", messageId);
            command.Parameters.AddWithValue("$e", enqueuedAt);
            command.Parameters.AddWithValue("$s", bytes.Length);
            command.Parameters.AddWithValue("$p", bytes);
            return (long)command.ExecuteScalar()!;
        }, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask<long> ScheduleAsync(ReadOnlyMemory<byte> payload, DateTimeOffset dueAt,
        CancellationToken cancellationToken = default)
    {
        EnsureWithinLimit(payload);
        byte[] bytes = payload.ToArray();
        string messageId = Sqlite.NewMessageId();
        ScheduleRefusedException? refused = null;

        long id = await _writer.EnqueueAsync((connection, transaction) =>
        {
            refused = null;
            // The clock is read here, on the writer thread. A row becomes visible when its batch commits. A
            // busy writer can put any amount of time between the call and the commit. A due time read at the
            // call can pass before the row lands. The row then lands below a position that another delivery
            // moved past, and no read ever offers it. Due times assigned in commit order prevent this. A row's
            // due time is then at or after the time that the row became visible. So it is at or above each
            // position set before that time. A position set after that time has the row in its window and
            // stops at it.
            DateTimeOffset now = _stamps.Now();
            long enqueuedAt = Sqlite.ToUnixMs(now);

            // A due time in the past means due now. The row stores it as now. Service Bus reads a scheduled
            // enqueue time in the past the same way. The storage as now is necessary here. A subscriber's
            // position in the scheduled table is a due time. A row with a due time behind a delivered row
            // would stay below that position, and no read would offer it.
            long due = Sqlite.ToUnixMs(dueAt < now ? now : dueAt);

            // The schedule call applies this bound. These rows are work that a caller requested for a future
            // time. Retention trims history, and a removal of these rows would cancel future work. So the
            // pressure goes back to the publisher. Service Bus does the same: scheduled messages count against
            // the entity quota, and Service Bus refuses the send. The measurement occurs inside the write, as
            // for every other bound here. Two schedules that arrive together then cannot both find room in a
            // table that neither has joined yet.
            if (Refusal(connection, transaction, bytes.Length, Sqlite.ToUnixMs(now)) is { } stopped)
            {
                refused = stopped;
                return 0;
            }

            using SqliteCommand command = Sqlite.Command(connection, transaction,
                "INSERT INTO scheduled (msg_id, enqueued_at, due_at, size, payload) " +
                "VALUES ($m, $e, $d, $s, $p) RETURNING id;");
            command.Parameters.AddWithValue("$m", messageId);
            command.Parameters.AddWithValue("$e", enqueuedAt);
            command.Parameters.AddWithValue("$d", due);
            command.Parameters.AddWithValue("$s", bytes.Length);
            command.Parameters.AddWithValue("$p", bytes);
            return (long)command.ExecuteScalar()!;
        }, cancellationToken).ConfigureAwait(false);

        // The refusal is thrown here, outside the write. A refusal belongs to the caller that caused it. A
        // refusal inside the write would roll back the batch, and the appends of other callers would fail.
        return refused is null ? id : throw refused;
    }

    /// <summary>
    /// Gets a refusal if the scheduled table is at a bound that the retention policy sets. The log uses the
    /// same pair of settings for trimming. Here they are a quota. The log removes its oldest records at the
    /// bound, and the scheduled table refuses the newest. So the maximum file size is the sum of the two bounds.
    /// </summary>
    /// <remarks>
    /// The count includes only the messages that wait for their due time. A scheduled row stays after
    /// delivery. With fan-out, the publisher cannot remove the row. Only retention removes it. A count of the
    /// full table would count delivered history as pending work. Five due-now messages, consumed and completed,
    /// would then refuse a sixth schedule until <c>MaxAge</c> expired, days later, with no work pending. The
    /// Service Bus quota is the model for this quota. Service Bus counts the waiting messages, because it
    /// removes a scheduled message from the table at enqueue.
    /// <para>
    /// So this value bounds future-dated work, and <c>MaxAge</c> bounds all other work. This is the documented
    /// trade-off, and it reaches further than a tail. A due time in the past is stored as now. Such rows are
    /// outside the counted set from the time of the write, and they never hold the quota. A schedule for now
    /// is outside the count. This is on purpose. A better count needs the amount of work still owed, and the
    /// publisher cannot know it. Delivery state is in each subscriber file, and fan-out gives no single answer.
    /// A count of the full table has the problem given above. The sizing calculation is on
    /// <c>RetentionPolicy</c>, where a person who selects the numbers reads it.
    /// </para>
    /// </remarks>
    private ScheduleRefusedException? Refusal(SqliteConnection connection, SqliteTransaction transaction,
        int size, long nowMs)
    {
        RetentionPolicy retention = _topicOptions.Retention;
        if (retention.MaxCount is null && retention.MaxBytes is null)
            return null;

        using var command = Sqlite.Command(connection, transaction,
            "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM scheduled WHERE due_at > $now;");
        command.Parameters.AddWithValue("$now", nowMs);
        using SqliteDataReader reader = command.ExecuteReader();
        if (!reader.Read())
            return null;

        long count = reader.GetInt64(0);
        long bytes = reader.GetInt64(1);

        if (retention.MaxCount is { } maxCount && count >= maxCount)
            return new ScheduleRefusedException(Topic, count, bytes, maxCount, SizeBound.Count);

        return retention.MaxBytes is { } maxBytes && bytes + size > maxBytes
            ? new ScheduleRefusedException(Topic, count, bytes, maxBytes, SizeBound.Bytes)
            : null;
    }

    /// <inheritdoc/>
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

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<SubscriberPosition>> InspectSubscribersAsync(
        CancellationToken cancellationToken = default)
    {
        long highest = await _writer.EnqueueAsync((connection, transaction)
            => HighestSequence(connection, transaction), cancellationToken).ConfigureAwait(false);

        return SubscriberScan.Positions(_options, Topic, LogUuid, highest);
    }

    /// <inheritdoc/>
    public async ValueTask<RetentionReport> ApplyRetentionAsync(CancellationToken cancellationToken = default)
    {
        RetentionPolicy policy = _topicOptions.Retention;
        long logRemovedTo = 0;
        long highest = 0;
        RetentionReport report = default;

        await _writer.EnqueueAsync((connection, transaction) =>
        {
            // The time is read here, on the writer thread, through the high-water that wrote the stamps. An
            // age cutoff selects a sequence by a time comparison. The delete then removes by sequence. If the
            // two disagree on which record came first, the delete removes records that the policy keeps.
            long now = _stamps.NowMs();

            long cutoff = 0;   // highest log sequence to remove. 0 removes nothing.

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
            highest = HighestSequence(connection, transaction);

            report = new RetentionReport(logRemoved, logRemovedTo, scheduledRemoved, []);
            return logRemoved + scheduledRemoved;
        }, cancellationToken).ConfigureAwait(false);

        // A scan after the delete gives the same result as a scan before it. Each position is in its subscriber
        // file, so a log removal leaves every position unchanged. The writer stays free during the scan.
        IReadOnlyList<SubscriberOvertaken> overtaken = logRemovedTo == 0
            ? []
            : SubscriberScan.Positions(_options, Topic, LogUuid, highest)
                .Where(p => p.Mark < logRemovedTo)
                .Select(p => new SubscriberOvertaken(p.Subscriber, p.Mark, logRemovedTo - p.Mark))
                .ToList();

        return report with { Overtaken = overtaken };
    }

    /// <summary>Checks the payload size before the message is queued or copied. A rejected message never reaches the file.</summary>
    private void EnsureWithinLimit(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length > _topicOptions.MaxPayloadBytes)
            throw new PayloadTooLargeException(Topic, payload.Length, _topicOptions.MaxPayloadBytes);
    }

    /// <summary>
    /// The highest sequence that this log has issued. It can be higher than the highest sequence that the log
    /// holds.
    /// </summary>
    /// <remarks>
    /// The value is the higher of the AUTOINCREMENT counter and the highest sequence in the log. A log that
    /// retention trimmed to zero records keeps its highest issued sequence in the counter. The difference from a subscriber position is the full amount that the
    /// subscriber is behind. A value of zero would show each subscriber as up to date, over records that
    /// retention destroyed unread. The counter is durable and never decreases. That is the reason for the
    /// declaration of <c>seq</c>.
    /// </remarks>
    private static long HighestSequence(SqliteConnection connection, SqliteTransaction transaction)
    {
        using SqliteCommand command = Sqlite.Command(connection, transaction,
            """
            SELECT MAX(
              (SELECT COALESCE(MAX(seq), 0) FROM log),
              COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'log'), 0));
            """);
        return (long)command.ExecuteScalar()!;
    }

    private static long HighestSequenceWhere(SqliteConnection connection, SqliteTransaction transaction,
        string predicate, long value)
    {
        using SqliteCommand command = Sqlite.Command(connection, transaction,
            $"SELECT COALESCE(MAX(seq), 0) FROM log WHERE {predicate};");
        command.Parameters.AddWithValue("$v", value);
        return (long)command.ExecuteScalar()!;
    }

    /// <summary>
    /// The newest time stamp in this file. The query excludes due times on purpose. A due time next week is a
    /// future value. It is no evidence of a clock reading.
    /// </summary>
    private static long NewestStamp(SqliteConnection connection)
    {
        using SqliteCommand command = Sqlite.Command(connection, null,
            """
            SELECT MAX(newest) FROM (
              SELECT COALESCE(MAX(enqueued_at), 0) AS newest FROM log
              UNION ALL
              SELECT COALESCE(MAX(enqueued_at), 0) FROM scheduled
            );
            """);
        return command.ExecuteScalar() is long value ? value : 0;
    }

    private static long Scalar(SqliteConnection connection, SqliteTransaction transaction, string sql, long value)
    {
        using SqliteCommand command = Sqlite.Command(connection, transaction, sql);
        command.Parameters.AddWithValue("$v", value);
        object? result = command.ExecuteScalar();
        return result is long l ? l : 0;
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _writer.DisposeAsync();
}
