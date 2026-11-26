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

    /// <summary>The file backing this topic. Open it read-only with any tool; it will not disturb anything.</summary>
    public string Path { get; }

    /// <summary>
    /// Identifies this log across restores. A subscriber records it, and refuses to carry on against a
    /// different one rather than skipping messages a restored log has reissued sequences for.
    /// </summary>
    public string LogUuid { get; }

    /// <summary>Opens the writer for <paramref name="topic"/>. The first open creates the topic's file.</summary>
    /// <remarks>
    /// Share one instance per topic per process. Each file has one writer. A second writer on the same file
    /// shows up as lock contention.
    /// </remarks>
    /// <param name="topic">The topic's name.</param>
    /// <param name="options">The root directory and the settings shared by every topic and subscriber under it.</param>
    /// <param name="topicOptions">Settings for this topic. The defaults apply if this is null.</param>
    /// <returns>The open log. Dispose it to close it.</returns>
    public static SqliteTopicLog Open(string topic, ChimeraOptions options, TopicOptions? topicOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(options);

        topicOptions ??= new TopicOptions();
        if (topicOptions.MaxPayloadBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(topicOptions),
                topicOptions.MaxPayloadBytes, "The payload limit must be greater than zero.");

        // Null is how a retention setting says "unbounded", so only supplied values are checked — and they
        // are checked because the way they fail is silent and destructive. A negative age is a cutoff in the
        // future, and a cutoff in the future removes every record in the log, including ones published a
        // moment ago that nobody has read. A negative size or count is the same arithmetic.
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

        // A byte bound smaller than the largest message the topic will accept is a bound no such message can
        // survive: it is accepted, written, and removed by the next retention pass as the record that put the
        // log over its size — possibly before anyone read it. Refused here, where it is a configuration
        // mistake with nothing at stake yet, rather than at the append, where the message is already in
        // somebody's hands. It is the same rule the dead set follows when it refuses a dead letter it could
        // not then keep, on the side of the file that keeps history.
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
            // Seeded from the file rather than started at zero: the high-water is a field, and a process that
            // restarts after a backwards step would otherwise begin by trusting the stepped clock again.
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
            // Stamped on the writer thread, for the same reason the scheduled path reads the clock there: a
            // stamp taken at the call and a sequence taken at the commit can disagree about which of two
            // records came first, and retention removes by sequence after choosing a cutoff by time.
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
            // The clock is read here, on the writer thread, and not where this was called. A row becomes
            // visible when its batch commits, and a busy writer puts an arbitrary distance between the two: a
            // due time read at the call can have passed by the time the row lands, below a position another
            // delivery has moved past in the meantime, where nothing will ever offer it. Assigning due times
            // in commit order closes that. A row's due time is then at or after the moment it became visible,
            // so it is at or above any position set before that moment, and a position set after it has the
            // row in its window and stops at it rather than passing over it.
            DateTimeOffset now = _stamps.Now();
            long enqueuedAt = Sqlite.ToUnixMs(now);

            // A due time already gone means due now, and is stored as now. Service Bus reads a scheduled
            // enqueue time in the past the same way. It also has to be stored that way here: a subscriber's
            // place in the scheduled table is a due time, so a row arriving behind one already delivered would
            // sit below that position and never be offered.
            long due = Sqlite.ToUnixMs(dueAt < now ? now : dueAt);

            // The bound is answered here rather than by retention, because retention has no answer to give:
            // these rows are work somebody asked for at a time that has not arrived, and removing them to
            // stay under a size bound would be cancelling the future rather than trimming history. So the
            // pressure goes back to the publisher, which is where Service Bus puts it too — scheduled
            // messages count against the entity quota and the send is refused. Measured inside the write for
            // the same reason every other bound here is: two schedules arriving together would otherwise both
            // find room in a table neither had joined yet.
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

        // Thrown out here rather than from inside the write. A refusal belongs to the caller that caused it,
        // and a batch rolled back by one would fail everybody else's appends with somebody else's bound.
        return refused is null ? id : throw refused;
    }

    /// <summary>
    /// Whether the scheduled table is at a bound the retention policy sets. It is the same pair of settings
    /// that trims the log, read here as a quota instead: what the log answers by removing its oldest, this
    /// answers by refusing the newest. Which means the file's ceiling is the two bounds added together, not
    /// either one of them — the log is held at its bound by trimming and the scheduled table by refusing.
    /// </summary>
    /// <remarks>
    /// What it counts is the messages still waiting for their due time, not every row in the table. A
    /// scheduled row stays after it has been delivered — fan-out means the publisher cannot remove it, and
    /// only retention does — so counting the table counts delivered history as pending work: five due-now
    /// messages consumed and completed would refuse the sixth schedule until <c>MaxAge</c> came round, days
    /// later, with nothing whatsoever pending. That is not the quota this is modelled on either; Service Bus
    /// counts what is still waiting, because it takes a scheduled message out of the table at enqueue.
    /// <para>
    /// Which makes this a bound on future-dated work, and <c>MaxAge</c> the bound on everything else — the
    /// documented trade, and it reaches further than a tail. A due time already gone is stored as now, so
    /// those rows are outside the counted set from the moment they are written and never hold the quota at
    /// all: schedule for now and the count does not apply. That is deliberate. The number this would have to
    /// count to do better is work still owed, and the publisher cannot know it — delivery lives in each
    /// subscriber's file, and fan-out means there is no single answer — while counting the whole table is the
    /// thing described above. The sizing arithmetic is on <c>RetentionPolicy</c>, where someone choosing the
    /// numbers will read it.
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
            // Read here, on the writer thread and through the same high-water the stamps were written
            // through. An age cutoff chooses a sequence by comparing times, and then the delete removes by
            // sequence: if the two disagree about which record came first, the delete takes records the
            // policy says to keep.
            long now = _stamps.NowMs();

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
            highest = HighestSequence(connection, transaction);

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

    /// <summary>
    /// The highest sequence this log has ever issued, which is not the same as the highest it still holds.
    /// </summary>
    /// <remarks>
    /// Taken from the AUTOINCREMENT counter rather than from the rows, so that a log retention has trimmed to
    /// nothing does not read as a log with nothing in it: the difference is the whole of what a subscriber is
    /// behind by, and reporting it as zero says everybody is up to date over records that were destroyed
    /// unread. The counter is durable and never goes backwards, which is why <c>seq</c> is declared the way
    /// it is.
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
    /// The newest moment this file has already been stamped with. Due times are deliberately not consulted:
    /// a row due next week is a statement about the future, not evidence that the clock ever read it.
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
