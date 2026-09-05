namespace SoEx.Messaging.Chimera;

/// <summary>How durable each commit is. Neither setting affects whether a crash of the process loses data.</summary>
public enum Durability
{
    /// <summary>Survives a process crash but not a power loss. The default, and what SQLite calls NORMAL.</summary>
    ProcessCrash = 0,

    /// <summary>Survives a power loss, at the cost of an fsync per commit. SQLite's FULL.</summary>
    PowerLoss = 1,
}

/// <summary>
/// When old records are removed. Trimming is by retention, never by what subscribers have read: gating on
/// consumption turns bounded loss into unbounded growth that lands on the publisher, and lets one stalled
/// subscriber stop every append.
/// </summary>
/// <remarks>
/// A topic holds two tables and these do not apply to both. <see cref="MaxBytes"/> and
/// <see cref="MaxCount"/> trim the log and leave scheduled messages alone: a scheduled message is work
/// somebody asked for at a time that has not arrived, and destroying it to stay under a size bound is not
/// retention of history, it is cancelling the future. <see cref="MaxAge"/> is the one that reaches both, and
/// on the scheduled side it is measured from the due time, so a message removes itself a retention window
/// after it came due rather than after it was published.
/// <para>
/// <see cref="MaxBytes"/> and <see cref="MaxCount"/> still bound the scheduled table, but by refusing rather
/// than by removing: when it is at the bound, <c>ScheduleAsync</c> throws <c>ScheduleRefusedException</c> and
/// stores nothing. Destroying a scheduled message to stay under a size bound would be cancelling the future
/// rather than trimming history, so the pressure goes back to the publisher, which is where Service Bus puts
/// it too. Ordinary appends are unaffected. The file's ceiling is therefore these bounds counted twice — once
/// for the log, held there by trimming, and once for the scheduled table, held there by refusing.
/// </para>
/// <para>
/// On the scheduled side those two bound the messages still waiting for a due time, and the rest of the table
/// is bounded by <see cref="MaxAge"/> instead. So size it as <c>MaxCount</c> plus the rate of due-or-past-due
/// schedules times <c>MaxAge</c>: a due time that has already arrived is stored as now, which puts the row
/// outside the counted set from the moment it is written, and a publisher scheduling for now is bounded by age
/// alone. There is no option that counts the past-due tail, because the bound would have to mean work still
/// owed and no publisher can know that — delivery is recorded in each subscriber's own file, and fan-out means
/// there is no one answer. Heavy due-now traffic belongs in <c>AppendAsync</c>, which is what a message with
/// no waiting to do is.
/// </para>
/// </remarks>
public sealed record RetentionPolicy
{
    /// <summary>
    /// Remove log records older than this, and scheduled messages this long past their due time. Null leaves
    /// age unbounded, which also leaves the scheduled table with no bound at all.
    /// </summary>
    public TimeSpan? MaxAge { get; init; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Remove oldest log records once their stored payload exceeds this, and refuse a scheduled message once
    /// the scheduled table holds this many bytes. Null leaves size unbounded — see the remarks on this type.
    /// </summary>
    public long? MaxBytes { get; init; }

    /// <summary>
    /// Remove oldest log records once this many are stored, and refuse a scheduled message once the scheduled
    /// table holds this many. Null leaves count unbounded — see the remarks on this type.
    /// </summary>
    public long? MaxCount { get; init; }

    /// <summary>Nothing is ever removed. Sound only when something else bounds the disk.</summary>
    public static RetentionPolicy Unbounded => new() { MaxAge = null };
}

/// <summary>Settings shared by every topic and subscriber under one root directory.</summary>
public sealed record ChimeraOptions
{
    /// <summary>Directory holding the topic and subscriber files. Created if absent.</summary>
    public required string RootDirectory { get; init; }

    public Durability Durability { get; init; } = Durability.ProcessCrash;

    /// <summary>How long a connection waits for a lock before failing. Also covers checkpointing pauses.</summary>
    public TimeSpan BusyTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Appends waiting at the moment the writer wakes are committed in one transaction, up to this many. At
    /// least one, and refused below that rather than raised to it: a value that is quietly overridden is a
    /// setting whose effect nobody can see, and here the effect is one commit per operation.
    /// </summary>
    public int MaxAppendBatch { get; init; } = 128;

    /// <summary>
    /// Where "now" comes from: stamping records, deciding what is due, and ageing things out.
    /// </summary>
    /// <remarks>
    /// <para>
    /// It is waited on in one place as well as read. A reader with nothing to deliver pauses before looking
    /// again, for no longer than <see cref="SubscriptionOptions.PollInterval"/>, and that pause is taken from
    /// this clock. It is a poll and not a schedule: there is no timer per message, and nothing here wakes up
    /// in order to deliver one.
    /// </para>
    /// <para>
    /// Which matters when you substitute a clock. A provider that answers GetUtcNow but does not drive timers
    /// leaves that pause on the real clock, so a reader meant to wake as a scheduled message falls due gets it
    /// from the next poll instead and the waking path never runs. A test written against one of those passes
    /// because the poll happened to fire, which is not what it says it checked. Under a clock you control, a
    /// reader wakes only when you advance it, and advancing fires the timers already registered and no others,
    /// so advance while nothing is part-way through a read.
    /// </para>
    /// </remarks>
    public TimeProvider Clock { get; init; } = TimeProvider.System;
}

/// <summary>Settings for one topic log.</summary>
public sealed record TopicOptions
{
    public RetentionPolicy Retention { get; init; } = new();

    /// <summary>
    /// Largest payload this topic accepts, counting the message bytes and nothing else.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The default is Service Bus's standard-tier limit, which is also SQS's. NATS and Kafka allow a megabyte,
    /// so 256 KB is the smallest of the places someone is likely to graduate to, and the rule here is to be no
    /// more forgiving than the destination: a message that works locally and is rejected in production is the
    /// failure that costs the most to find.
    /// </para>
    /// <para>
    /// It is not exact parity, and the difference runs the wrong way. Service Bus counts the whole encoded
    /// message — its envelope and any application properties as well as the body — against its 256 KB, whereas
    /// this counts the payload alone, because the payload is all this library has. A message that fits here can
    /// therefore still be refused there by the size of an envelope this library cannot see. If you routinely
    /// publish near the cap, lower it rather than find that out after moving.
    /// </para>
    /// </remarks>
    public int MaxPayloadBytes { get; init; } = 256 * 1024;
}
