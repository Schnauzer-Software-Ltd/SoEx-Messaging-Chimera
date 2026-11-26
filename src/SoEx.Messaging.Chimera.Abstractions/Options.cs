namespace SoEx.Messaging.Chimera;

/// <summary>How durable each commit is. Both settings survive a process crash.</summary>
public enum Durability
{
    /// <summary>Survives a process crash. A power loss can lose data. This is the default. SQLite calls it NORMAL.</summary>
    ProcessCrash = 0,

    /// <summary>Survives a power loss. Each commit costs one fsync. SQLite calls it FULL.</summary>
    PowerLoss = 1,
}

/// <summary>
/// Sets when old records are removed. Retention trims by age, size and count. Reads by subscribers have no
/// effect on it.
/// </summary>
/// <remarks>
/// <para>
/// A trim that waited for consumption would turn bounded loss into unbounded growth on the publisher. One
/// stalled subscriber would then stop all appends.
/// </para>
/// <para>
/// A topic holds two tables, and the bounds apply differently to them. <see cref="MaxBytes"/> and
/// <see cref="MaxCount"/> trim the log only. A scheduled message is work that somebody asked for at a future
/// time. Removal of it to stay under a size bound would cancel future work. <see cref="MaxAge"/> applies to
/// both tables. For the scheduled table, <c>MaxAge</c> counts from the due time. Thus a scheduled message is
/// removed one retention window after its due time.
/// </para>
/// <para>
/// <see cref="MaxBytes"/> and <see cref="MaxCount"/> also bound the scheduled table, by refusal. At the bound,
/// <c>ScheduleAsync</c> throws <c>ScheduleRefusedException</c> and stores nothing. The pressure goes back to
/// the publisher, as in Service Bus. Ordinary appends continue. Thus the maximum file size is these bounds
/// counted twice: once for the log, held by trimming, and once for the scheduled table, held by refusal.
/// </para>
/// <para>
/// In the scheduled table, these two bounds count only the messages that wait for a due time.
/// <see cref="MaxAge"/> bounds the rest of the table. Size the table as <c>MaxCount</c> plus the rate of due
/// or past-due schedules multiplied by <c>MaxAge</c>. A due time that has already arrived is stored as the
/// current time. Such a row is outside the counted set from the time of its write. Thus <c>MaxAge</c> alone
/// bounds a publisher that schedules for now.
/// </para>
/// <para>
/// No option counts the past-due rows. Such a bound would have to count work still owed, and no publisher can
/// know that. Each subscriber records delivery in its subscriber file. With fan-out, there is no single answer.
/// Send heavy due-now traffic with <c>AppendAsync</c>. <c>AppendAsync</c> is the operation for a message with
/// no wait.
/// </para>
/// </remarks>
public sealed record RetentionPolicy
{
    /// <summary>
    /// The maximum age of log records, and the maximum time that scheduled messages stay after their due time.
    /// If null, age has no bound, and the scheduled table has no bound at all.
    /// </summary>
    public TimeSpan? MaxAge { get; init; } = TimeSpan.FromDays(7);

    /// <summary>
    /// The maximum stored payload bytes. Above it, the oldest log records are removed. At it, the scheduled table
    /// refuses new scheduled messages. If null, size has no bound. See the remarks on this type.
    /// </summary>
    public long? MaxBytes { get; init; }

    /// <summary>
    /// The maximum number of stored rows. Above it, the oldest log records are removed. At it, the scheduled
    /// table refuses new scheduled messages. If null, count has no bound. See the remarks on this type.
    /// </summary>
    public long? MaxCount { get; init; }

    /// <summary>A policy that never removes a record. Use it only if something else bounds the disk.</summary>
    public static RetentionPolicy Unbounded => new() { MaxAge = null };
}

/// <summary>Settings shared by each topic and subscriber under one root directory.</summary>
public sealed record ChimeraOptions
{
    /// <summary>The directory that holds the topic files and the subscriber files. It is created if it is absent.</summary>
    public required string RootDirectory { get; init; }

    /// <summary>How durable each commit is. The default is <see cref="Durability.ProcessCrash"/>.</summary>
    public Durability Durability { get; init; } = Durability.ProcessCrash;

    /// <summary>How long a connection waits for a lock before it fails. This time also covers checkpoint pauses.</summary>
    public TimeSpan BusyTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The maximum number of appends in one transaction. When the writer wakes, it commits the waiting appends
    /// together, up to this number.
    /// </summary>
    /// <remarks>
    /// The minimum is 1. A lower value is refused. The library does not raise it to 1. A value that is
    /// silently overridden has an effect that nobody can see. Here, that effect would be one commit for each operation.
    /// </remarks>
    public int MaxAppendBatch { get; init; } = 128;

    /// <summary>
    /// The source of the current time. It stamps records, decides what is due, and ages records out.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This library also waits on this clock in one place. A reader with nothing to deliver pauses before it
    /// checks again, for no longer than <see cref="SubscriptionOptions.PollInterval"/>. That pause uses this
    /// clock. It is a poll. Each message has no timer, and no process wakes to deliver a message.
    /// </para>
    /// <para>
    /// This is important when you substitute a clock. Some providers answer GetUtcNow and do not drive timers.
    /// With such a provider, the pause stays on the real clock. A reader that must wake when a scheduled message
    /// falls due then gets the message from the next poll. The wake path never runs. A test against such a
    /// provider passes because the poll fired. That test does not check what it claims to check. Under a clock
    /// that you control, a reader wakes only when you advance the clock. An advance fires only the timers that
    /// are already registered. Advance the clock only when no read is in progress.
    /// </para>
    /// <para>
    /// The writer of a file never sees this clock go backwards. The writer holds its stamps at the newest time
    /// that the file has recorded. At open, the writer reads that time from the file. Thus a correction that
    /// moves the wall clock back cannot write a record that seems older than an existing record. Examples of
    /// such a correction are NTP, a resumed virtual machine, or a person who sets the date. Three things depend
    /// on this:
    /// <list type="bullet">
    /// <item>The position of a subscriber in the scheduled table.</item>
    /// <item>The age cutoff for retention deletes.</item>
    /// <item>The seek for the oldest record after a time.</item>
    /// </list>
    /// The cost: after a step back, a message published as due now falls due when the wall clock catches up.
    /// </para>
    /// <para>
    /// The rule holds within one file. Processes that write different files in one root directory use separate
    /// clocks. One file cannot put the stamps of two clocks in order. For this reason, the boundary is one machine.
    /// </para>
    /// </remarks>
    public TimeProvider Clock { get; init; } = TimeProvider.System;
}

/// <summary>Settings for one topic log.</summary>
public sealed record TopicOptions
{
    /// <summary>When the old records of the topic are removed. If this is not set, the defaults of <see cref="RetentionPolicy"/> apply.</summary>
    public RetentionPolicy Retention { get; init; } = new();

    /// <summary>
    /// The largest payload that this topic accepts. The limit counts the message bytes only.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The default is the Service Bus standard-tier limit, which is also the SQS limit. NATS and Kafka allow one
    /// megabyte. Thus 256 KB is the smallest limit of the likely destinations. The rule here is to be no more
    /// lenient than the destination. A message that works locally and fails in production is the most costly
    /// failure to find.
    /// </para>
    /// <para>
    /// The match is not exact, and the difference is in the unsafe direction. Service Bus counts the full
    /// encoded message against its 256 KB: the envelope, the application properties and the body. This library
    /// counts the payload only, because the payload is all that it has. Thus Service Bus can refuse a message
    /// that fits here, because of an envelope that this library cannot see. If you often publish near the
    /// limit, lower the limit before a move.
    /// </para>
    /// </remarks>
    public int MaxPayloadBytes { get; init; } = 256 * 1024;
}
