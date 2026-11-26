namespace SoEx.Messaging.Chimera;

/// <summary>
/// One delivered message.
/// </summary>
/// <remarks>
/// The envelope has the same shape for each source: the log, the scheduled table, or a replayed set-aside row.
/// Consumer code thus has no need to know the source.
/// </remarks>
/// <param name="MessageId">
/// The identifier of the message. It is opaque, stable and unique across all sources. Use it to identify a
/// message. Do not use <see cref="Sequence"/> for this. A sequence comes from one of three number spaces, and
/// its order has no meaning.
/// </param>
/// <param name="EnqueuedAt">
/// The time of the original publish. It stays the same after deferral and replay. Use it to measure end-to-end
/// latency.
/// </param>
/// <param name="EligibleAt">
/// The time when this delivery became available: the append time for a logged message, the due time for a
/// scheduled message, and the replay time for a set-aside message. Lag is measured from this time.
/// </param>
/// <param name="Attempt">The delivery attempt. The first attempt is 1.</param>
/// <param name="Payload">The message body as it was published.</param>
/// <param name="Tag">
/// Identifies this delivery. To settle the delivery, pass the envelope back. The subscription uses the tag to
/// find the delivery that you settle.
/// </param>
public readonly record struct EventEnvelope(
    string MessageId,
    DateTimeOffset EnqueuedAt,
    DateTimeOffset EligibleAt,
    int Attempt,
    ReadOnlyMemory<byte> Payload,
    DeliveryTag Tag)
{
    /// <summary>
    /// The row that this delivery came from. Use it for diagnostics and to find the row with an external tool.
    /// </summary>
    /// <remarks>
    /// Sequences from different sources cannot be compared and have no order between them. Write this value in
    /// a log line. Do not use it in program logic.
    /// </remarks>
    public long Sequence => Tag.Id;
}

/// <summary>
/// Identifies one delivery to the subscription that produced it. The tag is opaque by design.
/// </summary>
/// <remarks>
/// <para>
/// The tag names a row in a specific table. Only the subscription uses that information. Keep the tag and give
/// it back. Do not read it. A handler that branches on the source of a message will behave differently after a
/// move to a network broker.
/// </para>
/// <para>
/// A tag identifies a delivery. A failed message is delivered again, so one message can have more than one
/// delivery. Thus the tag includes the attempt. If the tag named only the row, both deliveries would have the
/// same tag. A stale settle of the first delivery would then succeed. It would settle the second delivery while
/// a handler works on it. The settle from that handler would then fail for an error that the handler did not
/// make. The Service Bus lock token makes the same distinction. The attempt is unique for each delivery during
/// the lifetime of a store, because each claim of a row increments it. Thus two deliveries always have
/// different tags.
/// </para>
/// </remarks>
public readonly record struct DeliveryTag
{
    internal DeliveryTag(EntrySource source, long id, int attempt)
    {
        Source = source;
        Id = id;
        Attempt = attempt;
    }

    internal EntrySource Source { get; }

    internal long Id { get; }

    internal int Attempt { get; }
}

/// <summary>The table that a delivery came from. This type is internal plumbing by design.</summary>
internal enum EntrySource
{
    Log = 0,
    Scheduled = 1,
    SetAside = 2,
}
