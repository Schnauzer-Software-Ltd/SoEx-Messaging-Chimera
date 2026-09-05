namespace SoEx.Messaging.Chimera;

/// <summary>
/// One delivered message. The same shape whatever it was read from — the log, the scheduled table, or a
/// set-aside row that was replayed — because consuming code has no reason to care which.
/// </summary>
/// <param name="MessageId">
/// Opaque, stable and unique across sources. Use this to identify a message; do not use <see cref="Sequence"/>,
/// which comes from three different number spaces and only looks orderable.
/// </param>
/// <param name="EnqueuedAt">
/// When the message was originally published. Stable across deferral and replay, so it gives end-to-end latency.
/// </param>
/// <param name="EligibleAt">
/// When this delivery became available: append time for a logged message, due time for a scheduled one, replay
/// time for a set-aside one. Lag is measured against this.
/// </param>
/// <param name="Attempt">Delivery attempt, starting at 1.</param>
public readonly record struct EventEnvelope(
    string MessageId,
    DateTimeOffset EnqueuedAt,
    DateTimeOffset EligibleAt,
    int Attempt,
    ReadOnlyMemory<byte> Payload,
    DeliveryTag Tag)
{
    /// <summary>
    /// The row this delivery came from, for diagnostics and for finding it with an external tool. Sequences
    /// from different sources are not comparable and carry no ordering guarantee between them, so this is for
    /// reading in a log line, not for reasoning with.
    /// </summary>
    public long Sequence => Tag.Id;
}

/// <summary>
/// Identifies one delivery to the subscription that produced it. Opaque on purpose: it names a row in a
/// particular table, which is the subscription's business and nobody else's. Hold it, hand it back, and do not
/// try to read it — a handler that branches on where a message came from is one that will behave differently
/// after a move to a real broker.
/// </summary>
/// <remarks>
/// A delivery, not a message. The attempt is part of it because a message that fails is handed out again, and
/// a tag naming only the row would be the same tag for both deliveries: a stale settle of the first one would
/// then be accepted and would settle the second out from under the handler working on it, whose own settle was
/// refused for a mistake it had not made. Service Bus draws the same line with its lock token. The attempt is
/// unique per delivery within a store's lifetime — claiming a row is what increments it — so the two are
/// distinguishable wherever they are compared.
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

/// <summary>Which table a delivery came from. Plumbing, deliberately not part of the public surface.</summary>
internal enum EntrySource
{
    Log = 0,
    Scheduled = 1,
    SetAside = 2,
}
