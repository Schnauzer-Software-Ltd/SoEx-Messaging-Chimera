namespace SoEx.Messaging.Chimera;

/// <summary>Why a message was set aside. Deferred expects to come back; dead does not.</summary>
public enum SetAsideKind
{
    /// <summary>Set aside to come back later, when the subscriber replays it.</summary>
    Deferred = 0,

    /// <summary>Given up on. Kept for inspection, replay or discard.</summary>
    Dead = 1,
}

/// <summary>
/// A message a subscriber has taken out of the normal flow. Self-contained: it carries its own payload rather
/// than pointing at the log, so log retention cannot strand it and replaying it does not depend on the original
/// record still existing.
/// </summary>
/// <param name="AsideId">Identifies the entry in this subscriber's store. Pass it to replay or discard the message.</param>
/// <param name="Kind">Whether the message is deferred or dead.</param>
/// <param name="Topic">The topic the message was read from.</param>
/// <param name="Origin">Provenance only — "log:1234", "scheduled:57". Nothing dereferences it.</param>
/// <param name="MessageId">The message's identifier, the same as on every delivery of it.</param>
/// <param name="EnqueuedAt">When the message was originally published.</param>
/// <param name="AsideAt">When the message was set aside.</param>
/// <param name="Attempts">How many times the message had been delivered when it was set aside.</param>
/// <param name="Reason">The reason the handler gave when it set the message aside, if it gave one.</param>
/// <param name="Payload">The message body, exactly as it was published.</param>
public sealed record SetAsideEntry(
    long AsideId,
    SetAsideKind Kind,
    string Topic,
    string Origin,
    string MessageId,
    DateTimeOffset EnqueuedAt,
    DateTimeOffset AsideAt,
    int Attempts,
    string? Reason,
    ReadOnlyMemory<byte> Payload);
