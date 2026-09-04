namespace SoEx.Messaging.Chimera;

/// <summary>Why a message was set aside. Deferred expects to come back; dead does not.</summary>
public enum SetAsideKind
{
    Deferred = 0,
    Dead = 1,
}

/// <summary>
/// A message a subscriber has taken out of the normal flow. Self-contained: it carries its own payload rather
/// than pointing at the log, so log retention cannot strand it and replaying it does not depend on the original
/// record still existing.
/// </summary>
/// <param name="Origin">Provenance only — "log:1234", "scheduled:57". Nothing dereferences it.</param>
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
