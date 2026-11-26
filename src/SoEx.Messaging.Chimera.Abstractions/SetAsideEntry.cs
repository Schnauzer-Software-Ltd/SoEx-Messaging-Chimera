namespace SoEx.Messaging.Chimera;

/// <summary>The reason a message was set aside. A deferred message is expected to come back. A dead letter is not.</summary>
public enum SetAsideKind
{
    /// <summary>Set aside to come back later, when the subscriber replays it.</summary>
    Deferred = 0,

    /// <summary>Abandoned by the subscriber. The message stays for inspection, replay or discard.</summary>
    Dead = 1,
}

/// <summary>
/// A message that a subscriber took out of the normal flow.
/// </summary>
/// <remarks>
/// The entry is self-contained. It holds a copy of the payload. Thus log retention cannot strand it, and a
/// replay needs no original record.
/// </remarks>
/// <param name="AsideId">Identifies the entry in this subscriber's store. Pass it to replay or discard the message.</param>
/// <param name="Kind">Whether the message is deferred or dead.</param>
/// <param name="Topic">The topic that the message was read from.</param>
/// <param name="Origin">The source of the message, for information only, for example "log:1234" or "scheduled:57". No code reads the source from it.</param>
/// <param name="MessageId">The identifier of the message. It is the same on each delivery of the message.</param>
/// <param name="EnqueuedAt">The time of the original publish.</param>
/// <param name="AsideAt">The time when the message was set aside.</param>
/// <param name="Attempts">The number of deliveries of the message before it was set aside.</param>
/// <param name="Reason">The reason that the handler gave when it set the message aside, if it gave one.</param>
/// <param name="Payload">The message body as it was published.</param>
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
