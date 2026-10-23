namespace Peers.Shared.Contracts;

public sealed class CreateChannelRequest
{
    public required string Name { get; init; }
    public string? RepoRoot { get; init; }
    public required string CreatedByPeerId { get; init; }
}

public sealed class CreateChannelResponse
{
    public required string ChannelId { get; init; }
    public required string Name { get; init; }
    public string? RepoRoot { get; init; }
}

public sealed class ListChannelsRequest
{
    public string? RepoRoot { get; init; }
    public int? Limit { get; init; }
}

public sealed class ListChannelsResponse
{
    public required ChannelSummary[] Channels { get; init; }
}

public sealed class ChannelSummary
{
    public required string ChannelId { get; init; }
    public required string Name { get; init; }
    public string? RepoRoot { get; init; }
    public required string CreatedByPeerId { get; init; }
    public required string CreatedAt { get; init; }
    public long MessageCount { get; init; }
}

public sealed class SendMessageRequest
{
    public required string AuthorPeerId { get; init; }

    /// <summary>Explicit mentions: peer IDs (prefix match) or repo paths. Mentions are what makes a quiet message visible.</summary>
    public string[]? To { get; init; }
    public bool Quiet { get; init; }

    /// <summary>
    /// Caller-chosen key making the send retry-safe: a second send with the same key in the same
    /// channel returns the first message instead of posting again. Unique per channel; omit for
    /// messages where a duplicate is harmless.
    /// </summary>
    public string? IdempotencyKey { get; init; }
    public required string Content { get; init; }
}

public sealed class SendMessageResponse
{
    public required string MessageId { get; init; }
    public required string ChannelId { get; init; }
    public long Seq { get; init; }
    public required string[] Mentions { get; init; }
    public RecipientDeliveryStatus? Delivery { get; init; }

    /// <summary>
    /// True when an idempotency key matched an earlier send: this response describes the message that
    /// already exists and nothing was written. Delivery is still current — the retry's real question.
    /// </summary>
    public bool Deduplicated { get; init; }
}

public sealed class ReadMessagesResponse
{
    public required string ChannelId { get; init; }
    public required MessageDto[] Messages { get; init; }
    public bool Truncated { get; init; }
}

public sealed class RemoveMessagesRequest
{
    public required string[] MessageIds { get; init; }
}

public sealed class RemoveMessagesResponse
{
    public required string ChannelId { get; init; }
    public int DeletedMessages { get; init; }
}

public sealed class MessageDto
{
    public required string MessageId { get; init; }
    public required string ChannelId { get; init; }
    public long Seq { get; init; }
    public required string AuthorPeerId { get; init; }
    public required string Content { get; init; }
    public required string[] Mentions { get; init; }
    public bool Quiet { get; init; }
    public required string CreatedAt { get; init; }
}

/// <summary>
/// Delivery status of one message's mentions: who was online to be notified, whose session is gone,
/// and which targets matched nobody. Same shape the scratchpad flow reported before channels.
/// </summary>
public sealed class RecipientDeliveryStatus
{
    public required string[] OnlinePeerIds { get; init; }
    public required string[] OfflinePeerIds { get; init; }
    public required string[] UnresolvedTargets { get; init; }
}
