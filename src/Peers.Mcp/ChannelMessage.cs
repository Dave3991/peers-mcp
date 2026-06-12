using ModelContextProtocol.Protocol;

namespace Peers.Mcp;

public sealed record PendingChannelMessage
{
    public required string MessageId { get; init; }
    public required long EventId { get; init; }
    public required string EventType { get; init; }
    public required DateTime CreatedAt { get; init; }
    public required string Content { get; init; }
    public required LoggingLevel Level { get; init; }
    public string? ScratchpadId { get; init; }
    public string? Title { get; init; }
    public string? FromPeerId { get; init; }
};
