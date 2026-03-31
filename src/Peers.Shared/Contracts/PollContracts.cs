using Peers.Shared.Models;

namespace Peers.Shared.Contracts;

public sealed class PollResponse
{
    public required List<PeerEvent> Events { get; init; }
    public required long Cursor { get; init; }
}

public sealed class InboxResponse
{
    public required List<InboxItemDto> Items { get; init; }
    public required int Count { get; init; }
}

public sealed class InboxItemDto
{
    public required string InboxId { get; init; }
    public required string PeerId { get; init; }
    public required PeerEvent Event { get; init; }
    public required DateTime CreatedAt { get; init; }
}
