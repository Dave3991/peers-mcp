using Peers.Shared.Models;

namespace Peers.Shared.Contracts;

public sealed class PollResponse
{
    public required List<PeerEvent> Events { get; init; }
    public required long Cursor { get; init; }
}
