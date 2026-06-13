namespace Peers.Shared.Contracts;

public sealed class SendMessageRequest
{
    public required string FromPeerId { get; init; }
    public string[]? Targets { get; init; }
    public required string Content { get; init; }
    public bool Quiet { get; init; }
}

public sealed class SendMessageResponse
{
    public RecipientDeliveryStatus? Recipients { get; init; }
}
