namespace Peers.Shared.Models;

public sealed class PeerEvent
{
    public required long EventId { get; init; }
    public required string Type { get; init; }
    public required string Payload { get; init; } // JSON
    public required DateTime CreatedAt { get; init; }
}

public static class EventTypes
{
    public const string PeerRegistered = "peer.registered";
    public const string PeerUpdated = "peer.updated";
    public const string PeerExpired = "peer.expired";
    public const string ChannelCreated = "channel.created";
    public const string ChannelMessageAdded = "channel.message_added";
    public const string ArtifactPublished = "artifact.published";
}
