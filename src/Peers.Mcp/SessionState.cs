using Peers.Mcp.Lifecycle;
using Peers.Shared.Contracts;

namespace Peers.Mcp;

public sealed class SessionState : IDisposable, IAsyncDisposable
{
    public SessionState()
    {
        ChatId = $"peers_{Environment.ProcessId}_{Guid.NewGuid():N}";
    }

    public string ChatId { get; }
    public string? PeerId { get; set; }
    public string? Tool { get; set; }
    public long LastCursor { get; set; }
    public HeartbeatService? Heartbeat { get; set; }
    public ChannelNotifier? ChannelNotifier { get; set; }
    public string[]? PendingInboxAckIds { get; set; }
    public RegisterPeerRequest? LastRegisterRequest { get; set; }

    public async ValueTask StopServicesAsync()
    {
        var channelNotifier = ChannelNotifier;
        ChannelNotifier = null;

        var heartbeat = Heartbeat;
        Heartbeat = null;

        if (channelNotifier is not null)
            await channelNotifier.DisposeAsync();
        if (heartbeat is not null)
            await heartbeat.DisposeAsync();
    }

    public ValueTask DisposeAsync() => StopServicesAsync();

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
