using Peers.Mcp.Lifecycle;
using Peers.Mcp.Resources;
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

    /// <summary>
    /// How far this session has consumed the broker's event stream. Three concurrent writers reach it —
    /// the channel notifier's poll loop, peer_poll, and the heartbeat's self-heal re-register — so every
    /// one of them advances it through <see cref="AdvanceCursor"/>.
    /// </summary>
    public long LastCursor
    {
        get { lock (_cursorGate) return _lastCursor; }
    }

    /// <summary>
    /// Rewinds the cursor. Correct for peer_register ONLY: a fresh identity resumes at its own cursor
    /// (0 for a new peer), and no loop is running at that point because peer_register stops them first.
    /// </summary>
    public void ResetCursorForRegistration(long cursor)
    {
        lock (_cursorGate) _lastCursor = cursor;
    }

    /// <summary>
    /// Moves the cursor forward, never backward, as one atomic step. A plain <c>x = Math.Max(x, n)</c>
    /// is a read-modify-write: two writers interleaving inside it can commit the lower value and leave
    /// the session permanently rewound, replaying every event in between as duplicates.
    /// </summary>
    // ponytail: one lock for one long, taken at most every 5s. Interlocked.CompareExchange would save
    // nothing measurable here and reads worse.
    public void AdvanceCursor(long cursor)
    {
        lock (_cursorGate)
        {
            if (cursor > _lastCursor)
                _lastCursor = cursor;
        }
    }

    private readonly Lock _cursorGate = new();
    private long _lastCursor;

    /// <summary>
    /// Broker head at registration. Resource dispatch starts strictly after this so a subscription only
    /// ever reports changes that happened after the session existed; the push path is unaffected and keeps
    /// resuming from <see cref="LastCursor"/>.
    /// </summary>
    public long ResourceBaseline { get; set; }
    public HeartbeatService? Heartbeat { get; set; }
    public ChannelNotifier? ChannelNotifier { get; set; }

    /// <summary>
    /// Open subscriptions/listen streams. Set once when the server options are built and deliberately
    /// NOT cleared by StopServicesAsync: subscriptions belong to the MCP connection, not to a peer
    /// registration, so re-registering as a different peer must not silently drop them.
    /// </summary>
    public SubscriptionRegistry? Subscriptions { get; set; }

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

    /// <summary>
    /// Stops this session's loops. Open subscriptions are deliberately left alone here too: by the time
    /// anything disposes the session the stdio transport is already closed, so a listen stream ends the
    /// way SEP-2575 calls an abrupt transport drop — no notification, no response, nothing owed. See
    /// SubscriptionRegistry.HandleListenAsync.
    /// </summary>
    public ValueTask DisposeAsync() => StopServicesAsync();

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
