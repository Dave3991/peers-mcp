using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;
using Peers.Mcp.Resources;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Mcp;

public sealed class SessionState : IDisposable, IAsyncDisposable
{

    public string? PeerId { get; set; }
    public string? Tool { get; set; }

    /// <summary>Derived at registration: the git workstream this session runs in.</summary>
    public string? RepoRoot { get; set; }

    /// <summary>The MCP connection this session serves; needed to construct the notifier and read client identity.</summary>
    public McpServer? Server { get; set; }

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
    /// Last roster the channel notifier fetched, or an empty list before the first refresh. Read by the
    /// wait ticker so a keepalive can say what other peers are doing WITHOUT doing I/O of its own: the
    /// notifier is already polling every 5s and already sees the peer events that make this stale, so a
    /// second fetch on the ticker's path would duplicate that loop inside a cancellation-sensitive
    /// request. Always replaced wholesale, never mutated, so a reader needs no lock beyond the swap.
    /// </summary>
    public IReadOnlyList<Peer> PeerRoster
    {
        get { lock (_rosterGate) return _peerRoster; }
        set { lock (_rosterGate) _peerRoster = value; }
    }

    private readonly Lock _rosterGate = new();
    private IReadOnlyList<Peer> _peerRoster = [];

    /// <summary>
    /// Broker head at registration. Resource dispatch starts strictly after this so a subscription only
    /// ever reports changes that happened after the session existed; the push path is unaffected and keeps
    /// resuming from <see cref="LastCursor"/>.
    /// </summary>
    public long ResourceBaseline { get; set; }
    public HeartbeatService? Heartbeat { get; set; }
    public ChannelNotifier? ChannelNotifier { get; set; }

    /// <summary>
    /// The session's resource-subscription state (which URIs the client subscribed to via
    /// resources/subscribe). Set once when the server options are built and deliberately NOT cleared by
    /// StopServicesAsync: subscriptions belong to the MCP connection, not to a peer registration, so
    /// re-registering as a different peer must not silently drop them.
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
    /// Stops this session's loops. Subscriptions are deliberately left alone here too: by the time
    /// anything disposes the session the stdio transport is already closed, so nothing is owed on it —
    /// no notification, no response. See SubscriptionRegistry.
    /// </summary>
    public ValueTask DisposeAsync() => StopServicesAsync();

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
