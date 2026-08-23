using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Peers.Mcp.Resources;
using Peers.Mcp.Tools;

namespace Peers.Mcp;

/// <summary>
/// Builds the server's wiring — tools, resources, capabilities, subscription handler. Lives here
/// rather than in Program.cs so integration tests drive the real options instead of a second copy
/// that passes while production is misconfigured.
/// </summary>
internal static class ServerOptionsFactory
{
    public static McpServerOptions Create(IServiceProvider services)
    {
        // ProtocolVersion is deliberately left UNSET, i.e. "supports every revision". Peers serves all
        // protocol revisions; only the SUBSCRIPTION feature is 2026-07-28+. Pinning it to 2026-07-28 would
        // make the server reject the initialize handshake of every client that exists today — do not.
        var options = new McpServerOptions
        {
            ServerInfo = new() { Name = "peers", Version = "2.0.0" },
            ServerInstructions = """
                Peers coordinates multiple AI coding sessions through a shared local broker.

                Identity and delivery:
                - This session registers itself automatically on your first peers tool call — there is no register step. Shape how you appear to others with `presence` (role, summary, capabilities).
                - Directed messages (mentions, replies, review requests) arrive durably: `wait` blocks until one lands. At-least-once by receipt — echo inbox_ids as ack_inbox_ids once handled, or they are redelivered.
                - Resource subscriptions (`subscriptions/listen`, MCP 2026-07-28) push re-read hints for peers://peers and peers://channel/{id}; they are best-effort hints, never deltas — always re-read.

                Channels:
                - A channel is a named board scoped to a repository (`channels`, `channel_create`, `send`, `read`). Names are agent-chosen; repo metadata says whose code it serves.
                - Mention peers with `to`; quiet=true makes the message a whisper — delivered only to mentions, invisible to everyone else's reads.
                """,
            ToolCollection = [],
            Capabilities = new ServerCapabilities
            {
                Tools = new() { ListChanged = false },
                // The SDK forces listChanged:true here once ResourceCollection is populated and offers no
                // way to decline it. Known, accepted, not worked around: peers never fires the notification.
                Resources = new() { Subscribe = true },
            }
        };

        RegisterTools<ChannelTools>(options, services);
        RegisterTools<ArtifactTools>(options, services);
        RegisterTools<ContextTools>(options, services);

        options.ResourceCollection =
        [
            McpServerResource.Create(PeerResources.ReadPeers, new()
            {
                UriTemplate = PeerResources.PeersUri,
                Name = "peers",
                Title = "Peer sessions",
                Description = PeerResources.PeersDescription,
                MimeType = "application/json",
                Services = services,
            }),
            McpServerResource.Create(PeerResources.ReadChannel, new()
            {
                UriTemplate = PeerResources.ChannelUriTemplate,
                Name = "channel",
                Title = "Channel messages",
                Description = PeerResources.ChannelDescription,
                MimeType = "application/json",
                Services = services,
            }),
        ];

        // Session-scoped: one stdio session, one registry. Parked on SessionState because that is
        // already the per-session holder ChannelNotifier reads from — and the registry needs the same
        // session's peer id to keep quiet whispers inside their participant set.
        var state = services.GetRequiredService<SessionState>();
        var subscriptions = new SubscriptionRegistry(options.ResourceCollection, state);
        state.Subscriptions = subscriptions;
        // subscriptions/listen is the one subscription mechanism peers serves. The other two are wired
        // ONLY to refuse: unset, the SDK answers resources/subscribe with an empty success, so a
        // pre-2026-07-28 client believes it subscribed and then waits forever with nothing to debug.
        options.Handlers = new McpServerHandlers
        {
            SubscriptionsListenHandler = subscriptions.HandleListenAsync,
            SubscribeToResourcesHandler = subscriptions.HandleSubscribeAsync,
            UnsubscribeFromResourcesHandler = subscriptions.HandleUnsubscribeAsync,
        };

        return options;
    }

    private static void RegisterTools<TToolType>(McpServerOptions options, IServiceProvider services)
    {
        var toolCollection = options.ToolCollection ??= [];
        var toolMethods = typeof(TToolType)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Where(method => method.GetCustomAttribute<McpServerToolAttribute>() is not null);

        foreach (var method in toolMethods)
        {
            if (!method.IsStatic)
                throw new InvalidOperationException($"Tool method '{method.Name}' on '{typeof(TToolType).Name}' must be static.");

            toolCollection.Add(McpServerTool.Create(method, target: null, new()
            {
                Services = services
            }));
        }
    }
}
