using System.Reflection;
using System.Text.Json;
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
            ServerInfo = new() { Name = "peers", Version = "1.0.0" },
            ServerInstructions = """
                Peers coordinates multiple AI coding sessions through a shared local broker.

                Message delivery modes:
                - Push mode: best-effort. Pre-2026-07-28 sessions get `notifications/message`; from 2026-07-28 (SEP-2577) nothing is pushed — use subscriptions or pull-mode `peer_inbox`. Claude Code sessions started with `--dangerously-load-development-channels server:peers` get direct channel pushes on any revision. `peer_test_push` reports which applies here.
                - Pull mode: always available via `peer_inbox`, which returns directed messages and keeps returning them until you acknowledge them by echoing their `inbox_id`s as `ack_inbox_ids`.

                Usage guidance:
                - Call `peer_register` at startup.
                - Call `peer_inbox` at startup, after completing a task chunk, when the user asks about peer activity, and before finishing.
                - Use `peer_poll` only when you need raw broker events or cursor-based debugging.
                - Use scratchpads for replies and longer async collaboration threads.

                Resources (read instead of polling a tool):
                - `peers://peers` — same payload as `peer_list`.
                - `peers://scratchpad/{id}` — same payload as `scratchpad_read`.
                - Both are inert until `peer_register`, and readable on any protocol revision.
                - Subscribing needs MCP 2026-07-28 (`subscriptions/listen`); the older `resources/subscribe` is
                  refused with an error. A `notifications/resources/updated` is A HINT TO RE-READ, never a delta
                  stream: delivery is best-effort and lossy, and several changes can collapse into one
                  notification or into none. Always re-read; never reconstruct state from the notifications.
                """,
            ToolCollection = [],
            Capabilities = new ServerCapabilities
            {
                Tools = new() { ListChanged = false },
                // The SDK forces listChanged:true here once ResourceCollection is populated and offers no
                // way to decline it. Known, accepted, not worked around: peers never fires the notification.
                Resources = new() { Subscribe = true },
#pragma warning disable MCP9005 // Logging capability deprecated by SEP-2577; still required by pre-2026-07-28 clients.
                Logging = new(),
#pragma warning restore MCP9005
                Experimental = new Dictionary<string, object>
                {
                    ["claude/channel"] = JsonDocument.Parse("{}").RootElement.Clone()
                }
            }
        };

        RegisterTools<PeerTools>(options, services);
        RegisterTools<ScratchpadTools>(options, services);
        RegisterTools<ArtifactTools>(options, services);
        RegisterTools<PollTools>(options, services);
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
            McpServerResource.Create(PeerResources.ReadScratchpad, new()
            {
                UriTemplate = PeerResources.ScratchpadUriTemplate,
                Name = "scratchpad",
                Title = "Scratchpad thread",
                Description = PeerResources.ScratchpadDescription,
                MimeType = "application/json",
                Services = services,
            }),
        ];

        // Session-scoped: one stdio session, one registry. Parked on SessionState because that is
        // already the per-session holder ChannelNotifier reads from.
        var subscriptions = new SubscriptionRegistry(options.ResourceCollection);
        services.GetRequiredService<SessionState>().Subscriptions = subscriptions;
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
