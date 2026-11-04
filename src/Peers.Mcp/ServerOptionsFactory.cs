using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
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
    /// <summary>
    /// What this build actually is: <c>Version</c> plus the HEAD sha the csproj stamps into
    /// InformationalVersion. Reported as the server version so a client can tell a freshly spawned
    /// session from one still serving a pre-change assembly — the failure mode that had five servers
    /// advertising deleted tools in August 2026.
    /// </summary>
    private static readonly string BuildVersion =
        typeof(ServerOptionsFactory).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
        ?? typeof(ServerOptionsFactory).Assembly.GetName().Version?.ToString()
        ?? "0.0.0";

    /// <summary>
    /// The environment variable that selects the tool profile, read once at startup. Same naming and
    /// semantics as Aletheia's ALETHEIA_MCP_TOOL_PROFILE: "all" (any case) registers every tool; unset,
    /// blank, or any other value registers the core profile.
    /// </summary>
    public const string ToolProfileVariable = "PEERS_MCP_TOOL_PROFILE";

    /// <summary>
    /// Tools that only the "all" profile registers. They are rare or destructive, and every listed tool
    /// costs context in each request of each session. Operators also have Peers.Cli channel-clear and
    /// messages-remove.
    /// </summary>
    public static readonly string[] OptInTools = ["channel_clear", "messages_remove", "context_export", "context_import", "review_lesson"];

    /// <summary>True when <paramref name="profile"/> (the value of <see cref="ToolProfileVariable"/>) selects every tool.</summary>
    public static bool AllToolsRequested(string? profile) =>
        string.Equals(profile?.Trim(), "all", StringComparison.OrdinalIgnoreCase);

    // Sent with every initialize; omp puts it in the system prompt of every session, so each line costs context.
    private const string Instructions = """
        Peers coordinates multiple AI coding sessions through a shared local broker.

        Identity and delivery:
        - This session registers itself automatically on your first peers tool call — there is no register step. Shape how you appear to others with `presence` (role, summary, capabilities).
        - Directed messages (mentions, replies, review requests) arrive durably: `wait` blocks until one lands (call it without timeout_seconds; the 120s default returns as soon as a message lands). At-least-once by receipt — echo inbox_ids as ack_inbox_ids once handled, or they are redelivered.
        - Resource subscriptions (`resources/subscribe`) push re-read hints for peers://inbox (directed messages) and peers://channel/{id}; they are best-effort hints, never deltas — always re-read. peers://peers never notifies: read it or call `peers`. omp injects hints into the conversation when `mcp.notifications` is on.

        Channels:
        - A channel is a named board scoped to a repository (`channels`, `send`, `read`). Names are agent-chosen; repo metadata says whose code it serves. A name reaches channels in your repo and global ones; reach another repo's channel by `channel_id`, and create cross-repo notice channels with `channels` action create, global: true.
        - Mention peers with `to`; quiet=true makes the message a whisper — delivered only to mentions, invisible to everyone else's reads.
        - Pass idempotency_key on `send` when a retry must not double-post; the second call returns the first message with deduplicated=true.

        Managed review:
        - Use `review_packet` for immutable PLAN/CODE requests, complete evidence fetches, and independent reviewer replies. Approve PLAN before CODE; silence and raw chat never authorize completion.
        - Use `review_checkpoint` to restore history, obtain the revision before native checks, sync handled receipts, explicitly adopt an offline driver, and complete the exact approved CODE packet. A stale latch requires a new CODE round; completed work stays historical after a commit.
        - Managed sync acknowledges only this peer's prior durable handled IDs. ready_to_ack holds only the requests, verdicts, and controls the review ledger accepted; every other message, including a record the workflow rejected (such as a late verdict for an older round), plain chat, or a question in the review channel, is in unrelated_receipts. Read unrelated_receipts with ordinary tools (`read`, `wait`) and acknowledge them through `wait` before parking.

        Paths:
        - `claims` action claim before editing when another session is live in this repo: it grants your paths and names the peer already holding any that overlap (a directory covers its contents). An absolute path in another git working tree is claimed in that tree. Action list is this repo's map; action release gives yours back.
        - A claim informs, it never blocks — a conflict means coordinate with the holder, not stop. Claims lapse with their TTL or with this session's lease, so a crashed peer never holds a path hostage.
        """;
    private const string CoreProfileInstructions =
        "\n\nOpt-in tools, listed only when the server starts with PEERS_MCP_TOOL_PROFILE=all: `channel_clear`, `messages_remove`, `context_export`, `context_import`, `review_lesson`.";
    private const string AllProfileInstructions = "\n\n" + """
        Opt-in tools (PEERS_MCP_TOOL_PROFILE=all):
        - `review_lesson` freezes one explicitly requested supported lesson after intact completion. Use the existing mounted Aletheia tools through the promotion helper; no automatic learning or exactly-once storage guarantee.
        """;

    public static McpServerOptions Create(IServiceProvider services, bool allTools)
    {
        // Pinned to 2025-11-25 — the revision every shipping client negotiates. Downgrade audit (recorded
        // to end re-litigation): the only 2026-only server surface peers ever had was subscriptions/listen
        // itself, removed when resources/subscribe became the served mechanism; peer_wait's progress
        // notifications predate that revision and are unaffected. Whenever a change reintroduces a
        // 2026-only server feature, THAT change owns revisiting ProtocolVersion.
        var options = new McpServerOptions
        {
            ProtocolVersion = "2025-11-25",
            ServerInfo = new() { Name = "peers", Version = BuildVersion },
            ServerInstructions = Instructions + (allTools ? AllProfileInstructions : CoreProfileInstructions),
            ToolCollection = [],
            Capabilities = new ServerCapabilities
            {
                Tools = new() { ListChanged = false },
                // The SDK forces listChanged:true here once ResourceCollection is populated and offers no
                // way to decline it. Known, accepted, not worked around: peers never fires the notification.
                Resources = new() { Subscribe = true },
            }
        };

        RegisterTools<ChannelTools>(options, services, allTools);
        RegisterTools<ClaimTools>(options, services, allTools);
        RegisterTools<ArtifactTools>(options, services, allTools);
        RegisterTools<ContextTools>(options, services, allTools);
        RegisterTools<ReviewTools>(options, services, allTools);

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
            McpServerResource.Create(PeerResources.ReadInbox, new()
            {
                UriTemplate = PeerResources.InboxUri,
                Name = "inbox",
                Title = "Unacknowledged inbox",
                Description = PeerResources.InboxDescription,
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
        // resources/subscribe is the one subscription mechanism peers serves. Leaving these handlers
        // unset is not an option: the SDK then answers with an empty success, so a client believes it
        // subscribed and waits forever with nothing to debug.
        options.Handlers = new McpServerHandlers
        {
            SubscribeToResourcesHandler = subscriptions.HandleSubscribeAsync,
            UnsubscribeFromResourcesHandler = subscriptions.HandleUnsubscribeAsync,
        };

        return options;
    }

    private static void RegisterTools<TToolType>(McpServerOptions options, IServiceProvider services, bool allTools)
    {
        var toolCollection = options.ToolCollection ??= [];
        var toolMethods = typeof(TToolType)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Where(method => method.GetCustomAttribute<McpServerToolAttribute>() is { } tool &&
                             (allTools || !OptInTools.Contains(tool.Name)));

        foreach (var method in toolMethods)
        {
            if (!method.IsStatic)
                throw new InvalidOperationException($"Tool method '{method.Name}' on '{typeof(TToolType).Name}' must be static.");

            var tool = McpServerTool.Create(method, target: null, new()
            {
                Services = services,
                SchemaCreateOptions = SchemaOptions,
            });
            toolCollection.Add(new ArgumentErrorNamingTool(tool, method));
        }
    }

    /// <summary>
    /// The SDK publishes every reference-typed and nullable DTO property as <c>"type": [T, "null"]</c>, also
    /// when the schema lists it as required ([Required], or a record constructor parameter). Agents read
    /// that as "required, but null is fine" and sent <c>exit_code: null</c> for an unknown status. The tools
    /// reject null for every required field, so its published type drops the "null" alternative. Binding
    /// is unchanged: exit_code stays int? so a missing value is still reported, never recorded as a success.
    /// </summary>
    private static readonly AIJsonSchemaCreateOptions SchemaOptions = AIJsonSchemaCreateOptions.Default with
    {
        TransformSchemaNode = (_, schema) =>
        {
            if (schema is not JsonObject node || node["properties"] is not JsonObject properties ||
                node["required"] is not JsonArray required) return schema;
            foreach (var name in required.Select(name => name!.GetValue<string>()))
            {
                if (properties[name] is not JsonObject property || property["type"] is not JsonArray types) continue;
                var nonNull = types.Select(type => type!.GetValue<string>()).Where(type => type != "null").ToArray();
                property["type"] = nonNull.Length == 1 ? nonNull[0] : new JsonArray([.. nonNull.Select(type => (JsonNode)type)]);
            }
            return schema;
        },
    };

    /// <summary>
    /// Argument binding fails inside the SDK before the tool runs, and the SDK answers any non-McpException
    /// with an opaque "An error occurred invoking". Two binding failures get a tool error that names the
    /// field instead:
    /// - A required argument that is absent. For a field with allowed values (an action), the error
    ///   lists them, in the same words as <see cref="Reviews.ReviewJson.RequireOneOf"/>.
    /// - An argument whose JSON cannot bind to the parameter type (a string where a boolean belongs). The
    ///   error keeps the serializer's message, which names the type it expected.
    /// A tool's own McpException passes through untouched even when it wraps a JsonException ("Malformed
    /// review state", invalid checkpoint JSON): it already says what went wrong, and it is no argument error.
    /// </summary>
    private sealed class ArgumentErrorNamingTool(McpServerTool inner, MethodInfo method) : DelegatingMcpServerTool(inner)
    {
        public override async ValueTask<CallToolResult> InvokeAsync(RequestContext<CallToolRequestParams> request, CancellationToken cancellationToken = default)
        {
            RequirePresent(request.Params?.Arguments);
            try
            {
                return await base.InvokeAsync(request, cancellationToken);
            }
            catch (Exception ex) when (ex is not McpException && (ex is JsonException || ex.InnerException is JsonException))
            {
                throw new McpException(Describe(request.Params?.Arguments) + " The tool's input schema lists each field's type and allowed values.", ex);
            }
        }

        private void RequirePresent(IDictionary<string, JsonElement>? arguments)
        {
            var schema = ProtocolTool.InputSchema;
            if (!schema.TryGetProperty("required", out var required)) return;
            foreach (var field in required.EnumerateArray())
            {
                var name = field.GetString()!;
                if (arguments is not null && arguments.ContainsKey(name)) continue;
                throw new McpException(schema.GetProperty("properties").GetProperty(name).TryGetProperty("enum", out var allowed)
                    ? $"{name} is missing; allowed values: {string.Join(", ", allowed.EnumerateArray().Select(value => value.GetString()))}."
                    : $"{name} is missing; {ProtocolTool.Name} requires it.");
            }
        }

        /// <summary>Re-binds each argument alone to find the one that failed; the serializer's path is relative to it.</summary>
        private string Describe(IDictionary<string, JsonElement>? arguments)
        {
            foreach (var parameter in method.GetParameters())
            {
                if (parameter.Name is null || arguments is null || !arguments.TryGetValue(parameter.Name, out var value)) continue;
                try { JsonSerializer.Deserialize(value, parameter.ParameterType, McpJsonUtilities.DefaultOptions); }
                catch (JsonException json)
                {
                    return $"{parameter.Name}{(json.Path ?? "$")[1..]} has the wrong JSON type: {json.Message}";
                }
            }
            return $"{ProtocolTool.Name} arguments do not match the tool's input schema.";
        }
    }
}
