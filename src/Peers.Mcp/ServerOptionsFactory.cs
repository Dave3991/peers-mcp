using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
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

    public static McpServerOptions Create(IServiceProvider services)
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
            ServerInstructions = """
                Peers coordinates multiple AI coding sessions through a shared local broker.

                Identity and delivery:
                - This session registers itself automatically on your first peers tool call — there is no register step. Shape how you appear to others with `presence` (role, summary, capabilities).
                - Directed messages (mentions, replies, review requests) arrive durably: `wait` blocks until one lands. At-least-once by receipt — echo inbox_ids as ack_inbox_ids once handled, or they are redelivered.
                - Resource subscriptions (`resources/subscribe`) push re-read hints for peers://peers and peers://inbox; they are best-effort hints, never deltas — always re-read. omp injects them into the conversation when `mcp.notifications` is on.

                Channels:
                - A channel is a named board scoped to a repository (`channels`, `channel_create`, `send`, `read`). Names are agent-chosen; repo metadata says whose code it serves.
                - Mention peers with `to`; quiet=true makes the message a whisper — delivered only to mentions, invisible to everyone else's reads.
                - Pass idempotency_key on `send` when a retry must not double-post; the second call returns the first message with deduplicated=true.

                Managed review:
                - Use `review_packet` for immutable PLAN/CODE requests, complete evidence fetches, and independent reviewer replies. Approve PLAN before CODE; silence and raw chat never authorize completion.
                - Use `review_checkpoint` to restore history, obtain the revision before native checks, sync handled receipts, explicitly adopt an offline driver, and complete the exact approved CODE packet. A stale latch requires a new CODE round; completed work stays historical after a commit.
                - Managed sync acknowledges only this peer's prior durable handled IDs. ready_to_ack holds only the requests, verdicts, and controls the review ledger accepted; every other message, including a record the workflow rejected (such as a late verdict for an older round), plain chat, or a question in the review channel, is in unrelated_receipts. Read unrelated_receipts with ordinary tools (`read`, `wait`) and acknowledge them through `wait` before parking.
                - `review_lesson` freezes one explicitly requested supported lesson after intact completion. Use the existing mounted Aletheia tools through the promotion helper; no automatic learning or exactly-once storage guarantee.

                Paths:
                - `claim` before editing when another session is live in this repo: it grants your paths and names the peer already holding any that overlap (a directory covers its contents). `claims` is the whole map, `release` gives yours back.
                - A claim informs, it never blocks — a conflict means coordinate with the holder, not stop. Claims lapse with their TTL or with this session's lease, so a crashed peer never holds a path hostage.
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
        RegisterTools<ClaimTools>(options, services);
        RegisterTools<ArtifactTools>(options, services);
        RegisterTools<ContextTools>(options, services);
        RegisterTools<ReviewTools>(options, services, nameBindingErrors: true);

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

    private static void RegisterTools<TToolType>(McpServerOptions options, IServiceProvider services, bool nameBindingErrors = false)
    {
        var toolCollection = options.ToolCollection ??= [];
        var toolMethods = typeof(TToolType)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Where(method => method.GetCustomAttribute<McpServerToolAttribute>() is not null);

        foreach (var method in toolMethods)
        {
            if (!method.IsStatic)
                throw new InvalidOperationException($"Tool method '{method.Name}' on '{typeof(TToolType).Name}' must be static.");

            var tool = McpServerTool.Create(method, target: null, new()
            {
                Services = services
            });
            toolCollection.Add(nameBindingErrors ? new BindingErrorNamingTool(tool, method) : tool);
        }
    }

    /// <summary>
    /// An argument whose JSON cannot bind to the parameter type (a string where a boolean belongs) fails
    /// inside the SDK before the tool runs, and the SDK answers any non-McpException with an opaque
    /// "An error occurred invoking". The review tools translate their own JSON faults inside their
    /// bodies, so a JsonException that escapes one of them is a binding failure: rethrow it as a tool
    /// error that names the field, the value it got, and the JSON type it needs.
    /// </summary>
    private sealed class BindingErrorNamingTool(McpServerTool inner, MethodInfo method) : DelegatingMcpServerTool(inner)
    {
        public override async ValueTask<CallToolResult> InvokeAsync(RequestContext<CallToolRequestParams> request, CancellationToken cancellationToken = default)
        {
            try
            {
                return await base.InvokeAsync(request, cancellationToken);
            }
            catch (Exception ex) when (ex is JsonException || ex.InnerException is JsonException)
            {
                throw new McpException(Describe(request.Params?.Arguments) + " The tool's input schema lists each field's type and allowed values.", ex);
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
                    var path = json.Path ?? "$";
                    var got = At(value, path) is { } bad ? bad.GetRawText() : "a value of the wrong type";
                    if (got.Length > 80) got = got[..80] + "…";
                    return $"{parameter.Name}{path[1..]} is {got}; it must be {Expected(json.Message)}.";
                }
            }
            return $"{ProtocolTool.Name} arguments do not match the tool's input schema.";
        }

        private static JsonElement? At(JsonElement root, string path)
        {
            var current = root;
            foreach (Match step in Regex.Matches(path, @"\.(?<name>[^.\[]+)|\['(?<quoted>[^']*)'\]|\[(?<index>\d+)\]"))
            {
                if (step.Groups["index"].Success)
                {
                    var index = int.Parse(step.Groups["index"].Value, System.Globalization.CultureInfo.InvariantCulture);
                    if (current.ValueKind != JsonValueKind.Array || index >= current.GetArrayLength()) return null;
                    current = current[index];
                }
                else
                {
                    var name = step.Groups["name"].Success ? step.Groups["name"].Value : step.Groups["quoted"].Value;
                    if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(name, out current)) return null;
                }
            }
            return current;
        }

        private static string Expected(string message)
        {
            var target = Regex.Match(message, @"converted to (?<type>\S+?)\.( Path|$)").Groups["type"].Value;
            return target switch
            {
                "System.Boolean" => "true or false",
                "System.String" => "a string",
                _ when target.Contains("System.Int", StringComparison.Ordinal) => "an integer",
                _ when target.EndsWith("[]", StringComparison.Ordinal) => "an array",
                "" => "the type the schema gives",
                _ => "an object",
            };
        }
    }
}
