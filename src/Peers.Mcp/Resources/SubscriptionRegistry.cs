using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;
using Peers.Shared.Models;

namespace Peers.Mcp.Resources;

/// <summary>
/// Owns the SEP-2575 <c>subscriptions/listen</c> stream: which URIs each open listen request was
/// granted, and the <c>notifications/resources/updated</c> fan-out to them. That is the ONLY resource
/// subscription mechanism peers serves; the pre-2026-07-28 <c>resources/subscribe</c> pair is refused,
/// not implemented (see <see cref="HandleSubscribeAsync"/>).
///
/// Why this exists instead of the SDK's built-in handler plus a plain SendNotificationAsync: the plain
/// notification API is not subscription-aware at delivery time. It fans a resources/updated out to the
/// session regardless of what the client subscribed to, and without the
/// <c>_meta io.modelcontextprotocol/subscriptionId</c> tag stdio clients need to demultiplex —
/// two spec violations. Overriding SubscriptionsListenHandler is a FULL replacement: the SDK then
/// neither tracks the subscription, nor sends the acknowledgement, nor does any */list_changed
/// fan-out for it. All three become ours. Peers fires neither tools/list_changed nor
/// resources/list_changed — both lists are static for the life of the process — so nothing is lost
/// by not implementing them; a future dynamic list would have to fan out from here.
///
/// Quiet whispers are filtered per session at fan-out time: a quiet message notifies only its own
/// participants' streams, because this registry always runs inside exactly one session's process and
/// knows that session's peer id.
///
/// ponytail: stdio-only, deliberately. ActiveSubscription.RelatedTransport is null on stdio, so every
/// notification routes over the one shared session channel and a subscription only needs to remember its
/// McpServer. Streamable HTTP would need per-request transport routing — peers does not have that
/// transport, so this does not build for it.
/// </summary>
public sealed class SubscriptionRegistry
{
    private readonly McpServerResourceCollection _resources;
    private readonly SessionState _state;
    private readonly Lock _gate = new();
    private readonly List<Subscription> _subscriptions = [];

    public SubscriptionRegistry(McpServerResourceCollection resources, SessionState state)
    {
        _resources = resources;
        _state = state;
    }

    /// <summary>
    /// Open listen requests. Kept because deregistration is a SERVER-side fact the wire cannot report:
    /// the SDK client faults a cancelled request locally the instant it sends notifications/cancelled, so
    /// a test that waited on the client would proceed before the server had processed anything. Two tests
    /// need that ordering to mean something — the mid-fan-out cancellation race and the transport-close
    /// teardown — and neither can be written behaviourally without it.
    /// </summary>
    internal int CountForTests { get { lock (_gate) return _subscriptions.Count; } }

    // A class, not a record: this is an identity in a list, not a value. Record equality would compare
    // the mutable flag below, which has no business deciding what Remove takes out.
    private sealed class Subscription(McpServer server, RequestId id, HashSet<string> uris)
    {
        public McpServer Server { get; } = server;
        public RequestId Id { get; } = id;
        public HashSet<string> Uris { get; } = uris;

        private bool _active = true;

        /// <summary>
        /// Re-checked by the fan-out immediately before every send, and cleared by deregistration BEFORE
        /// the list removal — so a subscriber cancelled after the fan-out snapshotted it is still skipped.
        /// Without this the client can receive a resources/updated tagged with a subscription id it just
        /// cancelled, AFTER the server processed the cancellation: exactly the ordering SEP-2575's
        /// subscription id exists to keep straight.
        /// </summary>
        public bool IsActive => Volatile.Read(ref _active);

        public void Deactivate() => Volatile.Write(ref _active, false);
    }

    /// <summary>
    /// Handles <c>subscriptions/listen</c>: acknowledge the granted subset, register, then park until the
    /// stream ends.
    ///
    /// A stdio session ends a subscription exactly two ways and BOTH are obligation-free, so peers has no
    /// server-initiated teardown at all. The client cancels the request (inbound notifications/cancelled:
    /// the server MUST NOT echo one back, and SDK 2.2.0 also suppresses the response frame), or the
    /// transport closes — which the subscriptions page lists as its own ending, "an abrupt transport drop,
    /// which carries no response". stdio process exit IS that second ending, and by the time anything in
    /// this process can act on it the transport is already gone: what releases this handler is McpHost
    /// disposing the server AFTER transport EOF, so a teardown path could only write to a dead transport
    /// and swallow the failure. Do not "restore" one; the result returned below reaches nobody either.
    /// </summary>
    public async ValueTask<EmptyResult> HandleListenAsync(
        RequestContext<SubscriptionsListenRequestParams> request,
        CancellationToken cancellationToken)
    {
        var requested = request.Params?.Notifications?.ResourceSubscriptions ?? [];
        var granted = new HashSet<string>(requested.Where(Serves), StringComparer.Ordinal);
        var id = request.JsonRpcRequest.Id;

        // Ack first — SEP-2575 requires it before anything else on this subscription. The registration
        // below happens only after it is on the wire, so no resources/updated can overtake it.
        // Built as a JsonNode because SubscriptionsAcknowledgedNotificationParams has no _meta of its own.
        var ack = JsonSerializer.SerializeToNode(
            new SubscriptionsAcknowledgedNotificationParams
            {
                Notifications = new SubscriptionsListenNotifications
                {
                    // null, not false: the spec says an unsupported notification type is OMITTED from the
                    // ack, and tells clients to compare the ack against what they asked for. Present-and-
                    // false is a promise about a type we never fire; absent is the truth. Peers fires
                    // neither list_changed.
                    //
                    // null becomes ABSENT only because McpJsonUtilities.DefaultOptions ignores nulls —
                    // the SDK's setting, not ours, and these properties carry no per-property JsonIgnore
                    // of their own (measured: JsonIgnoreCondition.Never emits `"toolsListChanged": null`).
                    ToolsListChanged = null,
                    PromptsListChanged = null,
                    ResourcesListChanged = null,
                    ResourceSubscriptions = granted.ToList(),
                },
            },
            McpJsonUtilities.DefaultOptions)!.AsObject();
        ack["_meta"] = new JsonObject { [MetaKeys.SubscriptionId] = SubscriptionId(id) };

        await request.Server.SendNotificationAsync(
            NotificationMethods.SubscriptionsAcknowledgedNotification,
            ack,
            McpJsonUtilities.DefaultOptions,
            cancellationToken);

        var subscription = new Subscription(request.Server, id, granted);
        lock (_gate) _subscriptions.Add(subscription);

        try
        {
            // The request token covers both endings: it fires on an inbound notifications/cancelled, and
            // the SDK cancels it when the session ends with the transport. Deregistration is the only
            // thing either ending owes anyone, and it happens in the finally.
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            // Deactivate BEFORE removal, and in this order: the fan-out can already be holding a snapshot
            // that contains this subscription, and the flag is the only thing that stops it sending on a
            // stream the client has just cancelled.
            subscription.Deactivate();
            lock (_gate) _subscriptions.Remove(subscription);
        }

        return new EmptyResult();
    }

    /// <summary>
    /// ponytail: this handler exists to REFUSE, not to support. Peers serves resource subscriptions only
    /// over subscriptions/listen; the pre-2026-07-28 resources/subscribe pair is not implemented.
    ///
    /// Leaving it unset is not the same thing and is not an option: the SDK then answers
    /// resources/subscribe with an empty success, so the client believes it subscribed and waits forever
    /// with nothing to debug.
    public ValueTask<EmptyResult> HandleSubscribeAsync(
        RequestContext<SubscribeRequestParams> request,
        CancellationToken cancellationToken)
    {
        throw Unsupported(RequestMethods.ResourcesSubscribe, request.Server);
    }

    public ValueTask<EmptyResult> HandleUnsubscribeAsync(
        RequestContext<UnsubscribeRequestParams> request,
        CancellationToken cancellationToken)
    {
        throw Unsupported(RequestMethods.ResourcesUnsubscribe, request.Server);
    }

    /// <summary>
    /// True when this server actually serves <paramref name="uri"/>; concrete URIs match templates via the SDK.
    ///
    /// A TEMPLATE string is rejected even though McpServerResource.IsMatch accepts it — and a client can
    /// plausibly send one, since 'peers://channel/{id}' is exactly what resources/templates/list hands
    /// it. Granting it would put a URI in the ack that the fan-out, which only ever offers concrete URIs,
    /// can never match: granted and permanently silent. RFC 6570 reserves braces for expressions, so an
    /// expanded URI never has one.
    /// </summary>
    private bool Serves(string uri)
    {
        if (uri.Contains('{'))
            return false;

        foreach (var resource in _resources)
        {
            if (resource.IsMatch(uri))
                return true;
        }

        return false;
    }

    private static McpProtocolException Unsupported(string method, McpServer server) =>
        new($"The method '{method}' is not served by peers; resource subscriptions are available only via " +
            $"'{RequestMethods.SubscriptionsListen}' (MCP 2026-07-28, SEP-2575), and the negotiated protocol " +
            $"version is '{server.NegotiatedProtocolVersion}'. Reading and listing resources is unaffected: " +
            "read the resource on demand, or poll with the read / peers tools.",
            McpErrorCode.MethodNotFound);

    /// <summary>
    /// Maps one poll batch of broker events to the resources they changed and notifies every subscriber
    /// granted those URIs — and only them. At most one notification per URI per batch: resources/updated
    /// is a hint to re-read, so N changes to one resource in one batch are one hint, not N.
    ///
    /// Quiet whispers never leave their participants: a whisper event contributes its channel URI to this
    /// session's dispatch only when this session authored it or was mentioned. Everyone else's subscriber
    /// lists never see the URI at all — there is no notification to suppress downstream because none is
    /// ever generated.
    /// </summary>
    public async Task NotifyEventsAsync(IEnumerable<PeerEvent> events, CancellationToken cancellationToken = default)
    {
        var uris = new HashSet<string>(StringComparer.Ordinal);
        foreach (var evt in events)
        {
            if (evt.Type == EventTypes.ChannelMessageAdded && IsWhisperNotForUs(evt.Payload))
                continue;

            if (ResourceUriFor(evt) is { } uri)
                uris.Add(uri);
        }

        foreach (var uri in uris)
            await NotifyUriAsync(uri, cancellationToken);
    }

    private async Task NotifyUriAsync(string uri, CancellationToken cancellationToken)
    {
        Subscription[] listeners;
        lock (_gate) listeners = _subscriptions.Where(s => s.Uris.Contains(uri)).ToArray();

        foreach (var subscription in listeners)
        {
            if (!subscription.IsActive)
                continue;

            try
            {
                await subscription.Server.SendNotificationAsync(
                    NotificationMethods.ResourceUpdatedNotification,
                    new ResourceUpdatedNotificationParams
                    {
                        Uri = uri,
                        Meta = new JsonObject { [MetaKeys.SubscriptionId] = SubscriptionId(subscription.Id) },
                    },
                    McpJsonUtilities.DefaultOptions,
                    cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException) { /* one dead stream, not the batch */ }
        }
    }

    /// <summary>True when this quiet message names neither us as author nor us as a mention.</summary>
    private bool IsWhisperNotForUs(string payload)
    {
        if (_state.PeerId is { } me)
        {
            try
            {
                using var doc = JsonDocument.Parse(payload);
                var root = doc.RootElement;
                var quiet = root.TryGetProperty("quiet", out var q) && q.ValueKind == JsonValueKind.True;
                if (!quiet) return false;

                if (root.TryGetProperty("author_peer_id", out var author) && author.GetString() == me)
                    return false;

                if (root.TryGetProperty("recipient_peer_ids", out var recipients))
                    foreach (var r in recipients.EnumerateArray())
                        if (r.GetString() == me)
                            return false;

                return true;
            }
            catch (JsonException)
            {
                return false; // unparsable payloads broadcast; delivery stays best-effort-loud
            }
        }
        return false;
    }

    /// <summary>The resource a broker event changed, or null when it changes none of them.</summary>
    internal static string? ResourceUriFor(PeerEvent evt) => evt.Type switch
    {
        EventTypes.PeerRegistered or EventTypes.PeerUpdated or EventTypes.PeerExpired => PeerResources.PeersUri,
        EventTypes.ChannelMessageAdded => ChannelIdOf(evt) is { } id ? PeerResources.ChannelUri(id) : null,
        _ => null,
    };

    private static string? ChannelIdOf(PeerEvent evt)
    {
        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);
            return doc.RootElement.GetProp("channel_id");
        }
        catch { return null; }
    }

    // Serialized fresh per notification: a JsonNode can only be parented once.
    private static JsonNode SubscriptionId(RequestId id) =>
        JsonSerializer.SerializeToNode(id, McpJsonUtilities.DefaultOptions)!;
}

internal static class JsonElementExtensions
{
    public static string? GetProp(this JsonElement el, string name) =>
        el.TryGetProperty(name, out var val) ? val.GetString() : null;
}
