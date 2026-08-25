using System.Text.Json;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;
using Peers.Shared.Models;

namespace Peers.Mcp.Resources;

/// <summary>
/// Tracks which resource URIs this session's client subscribed to via <c>resources/subscribe</c>, and
/// fans the <c>notifications/resources/updated</c> hints out to it. One stdio process serves exactly
/// one MCP session, so there is one subscriber set and one captured <see cref="McpServer"/> — a client
/// subscribes by URI and every later hint for a subscribed URI goes over the shared session channel.
///
/// The fan-out is deliberately plain: resources/updated is a HINT TO RE-READ, never a delta stream, so
/// no subscription id, acknowledgement, or demultiplexing metadata rides on it. Peers fires neither
/// tools/list_changed nor resources/list_changed — both lists are static for the life of the process.
///
/// Quiet whispers are filtered per session at dispatch time: a quiet message contributes URIs here only
/// when this session authored it or was addressed, because this registry always runs inside exactly one
/// session's process and knows that session's peer id.
///
/// ponytail: stdio-only, deliberately. Every notification routes over the one shared session channel,
/// so a subscription only needs to remember its URIs. Streamable HTTP would need per-request transport
/// routing — peers does not have that transport, so this does not build for it.
/// </summary>
public sealed class SubscriptionRegistry
{
    private readonly McpServerResourceCollection _resources;
    private readonly SessionState _state;
    private readonly Lock _gate = new();
    private readonly HashSet<string> _subscribed = new(StringComparer.Ordinal);
    private McpServer? _server;

    public SubscriptionRegistry(McpServerResourceCollection resources, SessionState state)
    {
        _resources = resources;
        _state = state;
    }

    /// <summary>Subscribed-URI count. Kept because "the subscribe attempt was not granted" is a
    /// SERVER-side fact: an unsubscribed URI and a rejected subscribe look identical to a quiet client.</summary>
    internal int CountForTests { get { lock (_gate) return _subscribed.Count; } }

    internal bool IsSubscribedForTests(string uri) { lock (_gate) return _subscribed.Contains(uri); }

    /// <summary>
    /// Handles <c>resources/subscribe</c>: grant the URI when this server actually serves it, otherwise
    /// silently ignore it. A refusal would be worse than silence for the template case (see
    /// <see cref="Serves"/>), and the SDK answers nothing at all if this handler were unset — a client
    /// would believe it subscribed and wait forever with no way to debug the silence.
    /// </summary>
    public ValueTask<EmptyResult> HandleSubscribeAsync(
        RequestContext<SubscribeRequestParams> request,
        CancellationToken cancellationToken)
    {
        if (request.Params?.Uri is { } uri && Serves(uri))
        {
            lock (_gate)
            {
                // stdio: one session per process, so first subscribe captures THE server everything
                // notifies over.
                _server ??= request.Server;
                _subscribed.Add(uri);
            }
        }

        return ValueTask.FromResult(new EmptyResult());
    }

    public ValueTask<EmptyResult> HandleUnsubscribeAsync(
        RequestContext<UnsubscribeRequestParams> request,
        CancellationToken cancellationToken)
    {
        if (request.Params?.Uri is { } uri)
        {
            lock (_gate) _subscribed.Remove(uri);
        }

        return ValueTask.FromResult(new EmptyResult());
    }

    /// <summary>
    /// True when this server actually serves <paramref name="uri"/>; concrete URIs match templates via the SDK.
    ///
    /// A TEMPLATE string is rejected even though McpServerResource.IsMatch accepts it — and a client can
    /// plausibly send one, since 'peers://channel/{id}' is exactly what resources/templates/list hands
    /// it. Granting it would put a URI in the subscriber set that the dispatch, which only ever offers
    /// concrete URIs, can never match: granted and permanently silent. RFC 6570 reserves braces for
    /// expressions, so an expanded URI never has one.
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

    /// <summary>
    /// Maps one poll batch of broker events to the resources they changed and notifies the session once
    /// per distinct subscribed URI — and only for those. At most one notification per URI per batch:
    /// resources/updated is a hint to re-read, so N changes to one resource in one batch are one hint,
    /// not N.
    ///
    /// Quiet whispers never leave their participants: a whisper event contributes its channel URI to
    /// this session's dispatch only when this session authored it or was mentioned, plus its inbox URI
    /// when this session was among the recipients. Everyone else never sees the URIs at all.
    /// </summary>
    public async Task NotifyEventsAsync(IEnumerable<PeerEvent> events, CancellationToken cancellationToken = default)
    {
        var uris = new HashSet<string>(StringComparer.Ordinal);
        foreach (var evt in events)
            uris.UnionWith(ResourceUrisFor(evt));

        foreach (var uri in uris)
            await NotifyUriAsync(uri, cancellationToken);
    }

    private async Task NotifyUriAsync(string uri, CancellationToken cancellationToken)
    {
        McpServer? server;
        bool subscribed;
        lock (_gate)
        {
            server = _server;
            subscribed = _subscribed.Contains(uri);
        }

        if (server is null || !subscribed)
            return;

        try
        {
            await server.SendNotificationAsync(
                NotificationMethods.ResourceUpdatedNotification,
                new ResourceUpdatedNotificationParams { Uri = uri },
                McpJsonUtilities.DefaultOptions,
                cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException) { /* one dead stream, not the batch */ }
    }

    /// <summary>Every resource a broker event changed, or none when it changes none of them.</summary>
    internal HashSet<string> ResourceUrisFor(PeerEvent evt)
    {
        var uris = new HashSet<string>(StringComparer.Ordinal);
        switch (evt.Type)
        {
            // A roster change is only OUR business when it is somebody we work with. Unscoped, every
            // presence call anywhere on the machine woke every subscribed session — and since progress
            // reporting rides peer.updated, that turned into constant churn from unrelated pairs.
            //
            // peer.updated carries channel_peer_ids (peers sharing a channel with the subject);
            // registered/expired already carry recipient_peer_ids, which the broker resolved the same
            // way it resolves a mention. Either list absent means "no audience" — notify nobody.
            case EventTypes.PeerUpdated:
                if (PayloadNamesUs(evt.Payload, "channel_peer_ids"))
                    uris.Add(PeerResources.PeersUri);
                break;

            // Registration and expiry push NOTHING here. Their recipient_peer_ids is the durable INBOX
            // audience — every peer in the repo — so reusing it as a roster hint is repo scoping, and a
            // stranger starting a session in your repo would keep waking you. Correspondence scoping
            // cannot apply either: a peer that just registered has not corresponded with anyone yet.
            // Nothing is lost — both events already deliver durably to that inbox, so `wait` returns
            // them and the roster is one `peers` call away.
            case EventTypes.PeerRegistered or EventTypes.PeerExpired:
                break;

            case EventTypes.ChannelMessageAdded:
                // Loud messages notify everyone watching the channel; quiet whispers only their own
                // participants (which includes us when we authored them).
                if (ChannelIdOf(evt) is { } id && !IsWhisperNotForUs(evt.Payload))
                    uris.Add(PeerResources.ChannelUri(id));

                // recipient_peer_ids rides loud AND quiet sends alike (ChannelService) and equals the
                // broker's durable inbox set, so the hint and the inbox can never disagree.
                if (IsForOurInbox(evt.Payload))
                    uris.Add(PeerResources.InboxUri);
                break;
        }

        return uris;
    }

    /// <summary>True when the payload names this session among its durable inbox recipients.</summary>
    private bool IsForOurInbox(string payload) => PayloadNamesUs(payload, "recipient_peer_ids");

    /// <summary>
    /// True when <paramref name="arrayProperty"/> is an array naming this session. A property that is
    /// missing or null is NOT a broadcast here — it means the emitter resolved no audience, so nobody
    /// is notified. (Loud undirected messages carry "recipient_peer_ids": null for exactly that reason.)
    /// </summary>
    private bool PayloadNamesUs(string payload, string arrayProperty)
    {
        if (_state.PeerId is not { } me)
            return false;

        try
        {
            using var doc = JsonDocument.Parse(payload);
            if (!doc.RootElement.TryGetProperty(arrayProperty, out var ids)
                || ids.ValueKind != JsonValueKind.Array)
                return false;

            foreach (var id in ids.EnumerateArray())
                if (id.GetString() == me)
                    return true;

            return false;
        }
        catch (JsonException)
        {
            return false; // unparsable payloads broadcast; delivery stays best-effort-loud
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

    private static string? ChannelIdOf(PeerEvent evt)
    {
        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);
            return doc.RootElement.GetProp("channel_id");
        }
        catch { return null; }
    }
}

internal static class JsonElementExtensions
{
    public static string? GetProp(this JsonElement el, string name) =>
        el.TryGetProperty(name, out var val) ? val.GetString() : null;
}
