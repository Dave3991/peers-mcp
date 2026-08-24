using System.Text.Json;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;

namespace Peers.Mcp.Resources;

/// <summary>
/// The MCP resources peers serves. All are thin read-through views over the same broker calls the
/// tools make, so a client can subscribe instead of polling a tool. Registration is automatic now, so
/// no resource has an inert phase: they read from the moment this process serves them.
/// </summary>
internal static class PeerResources
{
    public const string PeersUri = "peers://peers";
    public const string ChannelUriTemplate = "peers://channel/{id}";
    public const string InboxUri = "peers://inbox";

    public static string ChannelUri(string channelId) => $"peers://channel/{channelId}";

    public const string PeersDescription = """
        Live roster of peer sessions known to the local broker — same payload as the peers tool
        (peer_id, tool, repo_root, branch, role, summary, self, lease state).

        Subscriptions: notifications/resources/updated for this URI is A HINT TO RE-READ, never a delta stream.
        Delivery is best-effort and lossy: there is no retry, an MCP process restart drops whatever was in
        flight, and granularity is the 5s poll — several changes can collapse into one notification, or into
        none. Always re-read when notified; never reconstruct roster state from the notifications themselves.
        """;

    public const string ChannelDescription = """
        One channel's messages by id, oldest first — same payload and quiet-visibility rules as the read
        tool: whispers addressed to other participants are never part of your view.

        Subscriptions: notifications/resources/updated fires when any participant posts; quiet whispers
        notify only their own participants. Same best-effort hint-to-re-read semantics as peers://peers.
        """;

    public const string InboxDescription = """
        This session's durable inbox: directed mentions, replies and review requests not yet
        acknowledged — same payload as the wait tool returns, shaped and size-capped the same way.

        Subscriptions: notifications/resources/updated for this URI is A HINT TO RE-READ. Delivery is
        best-effort and lossy: several messages can collapse into one notification, or into none. Always
        re-read when notified; never assume nothing arrived because no hint came.

        READING ACKNOWLEDGES NOTHING: an item read here is delivered again by wait until you echo its
        inbox_id back as ack_inbox_ids. Acknowledgement happens only via wait.
        """;

    /// <summary>
    /// This session's unacknowledged inbox. Registration runs first because state.PeerId is null until
    /// it does — mirroring every tool's ordering.
    ///
    /// FAILURE POSTURE: no try/catch-to-empty anywhere. If anything fails (broker down, identity not
    /// derivable) the exception propagates and the SDK turns it into a visible resource error. An empty-
    /// success fallback here would be a silent TOTAL notification failure — the exact failure class the
    /// push path exists to eliminate.
    /// </summary>
    public static async Task<string> ReadInbox(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory);

        var inbox = await client.GetInboxAsync(state.PeerId!, limit: 100);
        return InboxResultShaper.ShapeInbox(inbox?.Items ?? [], state.LastCursor, Limits.MaxToolResultChars, SnakeCase).Json;
    }

    public static async Task<string> ReadPeers(BrokerClient client)
    {
        var peers = await client.ListPeersAsync();
        return JsonSerializer.Serialize(peers, SnakeCase);
    }

    /// <summary>
    /// One channel's messages. The viewer is this session's own peer id, which is what filters quiet
    /// whispers addressed to other participants out of the payload.
    /// </summary>
    public static async Task<string> ReadChannel(BrokerClient client, SessionState state, string id)
    {
        var messages = await client.ReadChannelMessagesAsync(id, state.PeerId);
        return JsonSerializer.Serialize(messages, SnakeCase);
    }

    public static readonly JsonSerializerOptions SnakeCase = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
}
