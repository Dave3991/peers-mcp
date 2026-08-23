using System.Text.Json;
using ModelContextProtocol.Protocol;

namespace Peers.Mcp.Resources;

/// <summary>
/// The two MCP resources peers serves. Both are thin read-through views over the same broker calls the
/// tools make, so a client can subscribe instead of polling a tool. Registration is automatic now, so
/// neither resource has an inert phase: they read from the moment this process serves them.
/// </summary>
internal static class PeerResources
{
    public const string PeersUri = "peers://peers";
    public const string ChannelUriTemplate = "peers://channel/{id}";

    public static string ChannelUri(string channelId) => $"peers://channel/{channelId}";

    public const string PeersDescription = """
        Live roster of peer sessions known to the local broker — same payload as the peers tool
        (peer_id, tool, repo_root, branch, role, summary, self, lease state).

        Subscriptions: notifications/resources/updated for this URI is A HINT TO RE-READ, never a delta stream.
        Delivery is best-effort and lossy: the broker cursor advances before dispatch, there is no retry, an MCP
        process restart drops whatever was in flight, and granularity is the 5s poll — several changes can
        collapse into one notification, or into none. Always re-read when notified; never reconstruct roster
        state from the notifications themselves.
        """;

    public const string ChannelDescription = """
        One channel's messages by id, oldest first — same payload and quiet-visibility rules as the read
        tool: whispers addressed to other participants are never part of your view.

        Subscriptions: notifications/resources/updated fires when any participant posts; quiet whispers
        notify only their own participants. Same best-effort hint-to-re-read semantics as peers://peers.
        """;

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
