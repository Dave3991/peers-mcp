using Peers.Mcp.Tools;

namespace Peers.Mcp.Resources;

/// <summary>
/// The two MCP resources peers serves. Both are thin read-through views over tools that already
/// exist (<c>peer_list</c>, <c>scratchpad_read</c>) — same payload, same size shaping — so a client
/// can subscribe instead of polling a tool.
/// </summary>
internal static class PeerResources
{
    public const string PeersUri = "peers://peers";
    public const string ScratchpadUriTemplate = "peers://scratchpad/{id}";

    public static string ScratchpadUri(string scratchpadId) => $"peers://scratchpad/{scratchpadId}";

    /// <summary>Same words as the tools' guard (PollTools.cs) so clients meet one idiom everywhere.</summary>
    internal const string NotRegistered = """{"error":"Not registered. Call peer_register first."}""";

    public const string PeersDescription = """
        Live list of peer sessions known to the local broker — the same payload peer_list returns
        (peer_id, tool, repo_root, branch, role, summary, focus_paths, self, is_expired), this session included.

        Subscriptions: notifications/resources/updated for this URI is A HINT TO RE-READ, never a delta stream.
        Delivery is best-effort and lossy: the broker cursor advances before dispatch, there is no retry, an MCP
        process restart drops whatever was in flight, and granularity is the 5s poll — so several changes can
        collapse into one notification, or into none. Always re-read when notified; never reconstruct state from
        the notifications themselves.

        Inert until peer_register has been called on this session: before that, reading returns
        {"error":"Not registered. Call peer_register first."} and no updates are ever dispatched.
        """;

    public const string ScratchpadDescription = """
        One scratchpad thread by id: metadata plus entries — the same payload scratchpad_read returns, with the
        same size cap and truncation behaviour (re-read via scratchpad_read with an offset to page the rest).

        Subscriptions: notifications/resources/updated fires on entry appended, resolved, and closed, with the
        same best-effort, lossy, hint-to-re-read semantics described on peers://peers. Always re-read when
        notified; never reconstruct the thread from the notifications themselves.

        Inert until peer_register has been called on this session: before that, reading returns
        {"error":"Not registered. Call peer_register first."} and no updates are ever dispatched.
        """;

    public static Task<string> ReadPeers(BrokerClient client, SessionState state) =>
        state.PeerId is null ? Task.FromResult(NotRegistered) : PeerTools.PeerList(client, state);

    public static Task<string> ReadScratchpad(BrokerClient client, SessionState state, string id) =>
        state.PeerId is null ? Task.FromResult(NotRegistered) : ScratchpadTools.Read(client, id);
}
