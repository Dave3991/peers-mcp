using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class PeerTools
{
    [McpServerTool(Name = "peer_register"), Description("Register this session as a peer. Returns peer_id for use in other tools.")]
    public static async Task<string> PeerRegister(
        BrokerClient client,
        SessionState state,
        McpServer server,
        [Description("AI tool: claude-code, codex, other")] string tool = "claude-code",
        [Description("Repository root path")] string? repo_root = null,
        [Description("Git branch")] string? branch = null,
        [Description("Current commit SHA")] string? commit_sha = null,
        [Description("Role: implementing, reviewing, debugging, spec")] string? role = null,
        [Description("Brief summary of current work")] string? summary = null,
        [Description("JSON array of file paths this session focuses on")] string? focus_paths = null,
        [Description("Parent peer ID if this is a spawned session")] string? parent_peer_id = null)
    {
        var req = new RegisterPeerRequest
        {
            Tool = tool,
            MachineId = Environment.MachineName,
            RepoRoot = repo_root,
            Branch = branch,
            CommitSha = commit_sha,
            Role = role,
            Summary = summary,
            FocusPaths = focus_paths,
            ParentPeerId = parent_peer_id,
        };
        var resp = await client.RegisterAsync(req);

        await state.StopServicesAsync();
        state.PeerId = resp.PeerId;

        state.Heartbeat = new HeartbeatService(client, resp.PeerId);
        state.Heartbeat.Start();

        state.ChannelNotifier = new ChannelNotifier(client, state, server);
        state.ChannelNotifier.Start();

        return JsonSerializer.Serialize(new { peer_id = resp.PeerId, message = "Registered. Heartbeat and channel notifications started." });
    }

    [McpServerTool(Name = "peer_heartbeat"), Description("Renew lease and optionally update status fields.")]
    public static async Task<string> PeerHeartbeat(
        BrokerClient client,
        SessionState state,
        [Description("Update work summary")] string? summary = null,
        [Description("Update role")] string? role = null,
        [Description("Update branch")] string? branch = null,
        [Description("Update commit SHA")] string? commit_sha = null,
        [Description("Update focus paths (JSON array)")] string? focus_paths = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        await client.HeartbeatAsync(state.PeerId, new HeartbeatRequest
        {
            Summary = summary, Role = role, Branch = branch, CommitSha = commit_sha, FocusPaths = focus_paths,
        });
        return """{"ok":true}""";
    }

    [McpServerTool(Name = "peer_list"), Description("Discover other active peer sessions. Filter by repo, branch, or tool.")]
    public static async Task<string> PeerList(
        BrokerClient client,
        [Description("Filter by repo root path")] string? repo_root = null,
        [Description("Filter by branch")] string? branch = null,
        [Description("Filter by tool (claude-code, codex)")] string? tool = null)
    {
        var peers = await client.ListPeersAsync(repo_root, branch, tool);
        return JsonSerializer.Serialize(peers, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower });
    }
}
