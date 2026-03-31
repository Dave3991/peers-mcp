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
        [Description("Parent peer ID if this is a spawned session")] string? parent_peer_id = null,
        [Description("Explicit peer ID to resume (reactivates an expired or active session)")] string? resume_peer_id = null)
    {
        try
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
                ResumePeerId = resume_peer_id,
            };
            var resp = await client.RegisterAsync(req);

            await state.StopServicesAsync();
            state.PeerId = resp.PeerId;
            state.Tool = tool;
            state.LastCursor = resp.LastCursor;

            state.Heartbeat = new HeartbeatService(client, resp.PeerId);
            state.Heartbeat.Start();

            state.ChannelNotifier = new ChannelNotifier(client, state, server);
            state.ChannelNotifier.Start();

            return JsonSerializer.Serialize(new
            {
                peer_id = resp.PeerId,
                resumed = resp.Resumed,
                last_cursor = resp.LastCursor,
                message = resp.Resumed
                    ? "Resumed previous peer identity. Heartbeat and push notifications started."
                    : "Registered. Heartbeat and push notifications started.",
            });
        }
        catch (Exception ex)
        {
            await Console.Error.WriteLineAsync($"[Peers] peer_register failed: {ex}");
            return JsonSerializer.Serialize(new { error = ex.Message });
        }
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
        try
        {
            await client.HeartbeatAsync(state.PeerId, new HeartbeatRequest
            {
                Summary = summary, Role = role, Branch = branch, CommitSha = commit_sha, FocusPaths = focus_paths,
            });
            return """{"ok":true}""";
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "peer_list"), Description("Discover other active peer sessions. Filter by repo, branch, or tool.")]
    public static async Task<string> PeerList(
        BrokerClient client,
        [Description("Filter by repo root path")] string? repo_root = null,
        [Description("Filter by branch")] string? branch = null,
        [Description("Filter by tool (claude-code, codex)")] string? tool = null)
    {
        try
        {
            var peers = await client.ListPeersAsync(repo_root, branch, tool);
            return JsonSerializer.Serialize(peers, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower });
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    private static string BrokerError(BrokerApiException ex) =>
        JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode });
}
