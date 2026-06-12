using System.ComponentModel;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class PeerTools
{
    [McpServerTool(Name = "peer_register"), Description(
        "Register this session as a peer in the local broker. Call once at startup before any other peer tool.\n" +
        "Side effects: starts a 30s heartbeat loop and a 5s channel-poll loop; lease expires after 2 min without heartbeat.\n" +
        "Returns: { peer_id, resumed, last_cursor, message }. Pass resume_peer_id to reclaim a previous identity (only succeeds if MachineId matches).\n" +
        "Pass worktree_root when working in a linked git worktree — it participates in identity/resume so two worktrees of the same repo+branch are distinct resumable peers.")]
    public static async Task<string> PeerRegister(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("AI tool identifying this session: 'claude-code', 'codex', or 'other'. Required — do not assume.")] string tool,
        [Description("Repository root path")] string? repo_root = null,
        [Description("Git branch")] string? branch = null,
        [Description("Current commit SHA")] string? commit_sha = null,
        [Description("Role: implementing, reviewing, debugging, spec")] string? role = null,
        [Description("Brief summary of current work")] string? summary = null,
        [Description("File paths this session focuses on")] string[]? focus_paths = null,
        [Description("Git worktree root (e.g. output of `git rev-parse --show-toplevel`). Pass this when working in a linked git worktree so two worktrees of the same repo+branch register as DISTINCT resumable identities (worktree_root is part of the resume key).")] string? worktree_root = null,
        [Description("Capability tags this session advertises, e.g. [\"build\",\"test\",\"review\"]")] string[]? capabilities = null,
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
                WorktreeRoot = worktree_root,
                Branch = branch,
                CommitSha = commit_sha,
                Role = role,
                Summary = summary,
                FocusPaths = focus_paths is null ? null : JsonSerializer.Serialize(focus_paths),
                Capabilities = capabilities is null ? null : JsonSerializer.Serialize(capabilities),
                ParentPeerId = parent_peer_id,
                ResumePeerId = resume_peer_id,
            };
            var resp = await client.RegisterAsync(req);

            await state.StopServicesAsync();
            state.PeerId = resp.PeerId;
            state.Tool = tool;
            state.LastCursor = resp.LastCursor;
            state.LastRegisterRequest = req;

            state.Heartbeat = new HeartbeatService(client, state, loggerFactory.CreateLogger<HeartbeatService>());
            state.Heartbeat.Start();

            state.ChannelNotifier = new ChannelNotifier(client, state, server, loggerFactory.CreateLogger<ChannelNotifier>());
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
            loggerFactory.CreateLogger<PeerTools>().LogError(ex, "peer_register failed");
            return JsonSerializer.Serialize(new { error = ex.Message });
        }
    }

    [McpServerTool(Name = "peer_heartbeat"), Description(
        "Renew this peer's 2-minute lease and optionally update status fields visible to other peers.\n" +
        "The background heartbeat loop already runs every 30s; only call this manually when role/branch/summary/focus changes.\n" +
        "Returns: {\"ok\":true} on success.")]
    public static async Task<string> PeerHeartbeat(
        BrokerClient client,
        SessionState state,
        [Description("Update work summary")] string? summary = null,
        [Description("Update role")] string? role = null,
        [Description("Update branch")] string? branch = null,
        [Description("Update commit SHA")] string? commit_sha = null,
        [Description("Update focus paths")] string[]? focus_paths = null,
        [Description("Update capability tags")] string[]? capabilities = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        try
        {
            await client.HeartbeatAsync(state.PeerId, new HeartbeatRequest
            {
                Summary = summary, Role = role, Branch = branch, CommitSha = commit_sha,
                FocusPaths = focus_paths is null ? null : JsonSerializer.Serialize(focus_paths),
                Capabilities = capabilities is null ? null : JsonSerializer.Serialize(capabilities),
            });
            return """{"ok":true}""";
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "peer_list"), Description(
        "Discover other peer sessions. Use to find collaborators, see what repos/branches are active, or pick targets for review requests.\n" +
        "Excludes peers whose 2-minute lease has expired (no includeExpired flag exposed here).\n" +
        "Returns: array of peer summaries (peer_id, tool, repo_root, branch, role, summary, focus_paths).")]
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
