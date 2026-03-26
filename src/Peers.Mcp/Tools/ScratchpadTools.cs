using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class ScratchpadTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "scratchpad_create"), Description("Create a shared scratchpad for async collaboration (review, discussion, spec, context).")]
    public static async Task<string> Create(
        BrokerClient client,
        SessionState state,
        [Description("Scratchpad type: review, discussion, spec, context")] string type,
        [Description("Title for the scratchpad")] string title,
        [Description("Repository root this scratchpad relates to")] string? repo_root = null,
        [Description("JSON array of peer IDs or repo paths to request review from")] string? requested_reviewers = null,
        [Description("JSON array of artifact IDs to reference")] string? artifact_refs = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        var resp = await client.CreateScratchpadAsync(new CreateScratchpadRequest
        {
            Type = type, Title = title, RepoRoot = repo_root,
            RequestedReviewers = requested_reviewers, ArtifactRefs = artifact_refs,
            CreatedByPeerId = state.PeerId,
        });
        return JsonSerializer.Serialize(resp, JsonOpts);
    }

    [McpServerTool(Name = "scratchpad_append"), Description("Append an entry to a scratchpad. Use for review comments, discussion, or context sharing.")]
    public static async Task<string> Append(
        BrokerClient client,
        SessionState state,
        [Description("Scratchpad ID to append to")] string scratchpad_id,
        [Description("Markdown content to append")] string content,
        [Description("Unique key for idempotent append (prevents duplicates)")] string? idempotency_key = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        var resp = await client.AppendEntryAsync(scratchpad_id, new AppendEntryRequest
        {
            AuthorPeerId = state.PeerId, Content = content, IdempotencyKey = idempotency_key,
        });
        return resp is not null
            ? JsonSerializer.Serialize(resp, JsonOpts)
            : """{"duplicate":true,"message":"Entry with this idempotency key already exists."}""";
    }

    [McpServerTool(Name = "scratchpad_read"), Description("Read entries from a scratchpad. Supports pagination.")]
    public static async Task<string> Read(
        BrokerClient client,
        [Description("Scratchpad ID to read")] string scratchpad_id,
        [Description("Max entries to return (default 200)")] int? limit = null,
        [Description("Offset for pagination")] int? offset = null)
    {
        var resp = await client.ReadScratchpadAsync(scratchpad_id, limit ?? 200, offset ?? 0);
        return resp is not null
            ? JsonSerializer.Serialize(resp, JsonOpts)
            : """{"error":"Scratchpad not found."}""";
    }

    [McpServerTool(Name = "scratchpad_list"), Description("List scratchpads. Filter by type, repo, or status.")]
    public static async Task<string> List(
        BrokerClient client,
        [Description("Filter by type: review, discussion, spec, context")] string? type = null,
        [Description("Filter by repo root")] string? repo_root = null,
        [Description("Filter by status: open, resolved, closed")] string? status = null)
    {
        var pads = await client.ListScratchpadsAsync(type, repo_root, status);
        return JsonSerializer.Serialize(pads, JsonOpts);
    }

    [McpServerTool(Name = "scratchpad_close"), Description("Close a scratchpad with an optional resolution summary.")]
    public static async Task<string> Close(
        BrokerClient client,
        [Description("Scratchpad ID to close")] string scratchpad_id,
        [Description("Resolution summary")] string? resolution = null)
    {
        await client.CloseScratchpadAsync(scratchpad_id, new CloseScratchpadRequest { Resolution = resolution });
        return """{"ok":true}""";
    }
}
