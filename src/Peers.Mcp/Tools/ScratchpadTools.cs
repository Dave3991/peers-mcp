using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Shared.Contracts;
using RecipientStatus = Peers.Shared.Contracts.RecipientDeliveryStatus;

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
        try
        {
            var resp = await client.CreateScratchpadAsync(new CreateScratchpadRequest
            {
                Type = type, Title = title, RepoRoot = repo_root,
                RequestedReviewers = requested_reviewers, ArtifactRefs = artifact_refs,
                CreatedByPeerId = state.PeerId,
            });
            return JsonSerializer.Serialize(new
            {
                scratchpad_id = resp.ScratchpadId,
                recipients = resp.Recipients,
                delivery_summary = FormatDeliverySummary(resp.Recipients),
            }, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "scratchpad_append"), Description("Append an entry to a scratchpad. Use for review comments, discussion, or context sharing.")]
    public static async Task<string> Append(
        BrokerClient client,
        SessionState state,
        [Description("Scratchpad ID to append to")] string scratchpad_id,
        [Description("Markdown content to append")] string content,
        [Description("Unique key for idempotent append (prevents duplicates)")] string? idempotency_key = null,
        [Description("Suppress push notification for this entry (event still recorded)")] bool? quiet = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        try
        {
            var resp = await client.AppendEntryAsync(scratchpad_id, new AppendEntryRequest
            {
                AuthorPeerId = state.PeerId, Content = content, IdempotencyKey = idempotency_key,
                Quiet = quiet ?? false,
            });
            if (resp is null)
                return """{"duplicate":true,"message":"Entry with this idempotency key already exists."}""";

            return JsonSerializer.Serialize(new
            {
                entry_id = resp.EntryId,
                recipients = resp.Recipients,
                delivery_summary = FormatDeliverySummary(resp.Recipients),
            }, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "scratchpad_read"), Description("Read entries from a scratchpad. Supports pagination.")]
    public static async Task<string> Read(
        BrokerClient client,
        [Description("Scratchpad ID to read")] string scratchpad_id,
        [Description("Max entries to return (default 200)")] int? limit = null,
        [Description("Offset for pagination")] int? offset = null)
    {
        try
        {
            var resp = await client.ReadScratchpadAsync(scratchpad_id, limit ?? 200, offset ?? 0);
            if (resp is null)
                return """{"error":"Scratchpad not found."}""";

            var result = JsonSerializer.Serialize(resp, JsonOpts);
            if (result.Length > Limits.MaxToolResultChars)
            {
                var meta = new
                {
                    scratchpad = resp.Scratchpad,
                    entry_count = resp.Entries.Count,
                    total_bytes = resp.Entries.Sum(e => e.ContentBytes),
                    truncated = true,
                    message = $"Response too large ({result.Length} chars). Use limit/offset to page through entries.",
                };
                return JsonSerializer.Serialize(meta, JsonOpts);
            }
            return result;
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "scratchpad_list"), Description("List scratchpads. Filter by type, repo, or status.")]
    public static async Task<string> List(
        BrokerClient client,
        [Description("Filter by type: review, discussion, spec, context")] string? type = null,
        [Description("Filter by repo root")] string? repo_root = null,
        [Description("Filter by status: open, resolved, closed")] string? status = null)
    {
        try
        {
            var pads = await client.ListScratchpadsAsync(type, repo_root, status);
            return JsonSerializer.Serialize(pads, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    private static string? FormatDeliverySummary(RecipientStatus? status)
    {
        if (status is null) return null;
        var parts = new List<string>();
        if (status.OnlinePeerIds.Length > 0)
            parts.Add($"{status.OnlinePeerIds.Length} online");
        if (status.OfflinePeerIds.Length > 0)
            parts.Add($"{status.OfflinePeerIds.Length} offline");
        if (status.UnresolvedTargets.Length > 0)
            parts.Add($"{status.UnresolvedTargets.Length} unresolved ({string.Join(", ", status.UnresolvedTargets)})");
        return parts.Count == 0 ? "No recipients resolved." : string.Join("; ", parts);
    }

    [McpServerTool(Name = "scratchpad_resolve"), Description("Mark a scratchpad as resolved (reviewed/approved) without closing it.")]
    public static async Task<string> Resolve(
        BrokerClient client,
        [Description("Scratchpad ID to resolve")] string scratchpad_id,
        [Description("Resolution summary")] string? resolution = null)
    {
        try
        {
            await client.ResolveScratchpadAsync(scratchpad_id, new ResolveScratchpadRequest { Resolution = resolution });
            return """{"ok":true}""";
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "scratchpad_close"), Description("Close a scratchpad with an optional resolution summary.")]
    public static async Task<string> Close(
        BrokerClient client,
        [Description("Scratchpad ID to close")] string scratchpad_id,
        [Description("Resolution summary")] string? resolution = null)
    {
        try
        {
            await client.CloseScratchpadAsync(scratchpad_id, new CloseScratchpadRequest { Resolution = resolution });
            return """{"ok":true}""";
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    private static string BrokerError(BrokerApiException ex) =>
        JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode }, JsonOpts);
}
