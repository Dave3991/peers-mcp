using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Mcp;
using Peers.Shared.Contracts;
using RecipientStatus = Peers.Shared.Contracts.RecipientDeliveryStatus;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class ScratchpadTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "scratchpad_create"), Description(
        "Create a new shared scratchpad (a thread) for async collaboration: review, discussion, spec, or context.\n" +
        "When to use: starting a fresh topic or review request. To add to an existing pad, call scratchpad_append instead.\n" +
        "Pass `content` to post the opening body as the first entry in the same call (recommended for review/spec pads).\n" +
        "Side effect: emits an event and pushes notifications to resolved recipients (peer IDs or repo paths in requested_reviewers).\n" +
        "Returns: { scratchpad_id, entry_id?, recipients (online/offline/unresolved), delivery_summary }.")]
    public static async Task<string> Create(
        BrokerClient client,
        SessionState state,
        [Description("Scratchpad type: review, discussion, spec, context")] string type,
        [Description("Title for the scratchpad")] string title,
        [Description("Repository root this scratchpad relates to")] string? repo_root = null,
        [Description("Peer IDs or repo paths to request review from")] string[]? requested_reviewers = null,
        [Description("Artifact IDs to reference")] string[]? artifact_refs = null,
        [Description("Optional opening markdown body — posted as the first entry on the new pad")] string? content = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        try
        {
            var resp = await client.CreateScratchpadAsync(new CreateScratchpadRequest
            {
                Type = type, Title = title, RepoRoot = repo_root,
                RequestedReviewers = requested_reviewers is null ? null : JsonSerializer.Serialize(requested_reviewers),
                ArtifactRefs = artifact_refs is null ? null : JsonSerializer.Serialize(artifact_refs),
                CreatedByPeerId = state.PeerId,
            });

            string? entryId = null;
            string? contentError = null;
            if (!string.IsNullOrEmpty(content))
            {
                try
                {
                    var append = await client.AppendEntryAsync(resp.ScratchpadId, new AppendEntryRequest
                    {
                        AuthorPeerId = state.PeerId,
                        Content = content,
                        Quiet = true, // create event already notified recipients
                    });
                    entryId = append?.EntryId;
                }
                catch (BrokerApiException ex)
                {
                    contentError = $"Scratchpad created but initial content append failed ({(int)ex.StatusCode}): {ex.ErrorBody ?? ex.Message}. Retry with scratchpad_append.";
                }
            }

            return JsonSerializer.Serialize(new
            {
                scratchpad_id = resp.ScratchpadId,
                entry_id = entryId,
                content_error = contentError,
                recipients = resp.Recipients,
                delivery_summary = FormatDeliverySummary(resp.Recipients),
            }, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "scratchpad_append"), Description(
        "Append a markdown entry to an existing scratchpad. Use for review comments, follow-ups, and ongoing discussion.\n" +
        "Limit: 100 KB per entry; for large blobs publish an artifact and reference it instead.\n" +
        "Idempotency: passing idempotency_key dedups per-pad — a duplicate returns {\"duplicate\":true,...} with no new entry.\n" +
        "Side effect: emits an event and pushes to recipients unless quiet=true. Returns: { entry_id, recipients, delivery_summary } on first write.")]
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

    [McpServerTool(Name = "scratchpad_read"), Description(
        "Read a scratchpad's metadata and entries. Paginated via limit (default 200) and offset.\n" +
        "Returns: { scratchpad, entries[] } when the full page fits within the 30 KB cap (PEERS_MAX_TOOL_RESULT_CHARS).\n" +
        "On overflow: returns a greedy partial page as { scratchpad, entries[], truncated: true, included_count, page_entry_count, next_offset }.\n" +
        "To page: call again with offset=next_offset (do NOT compare included_count to page_entry_count to decide whether to page — use next_offset as the signal).")]
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

            return OutputShaper.ShapeScratchpadRead(resp, offset ?? 0, Limits.MaxToolResultChars, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "scratchpad_list"), Description(
        "List scratchpad summaries (no entry bodies). Filter by type, repo_root, or status. Results are newest-first (default 50 per page).\n" +
        "Use to discover open reviews/discussions before calling scratchpad_read for full content.\n" +
        "Paginate via offset (e.g. offset=50 for the second page).\n" +
        "Returns: array of { scratchpad_id, type, title, status, created_by_peer_id, ... }.")]
    public static async Task<string> List(
        BrokerClient client,
        [Description("Filter by type: review, discussion, spec, context")] string? type = null,
        [Description("Filter by repo root")] string? repo_root = null,
        [Description("Filter by status: open, resolved, closed")] string? status = null,
        [Description("Max pads to return (default 50, newest first)")] int? limit = null,
        [Description("Offset for pagination")] int? offset = null)
    {
        try
        {
            var pads = await client.ListScratchpadsAsync(type, repo_root, status, limit ?? 50, offset ?? 0);
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

    [McpServerTool(Name = "scratchpad_resolve"), Description(
        "Mark a scratchpad as resolved (e.g. review approved) while keeping it open for further entries.\n" +
        "When NOT to use: if the thread is finished entirely, call scratchpad_close instead.\n" +
        "Returns: {\"ok\":true}.")]
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

    [McpServerTool(Name = "scratchpad_close"), Description(
        "Close a scratchpad permanently with an optional resolution summary; further appends are rejected.\n" +
        "When NOT to use: if the thread may still receive replies, call scratchpad_resolve to mark complete but stay open.\n" +
        "Returns: {\"ok\":true}.")]
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
