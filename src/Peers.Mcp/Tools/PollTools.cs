using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class PollTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "peer_poll"), Description(
        "Cursor-based debug feed of raw broker events (peer changes, scratchpad updates, artifact publishes) for this peer.\n" +
        "When NOT to use: do not use this for everyday directed-message reading — call peer_inbox instead. peer_poll exposes the cursor for diagnostics and replay.\n" +
        "Side effect: advances state.LastCursor to the effective cursor (last included event when truncated, broker cursor otherwise).\n" +
        "Returns: { cursor, events[], truncated, returned_count, remaining_hint }. Large payloads are truncated (content_truncated per event) and the result is size-capped; the cursor advances only past returned events (truncated=true means call again). Defaults to picking up from the session's last cursor when omitted.\n" +
        "Note: this is a best-effort debug feed — the background channel notifier may advance the cursor independently, so peer_poll is not a delivery guarantee; use peer_inbox for reliable directed-message delivery.")]
    public static async Task<string> Poll(
        BrokerClient client,
        SessionState state,
        [Description("Event cursor to poll from (0 for all). Use the cursor returned by previous poll.")] long? cursor = null,
        [Description("Max events to return (default 100)")] int? limit = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";

        try
        {
            var effectiveCursor = cursor ?? state.LastCursor;
            var resp = await client.PollAsync(state.PeerId, effectiveCursor, limit ?? 100);

            var shape = InboxResultShaper.ShapePoll(resp.Events, resp.Cursor, Limits.MaxToolResultChars, JsonOpts);
            state.LastCursor = shape.EffectiveCursor;

            return shape.Json;
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    private static string BrokerError(BrokerApiException ex) =>
        JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode }, JsonOpts);

    private static async Task AckPendingAsync(BrokerClient client, SessionState state)
    {
        var ids = state.PendingInboxAckIds;
        if (ids is { Length: > 0 })
        {
            await client.AckInboxAsync(state.PeerId!, ids);
            state.PendingInboxAckIds = null;
        }
    }

    private static string ShapeAndStage(SessionState state, InboxResponse? resp, bool? timedOut)
    {
        // Reserve headroom when we'll inject timed_out post-shape, so the final JSON stays <= cap (T13 finding #1 / T6 overshoot class).
        var reserve = timedOut is null ? 0 : 24;  // covers the widest ",\"timed_out\":false" rendering
        var shape = InboxResultShaper.ShapeInbox(resp?.Items ?? new List<InboxItemDto>(),
            state.LastCursor, Limits.MaxToolResultChars - reserve, JsonOpts);
        state.PendingInboxAckIds = shape.IncludedInboxIds.Length > 0 ? shape.IncludedInboxIds : null;
        if (timedOut is null) return shape.Json;
        var node = System.Text.Json.Nodes.JsonNode.Parse(shape.Json)!.AsObject();
        node["timed_out"] = timedOut.Value;
        return node.ToJsonString();
    }

    [McpServerTool(Name = "peer_inbox"), Description(
        "Drain the durable inbox of directed messages addressed to this peer (review requests, replies, DMs). This is the primary, everyday way to read peer activity — call at startup, after each task chunk, before finishing.\n" +
        "Delivery: at-least-once. Items fetched here are acked on the NEXT inbox call, so a tool error between calls causes redelivery.\n" +
        "When NOT to use: peer_poll is for raw cursor/event debugging — prefer peer_inbox for normal collaboration.\n" +
        "Returns: { cursor, items[] (each with inbox_id; large entry bodies are truncated to a ~2000-char preview with content_truncated:true plus scratchpad_id/entry_id — call scratchpad_read for the full text), truncated, returned_count, remaining_hint }. If truncated, call peer_inbox again to drain the rest.")]
    public static async Task<string> Inbox(
        BrokerClient client,
        SessionState state,
        [Description("Maximum number of durable inbox items to drain (default 100)")] int? limit = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";

        try
        {
            await AckPendingAsync(client, state);
            var resp = await client.GetInboxAsync(state.PeerId, limit ?? 100);
            return ShapeAndStage(state, resp, null);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "peer_wait"), Description(
        "Long-poll: block up to timeout_seconds (default 60, max 120) until a directed message arrives, then drain it exactly like peer_inbox (same deferred at-least-once ack, same size shaping).\n" +
        "Use this after requesting a review instead of repeatedly calling peer_inbox — it returns as soon as a message lands.\n" +
        "Returns the same shape as peer_inbox, plus timed_out:true when it returns empty at the deadline.")]
    public static async Task<string> Wait(
        BrokerClient client,
        SessionState state,
        [Description("Max seconds to block waiting for a directed event (default 60, max 120)")] int? timeout_seconds = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        var t = Math.Clamp(timeout_seconds ?? 60, 1, 120);
        try
        {
            await AckPendingAsync(client, state);
            var resp = await client.WaitInboxAsync(state.PeerId, t);
            return ShapeAndStage(state, resp, resp?.TimedOut ?? true);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "peer_test_push"), Description(
        "Diagnostic: emit a test push notification through this session's channel notifier.\n" +
        "If the message surfaces in the client UI, push mode is wired up; if not, fall back to peer_inbox for pull-mode delivery.\n" +
        "Returns: { ok, message } on success, { ok: false, error } if the notifier is not running.")]
    public static async Task<string> TestPush(
        SessionState state,
        [Description("Optional test message content")] string? message = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        if (state.ChannelNotifier is null) return """{"error":"Channel notifier not running."}""";

        var content = message ?? $"Peers push test from peer {state.PeerId} at {DateTime.UtcNow:O}";

        try
        {
            await state.ChannelNotifier.SendTestNotificationAsync(content);
            return JsonSerializer.Serialize(new { ok = true, message = "Test notification sent. If push is working, it should appear in the client." }, JsonOpts);
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new { ok = false, error = ex.Message }, JsonOpts);
        }
    }
}
