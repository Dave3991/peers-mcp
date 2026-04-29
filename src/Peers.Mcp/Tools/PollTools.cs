using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class PollTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "peer_poll"), Description(
        "Cursor-based debug feed of raw broker events (peer changes, scratchpad updates, artifact publishes) for this peer.\n" +
        "When NOT to use: do not use this for everyday directed-message reading — call peer_inbox instead. peer_poll exposes the cursor for diagnostics and replay.\n" +
        "Side effect: advances state.LastCursor to resp.Cursor. Returns: { cursor, events[] }. Defaults to picking up from the session's last cursor when omitted.")]
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
            state.LastCursor = resp.Cursor;

            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    private static string BrokerError(BrokerApiException ex) =>
        JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode }, JsonOpts);

    [McpServerTool(Name = "peer_inbox"), Description(
        "Drain the durable inbox of directed messages addressed to this peer (review requests, replies, DMs). This is the primary, everyday way to read peer activity — call at startup, after each task chunk, before finishing.\n" +
        "Delivery: at-least-once. Items fetched here are acked on the NEXT inbox call, so a tool error between calls causes redelivery.\n" +
        "When NOT to use: peer_poll is for raw cursor/event debugging — prefer peer_inbox for normal collaboration.\n" +
        "Returns: { cursor, items[] (each with inbox_id), dropped_message_count, remaining_count }.")]
    public static async Task<string> Inbox(
        BrokerClient client,
        SessionState state,
        [Description("Maximum number of buffered inbox items to drain (default 100)")] int? limit = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";

        try
        {
            // Ack previously fetched items first (deferred from last call)
            var pendingAckIds = state.PendingInboxAckIds;
            if (pendingAckIds is { Length: > 0 })
            {
                await client.AckInboxAsync(state.PeerId, pendingAckIds);
                state.PendingInboxAckIds = null;
            }

            // Fetch new unacked items from durable broker inbox
            var resp = await client.GetInboxAsync(state.PeerId, limit ?? 100);

            // Store IDs for deferred ack on next call (at-least-once delivery)
            if (resp is not null && resp.Items.Count > 0)
                state.PendingInboxAckIds = resp.Items.Select(i => i.InboxId).ToArray();

            // Also drain any in-memory buffer
            var drained = state.DrainChannelMessages(limit);

            return JsonSerializer.Serialize(new
            {
                cursor = state.LastCursor,
                dropped_message_count = drained.DroppedMessageCount,
                remaining_count = drained.RemainingCount,
                items = resp?.Items ?? [],
            }, JsonOpts);
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
