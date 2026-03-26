using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Shared.Models;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class PollTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "peer_poll"), Description(
        "Poll for new events since your last cursor. Returns peer changes, scratchpad updates, artifact publishes. " +
        "Call this at startup, after completing tasks, and before finishing your session.")]
    public static async Task<string> Poll(
        BrokerClient client,
        SessionState state,
        [Description("Event cursor to poll from (0 for all). Use the cursor returned by previous poll.")] long? cursor = null,
        [Description("Max events to return (default 100)")] int? limit = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";

        var effectiveCursor = cursor ?? state.LastCursor;
        var resp = await client.PollAsync(state.PeerId, effectiveCursor, limit ?? 100);
        state.LastCursor = resp.Cursor;

        return JsonSerializer.Serialize(resp, JsonOpts);
    }

    [McpServerTool(Name = "peer_inbox"), Description(
        "Read directed collaboration events addressed to this peer in a human-friendly form. " +
        "Advances the session event cursor like peer_poll.")]
    public static async Task<string> Inbox(
        BrokerClient client,
        SessionState state,
        [Description("Max events to read before filtering directed inbox items (default 100)")] int? limit = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";

        var resp = await client.PollAsync(state.PeerId, state.LastCursor, limit ?? 100);
        state.LastCursor = resp.Cursor;

        var items = resp.Events
            .Where(evt => IsDirectedToPeer(evt, state.PeerId) && !IsOwnEvent(evt, state.PeerId))
            .Select(FormatInboxItem)
            .Where(item => item is not null)
            .ToList();

        return JsonSerializer.Serialize(new
        {
            cursor = resp.Cursor,
            items,
        }, JsonOpts);
    }

    private static bool IsDirectedToPeer(PeerEvent evt, string peerId)
    {
        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);
            return doc.RootElement.TryGetProperty("recipient_peer_ids", out var recipients)
                && ContainsString(recipients, peerId);
        }
        catch
        {
            return false;
        }
    }

    private static bool IsOwnEvent(PeerEvent evt, string peerId)
    {
        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);

            foreach (var propertyName in new[] { "peer_id", "author_peer_id", "created_by_peer_id", "published_by_peer_id" })
            {
                if (doc.RootElement.TryGetProperty(propertyName, out var actor) &&
                    actor.GetString() == peerId)
                    return true;
            }
        }
        catch
        {
        }

        return false;
    }

    private static object? FormatInboxItem(PeerEvent evt)
    {
        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);
            var root = doc.RootElement;

            return evt.Type switch
            {
                EventTypes.ScratchpadCreated => new
                {
                    event_id = evt.EventId,
                    type = evt.Type,
                    created_at = evt.CreatedAt,
                    scratchpad_id = GetProp(root, "scratchpad_id"),
                    scratchpad_type = GetProp(root, "type"),
                    title = GetProp(root, "title"),
                    from_peer_id = GetProp(root, "created_by_peer_id"),
                    message = $"Review request: {GetProp(root, "title") ?? "Untitled"}",
                },
                EventTypes.ScratchpadEntryAdded => new
                {
                    event_id = evt.EventId,
                    type = evt.Type,
                    created_at = evt.CreatedAt,
                    scratchpad_id = GetProp(root, "scratchpad_id"),
                    title = GetProp(root, "title"),
                    from_peer_id = GetProp(root, "author_peer_id"),
                    content_preview = Preview(GetProp(root, "content")),
                    message = $"Scratchpad reply: {GetProp(root, "title") ?? "Untitled"}",
                },
                EventTypes.ScratchpadClosed => new
                {
                    event_id = evt.EventId,
                    type = evt.Type,
                    created_at = evt.CreatedAt,
                    scratchpad_id = GetProp(root, "scratchpad_id"),
                    title = GetProp(root, "title"),
                    resolution = Preview(GetProp(root, "resolution"), 400),
                    message = $"Scratchpad closed: {GetProp(root, "title") ?? "Untitled"}",
                },
                _ => null,
            };
        }
        catch
        {
            return null;
        }
    }

    private static string? GetProp(JsonElement el, string name) =>
        el.TryGetProperty(name, out var val) ? val.GetString() : null;

    private static bool ContainsString(JsonElement el, string value)
    {
        if (el.ValueKind != JsonValueKind.Array)
            return false;

        foreach (var item in el.EnumerateArray())
        {
            if (item.GetString() == value)
                return true;
        }

        return false;
    }

    private static string Preview(string? content, int maxLength = 200)
    {
        if (string.IsNullOrWhiteSpace(content))
            return "(empty)";

        return content.Length > maxLength
            ? content[..maxLength] + "..."
            : content;
    }
}
