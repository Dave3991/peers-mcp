using System.Text.Json;
using System.Text.Json.Nodes;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Mcp;

public sealed record InboxShape(string Json, string[] IncludedInboxIds, int ReturnedCount, bool Truncated);
public sealed record PollShape(string Json, long EffectiveCursor, bool Truncated);

public static class InboxResultShaper
{
    public const int PerItemContentPreviewChars = 2000;

    /// <summary>
    /// Truncates the "content" field in a scratchpad.entry_added payload JSON to <paramref name="previewChars"/> chars,
    /// adding "content_truncated": true when truncation occurs.
    ///
    /// Only the <c>content</c> field is truncated; a payload with some OTHER huge field would still be large —
    /// no current event type has one (only scratchpad.entry_added's content is big).
    /// Revisit if a fat payload field is added later.
    /// </summary>
    public static string TruncatePayloadContent(string payloadJson, int previewChars)
    {
        try
        {
            var node = JsonNode.Parse(payloadJson);
            if (node is not JsonObject obj)
                return payloadJson;

            if (obj["content"] is not JsonValue contentVal)
                return payloadJson;

            var content = contentVal.GetValue<string>();
            if (content.Length <= previewChars)
                return payloadJson;

            // Back off to avoid splitting a UTF-16 surrogate pair (reviewer finding #2)
            var cut = previewChars;
            if (cut > 0 && char.IsHighSurrogate(content[cut - 1]))
                cut--;

            obj["content"] = content[..cut];
            obj["content_truncated"] = true;

            return obj.ToJsonString();
        }
        catch
        {
            // If parse fails for any reason, return unchanged
            return payloadJson;
        }
    }

    /// <summary>
    /// Greedily shapes inbox items into a size-capped JSON envelope.
    /// Always includes at least 1 item if any are present.
    /// </summary>
    public static InboxShape ShapeInbox(
        IReadOnlyList<InboxItemDto> items,
        long cursor,
        int maxChars,
        JsonSerializerOptions opts)
    {
        if (items.Count == 0)
        {
            var emptyEnvelope = new
            {
                cursor,
                items = Array.Empty<object>(),
                truncated = false,
                returned_count = 0,
                remaining_hint = (string?)null
            };
            return new InboxShape(
                Json: JsonSerializer.Serialize(emptyEnvelope, opts),
                IncludedInboxIds: [],
                ReturnedCount: 0,
                Truncated: false);
        }

        var shaped = new List<object>();
        var includedIds = new List<string>();
        var truncated = false;
        string lastGoodJson = string.Empty;

        for (int i = 0; i < items.Count; i++)
        {
            var item = items[i];
            var shapedItem = new
            {
                item.InboxId,
                item.PeerId,
                Event = new
                {
                    item.Event.EventId,
                    item.Event.Type,
                    Payload = TruncatePayloadContent(item.Event.Payload, PerItemContentPreviewChars),
                    item.Event.CreatedAt
                },
                item.CreatedAt
            };

            shaped.Add(shapedItem);
            includedIds.Add(item.InboxId);

            var candidateEnvelope = BuildInboxEnvelope(cursor, shaped, items.Count, truncated, opts);
            var candidateJson = JsonSerializer.Serialize(candidateEnvelope, opts);

            if (candidateJson.Length > maxChars && shaped.Count > 1)
            {
                // Over the limit and we have more than 1 — remove last and stop
                shaped.RemoveAt(shaped.Count - 1);
                includedIds.RemoveAt(includedIds.Count - 1);
                truncated = true;

                // Rebuild with truncated=true; then drop until it fits (hint length may push it over)
                lastGoodJson = JsonSerializer.Serialize(BuildInboxEnvelope(cursor, shaped, items.Count, truncated, opts), opts);
                while (lastGoodJson.Length > maxChars && shaped.Count > 1)
                {
                    shaped.RemoveAt(shaped.Count - 1);
                    includedIds.RemoveAt(includedIds.Count - 1);
                    lastGoodJson = JsonSerializer.Serialize(BuildInboxEnvelope(cursor, shaped, items.Count, truncated, opts), opts);
                }
                break;
            }

            lastGoodJson = candidateJson;
        }

        return new InboxShape(
            Json: lastGoodJson,
            IncludedInboxIds: includedIds.ToArray(),
            ReturnedCount: shaped.Count,
            Truncated: truncated);
    }

    private static object BuildInboxEnvelope(
        long cursor,
        List<object> shaped,
        int totalCount,
        bool truncated,
        JsonSerializerOptions opts)
    {
        var remaining = totalCount - shaped.Count;
        var remainingHint = truncated
            ? $"{remaining} more item(s) withheld by the size cap; call peer_inbox again to drain them"
            : (string?)null;

        return new
        {
            cursor,
            items = shaped.ToArray(),
            truncated,
            returned_count = shaped.Count,
            remaining_hint = remainingHint
        };
    }

    /// <summary>
    /// Greedily shapes poll events into a size-capped JSON envelope.
    /// Always includes at least 1 event if any are present.
    /// The cursor advances only past included events when truncated.
    /// </summary>
    public static PollShape ShapePoll(
        IReadOnlyList<PeerEvent> events,
        long brokerCursor,
        int maxChars,
        JsonSerializerOptions opts)
    {
        if (events.Count == 0)
        {
            var emptyEnvelope = new
            {
                cursor = brokerCursor,
                events = Array.Empty<object>(),
                truncated = false,
                returned_count = 0,
                remaining_hint = (string?)null
            };
            return new PollShape(
                Json: JsonSerializer.Serialize(emptyEnvelope, opts),
                EffectiveCursor: brokerCursor,
                Truncated: false);
        }

        var shaped = new List<object>();
        var truncated = false;
        long effectiveCursor = brokerCursor;
        string lastGoodJson = string.Empty;

        for (int i = 0; i < events.Count; i++)
        {
            var ev = events[i];
            var shapedEvent = new
            {
                ev.EventId,
                ev.Type,
                Payload = TruncatePayloadContent(ev.Payload, PerItemContentPreviewChars),
                ev.CreatedAt
            };

            shaped.Add(shapedEvent);

            var candidateEnvelope = BuildPollEnvelope(brokerCursor, shaped, events.Count, truncated, opts);
            var candidateJson = JsonSerializer.Serialize(candidateEnvelope, opts);

            if (candidateJson.Length > maxChars && shaped.Count > 1)
            {
                // Over the limit and we have more than 1 — remove last and stop
                shaped.RemoveAt(shaped.Count - 1);
                truncated = true;
                effectiveCursor = events[shaped.Count - 1].EventId;

                // Rebuild with truncated=true; then drop until it fits (hint length may push it over)
                lastGoodJson = JsonSerializer.Serialize(BuildPollEnvelope(effectiveCursor, shaped, events.Count, truncated, opts), opts);
                while (lastGoodJson.Length > maxChars && shaped.Count > 1)
                {
                    shaped.RemoveAt(shaped.Count - 1);
                    effectiveCursor = events[shaped.Count - 1].EventId;
                    lastGoodJson = JsonSerializer.Serialize(BuildPollEnvelope(effectiveCursor, shaped, events.Count, truncated, opts), opts);
                }
                break;
            }

            lastGoodJson = candidateJson;
        }

        if (!truncated)
            effectiveCursor = brokerCursor;

        return new PollShape(
            Json: lastGoodJson,
            EffectiveCursor: effectiveCursor,
            Truncated: truncated);
    }

    private static object BuildPollEnvelope(
        long cursor,
        List<object> shaped,
        int totalCount,
        bool truncated,
        JsonSerializerOptions opts)
    {
        var remaining = totalCount - shaped.Count;
        var remainingHint = truncated
            ? $"{remaining} more event(s) withheld by the size cap; call peer_poll again to drain them"
            : (string?)null;

        return new
        {
            cursor,
            events = shaped.ToArray(),
            truncated,
            returned_count = shaped.Count,
            remaining_hint = remainingHint
        };
    }
}
