using System.Text.Json;
using System.Text.Json.Nodes;
using Peers.Mcp;
using Peers.Shared.Contracts;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Mcp.Tests;

public class InboxResultShaperTests
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    // Builds a scratchpad.entry_added payload JSON with content of length n
    private static string EntryAddedPayload(int i, int contentLength) =>
        $"{{\"scratchpad_id\":\"sp1\",\"entry_id\":\"e{i}\",\"content\":\"{new string('x', contentLength)}\"}}";

    private static InboxItemDto MakeInboxItem(int i, string payload, string type = "scratchpad.entry_added") =>
        new()
        {
            InboxId = $"inbox-{i}",
            PeerId = $"peer-{i}",
            Event = new PeerEvent
            {
                EventId = i,
                Type = type,
                Payload = payload,
                CreatedAt = DateTime.UtcNow
            },
            CreatedAt = DateTime.UtcNow
        };

    private static PeerEvent MakePeerEvent(int id, string payload, string type = "scratchpad.entry_added") =>
        new()
        {
            EventId = id,
            Type = type,
            Payload = payload,
            CreatedAt = DateTime.UtcNow
        };

    // Test 1: 10 items with ~50,000-char content → size-capped, truncated, all returned items have preview content.
    // After per-item content truncation to PerItemContentPreviewChars (~2000 chars), each item is ~2300 chars.
    // 10 items × ~2300 = ~23 KB, so we use maxChars: 10000 to ensure the size cap fires before all 10 fit.
    [Fact]
    public void ShapeInbox_LargeItems_SizeCappedAndTruncated()
    {
        var items = Enumerable.Range(0, 10)
            .Select(i => MakeInboxItem(i, EntryAddedPayload(i, 50_000)))
            .ToList();

        var shape = InboxResultShaper.ShapeInbox(items, cursor: 0, maxChars: 10_000, JsonOpts);

        Assert.True(shape.Json.Length <= 10_000, $"Json.Length={shape.Json.Length} exceeds 10000");
        Assert.True(shape.ReturnedCount < 10, $"ReturnedCount={shape.ReturnedCount} should be < 10 for 10 large items");
        Assert.True(shape.Truncated);
        Assert.Equal(shape.ReturnedCount, shape.IncludedInboxIds.Length);

        // IncludedInboxIds should be a contiguous prefix of inbox-0..inbox-9
        for (int i = 0; i < shape.ReturnedCount; i++)
            Assert.Equal($"inbox-{i}", shape.IncludedInboxIds[i]);

        // Parse and verify each returned item's payload content is truncated
        var doc = JsonNode.Parse(shape.Json)!.AsObject();
        var returnedItems = doc["items"]!.AsArray();
        foreach (var item in returnedItems)
        {
            var payloadStr = item!["event"]!["payload"]!.GetValue<string>();
            var payloadNode = JsonNode.Parse(payloadStr)!.AsObject();
            Assert.True(payloadNode["content"]!.GetValue<string>().Length <= InboxResultShaper.PerItemContentPreviewChars);
            Assert.True(payloadNode["content_truncated"]!.GetValue<bool>());
        }
    }

    // Test 2: Second page returns items disjoint from page 1 (uses same 10_000 cap as test 1)
    [Fact]
    public void ShapeInbox_SecondPage_DisjointFromFirstPage()
    {
        var items = Enumerable.Range(0, 10)
            .Select(i => MakeInboxItem(i, EntryAddedPayload(i, 50_000)))
            .ToList();

        var page1 = InboxResultShaper.ShapeInbox(items, cursor: 0, maxChars: 10_000, JsonOpts);
        var page1Ids = new HashSet<string>(page1.IncludedInboxIds);

        Assert.True(page1.ReturnedCount < 10, "page 1 should not have consumed all items");

        var remaining = items.Skip(page1.ReturnedCount).ToList();
        var page2 = InboxResultShaper.ShapeInbox(remaining, cursor: 0, maxChars: 10_000, JsonOpts);

        // All page 2 ids should be disjoint from page 1
        foreach (var id in page2.IncludedInboxIds)
            Assert.DoesNotContain(id, page1Ids);

        Assert.True(page2.ReturnedCount > 0);
    }

    // Test 3: Small payloads → not truncated, all items returned, no content_truncated key
    [Fact]
    public void ShapeInbox_SmallPayloads_NotTruncatedAllReturned()
    {
        var items = new List<InboxItemDto>
        {
            MakeInboxItem(0, "{\"peer_id\":\"p1\"}", "peer.registered"),
            MakeInboxItem(1, "{\"peer_id\":\"p2\"}", "peer.registered"),
            MakeInboxItem(2, EntryAddedPayload(2, 100)) // short content
        };

        var shape = InboxResultShaper.ShapeInbox(items, cursor: 42, maxChars: 30_000, JsonOpts);

        Assert.False(shape.Truncated);
        Assert.Equal(3, shape.ReturnedCount);
        Assert.Equal(3, shape.IncludedInboxIds.Length);

        // Verify cursor is passed through
        var doc = JsonNode.Parse(shape.Json)!.AsObject();
        Assert.Equal(42, doc["cursor"]!.GetValue<long>());

        // No content_truncated in payloads
        var returnedItems = doc["items"]!.AsArray();
        foreach (var item in returnedItems)
        {
            var payloadStr = item!["event"]!["payload"]!.GetValue<string>();
            var payloadNode = JsonNode.Parse(payloadStr)!.AsObject();
            Assert.Null(payloadNode["content_truncated"]);
        }
    }

    // Test 4: Single item with huge content → ReturnedCount==1, Truncated==false (only 1 item total), content still truncated
    [Fact]
    public void ShapeInbox_SingleHugeItem_ReturnedWithTruncatedContent()
    {
        var items = new List<InboxItemDto>
        {
            MakeInboxItem(0, EntryAddedPayload(0, 50_000))
        };

        var shape = InboxResultShaper.ShapeInbox(items, cursor: 0, maxChars: 30_000, JsonOpts);

        // Only 1 item — always keep ≥1, so Truncated==false (no more items to withhold)
        Assert.Equal(1, shape.ReturnedCount);
        Assert.False(shape.Truncated);

        // But the content inside should be truncated to the preview limit
        var doc = JsonNode.Parse(shape.Json)!.AsObject();
        var returnedItems = doc["items"]!.AsArray();
        var payloadStr = returnedItems[0]!["event"]!["payload"]!.GetValue<string>();
        var payloadNode = JsonNode.Parse(payloadStr)!.AsObject();
        Assert.True(payloadNode["content"]!.GetValue<string>().Length <= InboxResultShaper.PerItemContentPreviewChars);
        Assert.True(payloadNode["content_truncated"]!.GetValue<bool>());
    }

    // Test 5: ShapePoll — large events truncated, cursor behavior.
    // Per-item content is truncated to ~2000 chars, so each event is ~2200 chars.
    // Use maxChars: 10000 to ensure the size cap fires within 10 events.
    [Fact]
    public void ShapePoll_LargeEvents_TruncatedAndCursorIsLastIncluded()
    {
        var events = Enumerable.Range(1, 10)
            .Select(i => MakePeerEvent(i, EntryAddedPayload(i, 50_000)))
            .ToList();

        var shape = InboxResultShaper.ShapePoll(events, brokerCursor: 999, maxChars: 10_000, JsonOpts);

        Assert.True(shape.Truncated);
        Assert.True(shape.Json.Length <= 10_000, $"Json.Length={shape.Json.Length} exceeds 10000");

        // EffectiveCursor should be the last included event's EventId, not brokerCursor
        Assert.NotEqual(999, shape.EffectiveCursor);

        // Parse to find returned_count and verify cursor = last included event id
        var doc = JsonNode.Parse(shape.Json)!.AsObject();
        var returnedCount = doc["returned_count"]!.GetValue<int>();
        var cursor = doc["cursor"]!.GetValue<long>();
        Assert.Equal(shape.EffectiveCursor, cursor);
        Assert.Equal(returnedCount, events.Take(returnedCount).Last().EventId);
    }

    [Fact]
    public void ShapePoll_SmallEvents_NotTruncatedBrokerCursor()
    {
        var events = Enumerable.Range(1, 3)
            .Select(i => MakePeerEvent(i, "{\"peer_id\":\"p1\"}", "peer.registered"))
            .ToList();

        var shape = InboxResultShaper.ShapePoll(events, brokerCursor: 999, maxChars: 30_000, JsonOpts);

        Assert.False(shape.Truncated);
        Assert.Equal(999, shape.EffectiveCursor);

        var doc = JsonNode.Parse(shape.Json)!.AsObject();
        Assert.Equal(999, doc["cursor"]!.GetValue<long>());
        Assert.Equal(3, doc["returned_count"]!.GetValue<int>());
    }

    // Test: Fix 1 regression — hint overhead cannot push the final envelope over maxChars.
    // Strategy: probe the exact size of a 2-item no-hint envelope, then set maxChars to be
    // just above it (+ 40). The hint for "8 more items" is ~96 chars wider in JSON, so the
    // 2-item-with-hint envelope would exceed maxChars by ~56 chars under the old code.
    // The fix must drop further until it actually fits.
    [Fact]
    public void ShapeInbox_TruncatedWithHint_NeverExceedsMaxChars()
    {
        // 10 items with small fixed-size payloads (no per-item truncation fires)
        var items = Enumerable.Range(0, 10)
            .Select(i => MakeInboxItem(i, $"{{\"scratchpad_id\":\"sp1\",\"entry_id\":\"e{i:D4}\",\"content\":\"{new string('a', 100)}\"}}"))
            .ToList();

        // Probe: find the exact JSON length of a 2-item envelope with truncated=false, remaining_hint=null
        var probe2 = InboxResultShaper.ShapeInbox(items.Take(2).ToList(), cursor: 0, maxChars: int.MaxValue, JsonOpts);
        var probe2Len = probe2.Json.Length; // 2-item non-truncated envelope size

        // Set maxChars just 40 chars above the 2-item no-hint size.
        // The hint string "8 more item(s) withheld..." is ~76 chars → JSON field is ~96 chars wider than "null".
        // So 2-item-with-hint would be probe2Len + 92 ≈ probe2Len + 92, exceeding maxChars = probe2Len + 40.
        // Old code: returned that oversized JSON. New code: must drop to 1 item.
        var maxChars = probe2Len + 40;

        var shape = InboxResultShaper.ShapeInbox(items, cursor: 0, maxChars: maxChars, JsonOpts);

        Assert.True(shape.Json.Length <= maxChars,
            $"Json.Length={shape.Json.Length} exceeds maxChars={maxChars} (probe2Len={probe2Len})");
        Assert.True(shape.Truncated);
        Assert.True(shape.ReturnedCount >= 1, "must include at least 1 item");
        Assert.Equal(shape.ReturnedCount, shape.IncludedInboxIds.Length);
    }

    // Same regression test for ShapePoll — hint overhead cannot push final poll envelope over maxChars.
    [Fact]
    public void ShapePoll_TruncatedWithHint_NeverExceedsMaxChars()
    {
        var events = Enumerable.Range(1, 10)
            .Select(i => MakePeerEvent(i, $"{{\"scratchpad_id\":\"sp1\",\"entry_id\":\"e{i:D4}\",\"content\":\"{new string('a', 100)}\"}}"))
            .ToList();

        // Probe 2-event non-truncated envelope size
        var probe2 = InboxResultShaper.ShapePoll(events.Take(2).ToList(), brokerCursor: 999, maxChars: int.MaxValue, JsonOpts);
        var probe2Len = probe2.Json.Length;

        var maxChars = probe2Len + 40;

        var shape = InboxResultShaper.ShapePoll(events, brokerCursor: 999, maxChars: maxChars, JsonOpts);

        Assert.True(shape.Json.Length <= maxChars,
            $"Json.Length={shape.Json.Length} exceeds maxChars={maxChars} (probe2Len={probe2Len})");
        Assert.True(shape.Truncated);
        Assert.True(shape.EffectiveCursor > 0 && shape.EffectiveCursor < 999,
            $"EffectiveCursor={shape.EffectiveCursor} should be a real event id, not brokerCursor");

        var doc = JsonNode.Parse(shape.Json)!.AsObject();
        Assert.Equal(shape.EffectiveCursor, doc["cursor"]!.GetValue<long>());
    }

    // Test 6: TruncatePayloadContent on payload with no "content" key → returns unchanged
    [Fact]
    public void TruncatePayloadContent_NoContentKey_Unchanged()
    {
        var payload = "{\"peer_id\":\"p1\"}";
        var result = InboxResultShaper.TruncatePayloadContent(payload, 2000);
        Assert.Equal(payload, result);
    }

    // Test 7: Surrogate guard — content ending in a high surrogate is safely cut
    [Fact]
    public void TruncatePayloadContent_SurrogateAtCutPoint_CutsCleanly()
    {
        var previewChars = InboxResultShaper.PerItemContentPreviewChars;

        // Build: (previewChars - 1) ASCII chars + emoji (😀 = U+1F600, which is two UTF-16 chars: high surrogate + low surrogate)
        // so char[previewChars-1] is the high surrogate, which must be backed off
        var ascii = new string('a', previewChars - 1);
        var emoji = "😀"; // 2 UTF-16 chars
        var content = ascii + emoji + new string('b', 100); // total > previewChars

        var payload = $"{{\"content\":\"{content}\"}}";
        var result = InboxResultShaper.TruncatePayloadContent(payload, previewChars);

        // Result must parse cleanly
        var node = JsonNode.Parse(result);
        Assert.NotNull(node);
        var resultContent = node!["content"]!.GetValue<string>();

        // The cut should NOT end in a lone high surrogate
        if (resultContent.Length > 0)
        {
            var lastChar = resultContent[resultContent.Length - 1];
            Assert.False(char.IsHighSurrogate(lastChar),
                $"Result ended in a high surrogate (\\u{(int)lastChar:X4}) — surrogate guard failed");
        }

        // Content should be shorter than the original (truncation happened)
        Assert.True(resultContent.Length < content.Length);
        // content_truncated should be set
        Assert.True(node["content_truncated"]!.GetValue<bool>());
    }
}
