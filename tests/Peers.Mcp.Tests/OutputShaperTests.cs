using System.Text.Json;
using System.Text.Json.Nodes;
using Peers.Mcp;
using Peers.Shared.Contracts;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Mcp.Tests;

public class OutputShaperTests
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    // ─── Helpers ────────────────────────────────────────────────────────────────

    private static Scratchpad MakeScratchpad() => new()
    {
        ScratchpadId = "sp-test",
        Type = "discussion",
        Title = "Test pad",
        Status = "open",
        CreatedByPeerId = "peer-1",
        CreatedAt = DateTime.UtcNow,
    };

    private static ScratchpadEntry MakeEntry(int i, int contentLength) => new()
    {
        EntryId = $"e{i}",
        ScratchpadId = "sp-test",
        AuthorPeerId = "peer-1",
        Content = new string('x', contentLength),
        ContentBytes = contentLength,
        CreatedAt = DateTime.UtcNow,
    };

    private static ReadScratchpadResponse MakeResponse(int entryCount, int contentLength) => new()
    {
        Scratchpad = MakeScratchpad(),
        Entries = Enumerable.Range(0, entryCount)
            .Select(i => MakeEntry(i, contentLength))
            .ToList(),
    };

    private static Artifact MakeArtifact(string content) => new()
    {
        ArtifactId = "art-1",
        Namespace = "ns",
        Name = "test-artifact",
        Version = 1,
        Type = "summary",
        Content = content,
        ContentHash = "abc123",
        ContentBytes = content.Length,
        PublishedByPeerId = "peer-1",
        CreatedAt = DateTime.UtcNow,
    };

    // ─── Scratchpad Tests ────────────────────────────────────────────────────────

    // Test 1: 10 entries × ~5000-char content, small maxChars → truncated, result ≤ maxChars
    [Fact]
    public void ShapeScratchpadRead_LargeEntries_TruncatesWithinBudget()
    {
        var resp = MakeResponse(10, 5_000);
        var maxChars = 20_000;

        var json = OutputShaper.ShapeScratchpadRead(resp, offset: 0, maxChars, JsonOpts);

        Assert.True(json.Length <= maxChars, $"json.Length={json.Length} exceeds maxChars={maxChars}");

        var doc = JsonNode.Parse(json)!.AsObject();
        Assert.True(doc["truncated"]!.GetValue<bool>(), "truncated should be true");
        var includedCount = doc["included_count"]!.GetValue<int>();
        Assert.True(includedCount < 10, $"included_count={includedCount} should be < 10");
        var entries = doc["entries"]!.AsArray();
        Assert.Equal(includedCount, entries.Count);
        var nextOffset = doc["next_offset"]!.GetValue<int>();
        Assert.Equal(0 + includedCount, nextOffset);
    }

    // Test 2: entries that all fit → no truncated field, unchanged shape
    [Fact]
    public void ShapeScratchpadRead_SmallEntries_FullShapeNoTruncated()
    {
        var resp = MakeResponse(3, 50);
        var maxChars = 30_000;

        var json = OutputShaper.ShapeScratchpadRead(resp, offset: 0, maxChars, JsonOpts);

        var doc = JsonNode.Parse(json)!.AsObject();
        Assert.Null(doc["truncated"]);
        Assert.Null(doc["included_count"]);
        Assert.Null(doc["next_offset"]);
        Assert.Equal(3, doc["entries"]!.AsArray().Count);
    }

    // Test 3: one entry alone > maxChars → exactly 1 entry, included_count==1
    [Fact]
    public void ShapeScratchpadRead_SingleHugeEntry_IncludesItAnyway()
    {
        var resp = MakeResponse(5, 50_000);
        var maxChars = 100; // tiny — even 1 entry won't fit, but must be kept

        var json = OutputShaper.ShapeScratchpadRead(resp, offset: 0, maxChars, JsonOpts);

        var doc = JsonNode.Parse(json)!.AsObject();
        Assert.True(doc["truncated"]!.GetValue<bool>());
        Assert.Equal(1, doc["included_count"]!.GetValue<int>());
        Assert.Single(doc["entries"]!.AsArray());
    }

    // ─── Artifact Tests ──────────────────────────────────────────────────────────

    // Test 4: content > cap, no range params → offset==0, next_offset>0, total_chars, truncated, ≤ cap
    [Fact]
    public void ShapeArtifactGet_LargeContent_TruncatesWithinCap()
    {
        var content = new string('a', 60_000);
        var artifact = MakeArtifact(content);
        var cap = 30_000;

        var json = OutputShaper.ShapeArtifactGet(artifact, offset: null, maxChars: null, cap, JsonOpts);

        Assert.True(json.Length <= cap, $"json.Length={json.Length} exceeds cap={cap}");

        var doc = JsonNode.Parse(json)!.AsObject();
        Assert.Equal(0, doc["offset"]!.GetValue<int>());
        Assert.True(doc["next_offset"]!.GetValue<int>() > 0);
        Assert.Equal(content.Length, doc["total_chars"]!.GetValue<int>());
        Assert.True(doc["truncated"]!.GetValue<bool>());
    }

    // Test 5: paging terminates; concatenated chunks reconstruct the full content
    [Fact]
    public void ShapeArtifactGet_Paging_ReconstructsFullContent()
    {
        var content = new string('b', 80_000);
        var artifact = MakeArtifact(content);
        var cap = 30_000;
        var maxCalls = 20; // safety upper bound

        var chunks = new System.Text.StringBuilder();
        int? nextOffset = null;
        int calls = 0;

        do
        {
            var json = OutputShaper.ShapeArtifactGet(artifact, offset: nextOffset, maxChars: null, cap, JsonOpts);
            Assert.True(json.Length <= cap, $"Chunk json.Length={json.Length} > cap={cap}");

            var doc = JsonNode.Parse(json)!.AsObject();
            chunks.Append(doc["content"]!.GetValue<string>());

            var nxt = doc["next_offset"];
            nextOffset = nxt?.GetValue<int?>();
            calls++;

            Assert.True(calls <= maxCalls, "Paging did not terminate within expected call count");
        }
        while (nextOffset != null);

        Assert.Equal(content, chunks.ToString());
    }

    // Test 6: small content, no range → full shape unchanged
    [Fact]
    public void ShapeArtifactGet_SmallContent_FullShapeUnchanged()
    {
        var content = "hello world";
        var artifact = MakeArtifact(content);
        var cap = 30_000;

        var json = OutputShaper.ShapeArtifactGet(artifact, offset: null, maxChars: null, cap, JsonOpts);

        var doc = JsonNode.Parse(json)!.AsObject();
        Assert.Equal(content, doc["content"]!.GetValue<string>());
        Assert.Null(doc["next_offset"]);
        Assert.Null(doc["truncated"]); // full shape: no paging fields
        Assert.Null(doc["total_chars"]);
    }

    // Test 7: explicit max_chars on larger content → returns ~that many chars, ≤ cap, next_offset set
    [Fact]
    public void ShapeArtifactGet_ExplicitMaxChars_ReturnsApproximately()
    {
        var content = new string('c', 10_000);
        var artifact = MakeArtifact(content);
        var cap = 30_000;

        var json = OutputShaper.ShapeArtifactGet(artifact, offset: 0, maxChars: 100, cap, JsonOpts);

        Assert.True(json.Length <= cap);
        var doc = JsonNode.Parse(json)!.AsObject();
        var returnedContent = doc["content"]!.GetValue<string>();
        Assert.True(returnedContent.Length <= 100, $"content.Length={returnedContent.Length} > 100");
        Assert.True(returnedContent.Length >= 1);
        var nextOffset = doc["next_offset"]!.GetValue<int>();
        Assert.Equal(0 + returnedContent.Length, nextOffset);
    }

    // Test 8: forward-progress — max_chars=0 still returns ≥1 char and terminates paging
    [Fact]
    public void ShapeArtifactGet_MaxCharsZero_ForwardProgress()
    {
        var content = new string('d', 500);
        var artifact = MakeArtifact(content);
        var cap = 30_000;

        // Single call with max_chars=0: must return ≥1 char and advance
        var json = OutputShaper.ShapeArtifactGet(artifact, offset: 0, maxChars: 0, cap, JsonOpts);
        var doc = JsonNode.Parse(json)!.AsObject();
        var returnedContent = doc["content"]!.GetValue<string>();
        Assert.True(returnedContent.Length >= 1, "Must return at least 1 char even with max_chars=0");
        var nextOffset = doc["next_offset"]!.GetValue<int?>();
        Assert.NotNull(nextOffset);
        Assert.True(nextOffset > 0, "next_offset must be strictly > 0 (forward progress)");

        // Full paging loop with max_chars=0 still terminates and reconstructs
        var chunks = new System.Text.StringBuilder();
        int? offset2 = null;
        int calls = 0;
        const int maxCalls = 1000;

        do
        {
            var j = OutputShaper.ShapeArtifactGet(artifact, offset: offset2, maxChars: 0, cap, JsonOpts);
            var d = JsonNode.Parse(j)!.AsObject();
            chunks.Append(d["content"]!.GetValue<string>());
            offset2 = d["next_offset"]?.GetValue<int?>();
            calls++;
            Assert.True(calls <= maxCalls, "Paging with max_chars=0 did not terminate");
        }
        while (offset2 != null);

        Assert.Equal(content, chunks.ToString());
    }

    // Test 9: surrogate-safe — emoji at cut boundary doesn't produce a lone surrogate
    [Fact]
    public void ShapeArtifactGet_SurrogateAtBoundary_ParsesCleanly()
    {
        // 😀 = U+1F600 = two UTF-16 code units (high surrogate + low surrogate)
        // Build content so the cut boundary lands at the high surrogate of an emoji.
        var maxChars = 100; // small chunk
        // Pad up to (maxChars - 1) ASCII chars then an emoji, so high surrogate is at index maxChars-1
        var pad = new string('a', maxChars - 1);
        var emoji = "😀"; // 2 UTF-16 chars
        var content = pad + emoji + new string('b', 200);
        var artifact = MakeArtifact(content);
        var cap = 30_000;

        var json = OutputShaper.ShapeArtifactGet(artifact, offset: 0, maxChars, cap, JsonOpts);

        // Must parse without exception
        var doc = JsonNode.Parse(json);
        Assert.NotNull(doc);

        var returnedContent = doc!["content"]!.GetValue<string>();

        // Must not end with a lone high surrogate
        if (returnedContent.Length > 0)
        {
            var last = returnedContent[returnedContent.Length - 1];
            Assert.False(char.IsHighSurrogate(last),
                $"Result ended in high surrogate \\u{(int)last:X4} — surrogate guard failed");
        }
    }
}
