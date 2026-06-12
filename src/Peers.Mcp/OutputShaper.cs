using System.Text.Json;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Mcp;

public static class OutputShaper
{
    public const int DefaultArtifactChunkChars = 24_000;

    // Returns a cut length <= requested that does not split a UTF-16 surrogate pair at the boundary.
    private static int SurrogateSafeLen(string s, int start, int requested)
    {
        var len = Math.Min(requested, s.Length - start);
        if (len > 0 && char.IsHighSurrogate(s[start + len - 1])) len--;
        return len;
    }

    /// <summary>
    /// Shape a <see cref="ReadScratchpadResponse"/> to fit within <paramref name="maxChars"/>.
    /// Returns the full serialization unchanged when it already fits (back-compat).
    /// On overflow, greedily includes entries and adds truncation metadata.
    /// Always includes at least 1 entry whole, even if it alone exceeds maxChars.
    /// </summary>
    public static string ShapeScratchpadRead(
        ReadScratchpadResponse resp,
        int offset,
        int maxChars,
        JsonSerializerOptions opts)
    {
        // Fast path: full response fits — return as-is, no new fields.
        var full = JsonSerializer.Serialize(new { scratchpad = resp.Scratchpad, entries = resp.Entries }, opts);
        if (full.Length <= maxChars)
            return full;

        // Greedy partial: accumulate entries, checking size after each addition.
        var included = new List<ScratchpadEntry>();

        foreach (var entry in resp.Entries)
        {
            included.Add(entry);

            // Build candidate envelope to check size.
            var candidate = JsonSerializer.Serialize(new
            {
                scratchpad = resp.Scratchpad,
                entries = included,
                truncated = true,
                included_count = included.Count,
                page_entry_count = resp.Entries.Count,
                next_offset = offset + included.Count,
            }, opts);

            if (candidate.Length > maxChars && included.Count > 1)
            {
                // Over limit with more than one entry — remove last and stop.
                included.RemoveAt(included.Count - 1);
                break;
            }
            // else: either still fits, or it's the first entry (must keep ≥1).
        }

        // Re-serialize the settled result.
        return JsonSerializer.Serialize(new
        {
            scratchpad = resp.Scratchpad,
            entries = included,
            truncated = true,
            included_count = included.Count,
            page_entry_count = resp.Entries.Count,
            next_offset = offset + included.Count,
        }, opts);
    }

    /// <summary>
    /// Shape an <see cref="Artifact"/> for the artifact_get tool.
    /// Returns the full serialization unchanged when no range is requested and it fits (back-compat).
    /// On overflow or explicit range, returns a paged envelope with offset/next_offset/total_chars.
    /// </summary>
    public static string ShapeArtifactGet(
        Artifact result,
        int? offset,
        int? maxChars,
        int cap,
        JsonSerializerOptions opts)
    {
        bool rangeRequested = offset is not null || maxChars is not null;

        // Build full shape to check size.
        var fullObj = new
        {
            artifact_id = result.ArtifactId,
            @namespace = result.Namespace,
            name = result.Name,
            version = result.Version,
            type = result.Type,
            content_hash = result.ContentHash,
            content_bytes = result.ContentBytes,
            content = result.Content,
        };

        if (!rangeRequested)
        {
            var fullJson = JsonSerializer.Serialize(fullObj, opts);
            if (fullJson.Length <= cap)
                return fullJson;
        }

        // Range requested OR overflow: paginate.
        var start = Math.Clamp(offset ?? 0, 0, result.Content.Length);

        // Forward-progress floor: budget is at least 1.
        var budget = Math.Max(1, maxChars ?? DefaultArtifactChunkChars);

        var len = SurrogateSafeLen(result.Content, start, budget);

        // Guarantee forward progress when start < Length.
        if (len == 0 && start < result.Content.Length)
            len = 1;

        // Build and shrink loop to fit within cap (bounded ~8 passes).
        string envelope;
        int passes = 0;
        while (true)
        {
            var chunk = result.Content.Substring(start, len);
            var nextOffset = (start + chunk.Length >= result.Content.Length)
                ? (int?)null
                : start + chunk.Length;
            var truncated = nextOffset != null;
            var message = truncated
                ? $"Returned chars {start}-{start + chunk.Length} of {result.Content.Length}. Call artifact_get again with offset={nextOffset} for more."
                : (string?)null;

            envelope = JsonSerializer.Serialize(new
            {
                artifact_id = result.ArtifactId,
                @namespace = result.Namespace,
                name = result.Name,
                version = result.Version,
                type = result.Type,
                content_hash = result.ContentHash,
                content_bytes = result.ContentBytes,
                total_chars = result.Content.Length,
                offset = start,
                next_offset = nextOffset,
                truncated,
                content = chunk,
                message,
            }, opts);

            if (envelope.Length <= cap || passes >= 8)
                break;

            // Shrink len, keeping ≥1 when start < Length.
            len = Math.Max(start < result.Content.Length ? 1 : 0, (int)(len * 0.8));
            // Reapply surrogate-safety after shrink.
            len = SurrogateSafeLen(result.Content, start, len);
            if (len == 0 && start < result.Content.Length)
                len = 1;

            passes++;
        }

        return envelope;
    }
}
