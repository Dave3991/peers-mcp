using System.ComponentModel;
using System.Text.Json;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed partial class ContextTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
    private static readonly JsonSerializerOptions JsonOptsIndented = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower, WriteIndented = true };

    [McpServerTool(Name = "context_export"), Description(
        "Export the current session's context as a peers-context:v1 markdown packet, suitable for pasting into Codex or another tool during a handoff.\n" +
        "Includes (toggleable): active peers, open scratchpads, artifact list, optional task description, and free-form notes.\n" +
        "Returns: a markdown string starting with '<!-- peers-context:v1 -->' and a single ```json block. Recommended Claude<->Codex handoff format.")]
    public static async Task<string> Export(
        BrokerClient client,
        SessionState state,
        [Description("Include active peer list")] bool include_peers = true,
        [Description("Include open scratchpads")] bool include_scratchpads = true,
        [Description("Include artifact list")] bool include_artifacts = true,
        [Description("Custom task description to include")] string? task = null,
        [Description("Custom notes to include (JSON array of strings)")] string? notes = null,
        [Description("Include the newest ~3 entries (500-char previews) of each open scratchpad. Note: drawn from a bounded window of up to 200 entries per pad — on threads longer than 200 entries the absolute latest may be omitted; use scratchpad_read for the full tail.")] bool include_scratchpad_entries = false)
    {
        try { return await ExportInner(client, state, include_peers, include_scratchpads, include_artifacts, task, notes, include_scratchpad_entries); }
        catch (BrokerApiException ex) { return JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode }, JsonOpts); }
    }

    private static async Task<string> ExportInner(
        BrokerClient client,
        SessionState state,
        bool include_peers,
        bool include_scratchpads,
        bool include_artifacts,
        string? task,
        string? notes,
        bool include_scratchpad_entries = false)
    {
        var doc = new Dictionary<string, object?>();

        doc["task"] = new { title = task ?? "No task specified" };
        doc["session"] = new { tool = state.Tool ?? "unknown", peer_id = state.PeerId ?? "not-registered" };

        if (include_peers)
        {
            var peers = await client.ListPeersAsync();
            doc["peers"] = peers.Select(p => new
            {
                peer_id = p.PeerId,
                tool = p.Tool,
                repo_root = p.RepoRoot,
                branch = p.Branch,
                summary = p.Summary
            }).ToList();
        }

        if (include_scratchpads)
        {
            var pads = await client.ListScratchpadsAsync(status: "open");
            var padList = new List<object>();
            foreach (var pad in pads)
            {
                if (include_scratchpad_entries)
                {
                    var read = await client.ReadScratchpadAsync(pad.ScratchpadId, 200, 0);
                    var entries = (read?.Entries ?? [])
                        .OrderByDescending(e => e.CreatedAt)
                        .Take(3)
                        .Select(e => new
                        {
                            author_peer_id = e.AuthorPeerId,
                            created_at = e.CreatedAt,
                            preview = e.Content.Length <= 500 ? e.Content : e.Content[..500]
                        })
                        .ToList();
                    padList.Add(new { id = pad.ScratchpadId, type = pad.Type, title = pad.Title, status = pad.Status, entries });
                }
                else
                {
                    padList.Add(new { id = pad.ScratchpadId, type = pad.Type, title = pad.Title, status = pad.Status });
                }
            }
            doc["scratchpads"] = padList;
        }

        if (include_artifacts)
        {
            var arts = await client.ListArtifactsAsync();
            doc["artifacts"] = arts.Select(a => new
            {
                id = a.ArtifactId,
                @namespace = a.Namespace,
                name = a.Name,
                version = a.Version,
                type = a.Type
            }).ToList();
        }

        if (notes is not null)
        {
            try
            {
                var noteList = JsonSerializer.Deserialize<List<string>>(notes);
                doc["notes"] = noteList ?? (object)new[] { notes };
            }
            catch (JsonException)
            {
                doc["notes"] = new[] { notes };
            }
        }

        var json = JsonSerializer.Serialize(doc, JsonOptsIndented);
        return $"<!-- peers-context:v1 -->\n```json\n{json}\n```\n";
    }

    [McpServerTool(Name = "context_import"), Description(
        "Parse a peers-context:v1 markdown packet (typically produced by context_export on a peer) into a typed JSON object: task, session, peers, scratchpads, artifacts, notes, changes.\n" +
        "Accepts a ```json block (current format) or the legacy ```yaml block.\n" +
        "If no recognizable peers-context:v1 block is found, returns { raw, warning } with the original markdown unparsed.\n" +
        "Returns: a JSON object keyed by section name.")]
    public static Task<string> Import(
        [Description("Structured markdown context packet")] string markdown)
    {
        // Try to parse the current JSON format first
        var markerIdx = markdown.IndexOf("<!-- peers-context:v1 -->", StringComparison.Ordinal);
        if (markerIdx >= 0)
        {
            var braceIdx = markdown.IndexOf('{', markerIdx);
            if (braceIdx >= 0)
            {
                var slice = markdown[braceIdx..];
                var bytes = System.Text.Encoding.UTF8.GetBytes(slice);
                try
                {
                    var reader = new System.Text.Json.Utf8JsonReader(bytes);
                    reader.Read();   // StartObject
                    reader.Skip();   // advance to matching EndObject
                    var consumed = checked((int)reader.BytesConsumed);
                    using var doc = JsonDocument.Parse(new ReadOnlyMemory<byte>(bytes, 0, consumed));
                    return Task.FromResult(JsonSerializer.Serialize(doc.RootElement, JsonOpts));
                }
                catch (JsonException) { /* fall through to yaml legacy */ }
            }
        }

        // Legacy YAML fallback
        var result = new Dictionary<string, object?>();
        var yamlMatch = YamlBlockRegex().Match(markdown);
        if (yamlMatch.Success)
        {
            var yaml = yamlMatch.Groups[1].Value;

            // Simple line-based YAML parsing for the flat structure we use
            string? currentSection = null;
            var sections = new Dictionary<string, List<string>>();

            foreach (var line in yaml.Split('\n'))
            {
                var trimmed = line.TrimEnd();
                if (string.IsNullOrWhiteSpace(trimmed)) continue;

                if (!trimmed.StartsWith(' ') && trimmed.EndsWith(':'))
                {
                    currentSection = trimmed.TrimEnd(':');
                    sections[currentSection] = [];
                }
                else if (currentSection is not null)
                {
                    sections[currentSection].Add(trimmed.TrimStart());
                }
            }

            // Extract task title
            if (sections.TryGetValue("task", out var taskLines))
            {
                var titleLine = taskLines.FirstOrDefault(l => l.StartsWith("title:"));
                if (titleLine is not null)
                    result["task"] = titleLine["title:".Length..].Trim().Trim('"');
            }

            // Extract session info
            if (sections.TryGetValue("session", out var sessionLines))
                result["session"] = sessionLines;

            // Pass through other sections
            foreach (var key in new[] { "peers", "scratchpads", "artifacts", "notes", "changes" })
                if (sections.TryGetValue(key, out var lines))
                    result[key] = lines;
        }
        else
        {
            result["raw"] = markdown;
            result["warning"] = "No peers-context:v1 YAML block found. Returning raw content.";
        }

        return Task.FromResult(JsonSerializer.Serialize(result, JsonOpts));
    }

    [GeneratedRegex(@"```yaml\s*\n([\s\S]*?)```", RegexOptions.Multiline)]
    private static partial Regex YamlBlockRegex();
}
