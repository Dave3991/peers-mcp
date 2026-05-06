using System.ComponentModel;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed partial class ContextTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "context_export"), Description(
        "Export the current session's context as a peers-context:v1 markdown packet, suitable for pasting into Codex or another tool during a handoff.\n" +
        "Includes (toggleable): active peers, open scratchpads, artifact list, optional task description, and free-form notes.\n" +
        "Returns: a markdown string starting with '<!-- peers-context:v1 -->' and a single ```yaml block. Recommended Claude<->Codex handoff format.")]
    public static async Task<string> Export(
        BrokerClient client,
        SessionState state,
        [Description("Include active peer list")] bool include_peers = true,
        [Description("Include open scratchpads")] bool include_scratchpads = true,
        [Description("Include artifact list")] bool include_artifacts = true,
        [Description("Custom task description to include")] string? task = null,
        [Description("Custom notes to include (JSON array of strings)")] string? notes = null)
    {
        try { return await ExportInner(client, state, include_peers, include_scratchpads, include_artifacts, task, notes); }
        catch (BrokerApiException ex) { return JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode }, JsonOpts); }
    }

    private static async Task<string> ExportInner(
        BrokerClient client,
        SessionState state,
        bool include_peers,
        bool include_scratchpads,
        bool include_artifacts,
        string? task,
        string? notes)
    {
        var sb = new StringBuilder();
        sb.AppendLine("<!-- peers-context:v1 -->");
        sb.AppendLine("```yaml");
        sb.AppendLine("task:");
        sb.AppendLine($"  title: \"{task ?? "No task specified"}\"");
        sb.AppendLine("session:");
        sb.AppendLine($"  tool: {state.Tool ?? "unknown"}");
        sb.AppendLine($"  peer_id: {state.PeerId ?? "not-registered"}");

        if (include_peers)
        {
            var peers = await client.ListPeersAsync();
            sb.AppendLine("peers:");
            foreach (var p in peers)
            {
                sb.AppendLine($"  - peer_id: {p.PeerId}");
                sb.AppendLine($"    tool: {p.Tool}");
                sb.AppendLine($"    repo_root: {p.RepoRoot ?? "unknown"}");
                sb.AppendLine($"    branch: {p.Branch ?? "unknown"}");
                sb.AppendLine($"    summary: \"{p.Summary ?? ""}\"");
            }
        }

        if (include_scratchpads)
        {
            var pads = await client.ListScratchpadsAsync(status: "open");
            sb.AppendLine("scratchpads:");
            foreach (var pad in pads)
            {
                sb.AppendLine($"  - id: {pad.ScratchpadId}");
                sb.AppendLine($"    type: {pad.Type}");
                sb.AppendLine($"    title: \"{pad.Title}\"");
                sb.AppendLine($"    status: {pad.Status}");
            }
        }

        if (include_artifacts)
        {
            var arts = await client.ListArtifactsAsync();
            sb.AppendLine("artifacts:");
            foreach (var a in arts)
            {
                sb.AppendLine($"  - id: {a.ArtifactId}");
                sb.AppendLine($"    namespace: {a.Namespace}");
                sb.AppendLine($"    name: {a.Name}");
                sb.AppendLine($"    version: {a.Version}");
                sb.AppendLine($"    type: {a.Type}");
            }
        }

        if (notes is not null)
        {
            sb.AppendLine("notes:");
            try
            {
                var noteList = JsonSerializer.Deserialize<List<string>>(notes);
                if (noteList is not null)
                    foreach (var n in noteList)
                        sb.AppendLine($"  - \"{n}\"");
            }
            catch { sb.AppendLine($"  - \"{notes}\""); }
        }

        sb.AppendLine("```");
        return sb.ToString();
    }

    [McpServerTool(Name = "context_import"), Description(
        "Parse a peers-context:v1 markdown packet (typically produced by context_export on a peer) into a typed JSON object: task, session, peers, scratchpads, artifacts, notes, changes.\n" +
        "If no peers-context:v1 YAML block is found, returns { raw, warning } with the original markdown unparsed.\n" +
        "Returns: a JSON object keyed by section name.")]
    public static Task<string> Import(
        [Description("Structured markdown context packet")] string markdown)
    {
        var result = new Dictionary<string, object?>();

        // Extract YAML block
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
