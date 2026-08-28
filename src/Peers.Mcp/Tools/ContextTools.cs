using System.ComponentModel;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed partial class ContextTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
    private static readonly JsonSerializerOptions JsonOptsIndented = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower, WriteIndented = true };

    [McpServerTool(Name = "context_export"), Description(
        "Export the current session's context as a peers-context:v1 markdown packet, suitable for pasting into Codex or another tool during a handoff.\n" +
        "Includes (toggleable): active peers, channels, artifact list, optional task description, and free-form notes.\n" +
        "Returns: a markdown string starting with '<!-- peers-context:v1 -->' and a single ```json block. Recommended Claude<->Codex handoff format.")]
    public static async Task<string> Export(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Include active peer list")] bool include_peers = true,
        [Description("Include channel list")] bool include_channels = true,
        [Description("Include artifact list")] bool include_artifacts = true,
        [Description("Custom task description to include")] string? task = null,
        [Description("Custom notes to include (JSON array of strings)")] string? notes = null)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory);
        try
        {
            var doc = new Dictionary<string, object?>();

            doc["task"] = new { title = task ?? "No task specified" };
            doc["session"] = new { tool = state.Tool ?? "unknown", peer_id = state.PeerId };

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

            if (include_channels)
                doc["channels"] = (await client.ListChannelsAsync()).Channels.Select(c => new
                {
                    id = c.ChannelId,
                    name = c.Name,
                    repo_root = c.RepoRoot,
                    message_count = c.MessageCount
                }).ToList();

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
        // A broker failure is a tool error, not an exported packet: the SDK turns this into isError=true.
        catch (BrokerApiException ex) { throw new McpException(ex.Message, ex); }
    }

    [McpServerTool(Name = "context_import"), Description(
        "Parse a peers-context:v1 markdown packet (typically produced by context_export on a peer) into a typed JSON object: task, session, peers, channels, artifacts, notes, changes.\n" +
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

            if (sections.TryGetValue("task", out var taskLines))
            {
                var titleLine = taskLines.FirstOrDefault(l => l.StartsWith("title:"));
                if (titleLine is not null)
                    result["task"] = titleLine["title:".Length..].Trim().Trim('"');
            }

            if (sections.TryGetValue("session", out var sessionLines))
                result["session"] = sessionLines;

            foreach (var key in new[] { "peers", "channels", "artifacts", "notes", "changes" })
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
