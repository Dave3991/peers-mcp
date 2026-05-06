using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class ArtifactTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "artifact_publish"), Description(
        "Publish a versioned artifact (typescript-types, openapi-spec, diff, summary, schema, ...) for other peers to consume.\n" +
        "When to use: instead of pasting large blobs into a scratchpad — share the artifact_id and let recipients fetch it.\n" +
        "Versioning: auto-increments per (namespace, name). Content is hashed; identical content reuses the prior version (dedup). Limit 1 MB per artifact.\n" +
        "Side effect: emits an event to peers. Returns: { artifact_id, namespace, name, version, content_hash, ... }.")]
    public static async Task<string> Publish(
        BrokerClient client,
        SessionState state,
        [Description("Artifact namespace (e.g. repo name or project)")] string @namespace,
        [Description("Artifact name (e.g. 'auth-api-contract')")] string name,
        [Description("Artifact type: openapi-spec, typescript-types, diff, summary, test-report, schema, config, other")] string type,
        [Description("Artifact content")] string content,
        [Description("Repository root")] string? repo_root = null,
        [Description("Git branch")] string? branch = null,
        [Description("Commit SHA")] string? commit_sha = null,
        [Description("Content MIME type")] string? content_type = null,
        [Description("Tags to attach to this artifact")] string[]? tags = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        try
        {
            var resp = await client.PublishArtifactAsync(new PublishArtifactRequest
            {
                Namespace = @namespace, Name = name, Type = type, Content = content,
                RepoRoot = repo_root, Branch = branch, CommitSha = commit_sha,
                ContentType = content_type,
                Tags = tags is null ? null : JsonSerializer.Serialize(tags),
                PublishedByPeerId = state.PeerId,
            });
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "artifact_get"), Description(
        "Fetch an artifact's full content. Look up by artifact_id (specific version) OR by namespace+name (latest version).\n" +
        "Auto-truncates: if the response exceeds the 30 KB tool-result cap (PEERS_MAX_TOOL_RESULT_CHARS), only the metadata plus a content_preview (first ~15 KB) is returned with { truncated: true }.\n" +
        "Returns: full artifact { artifact_id, namespace, name, version, type, content, content_hash, content_bytes } or the truncated form above.")]
    public static async Task<string> Get(
        BrokerClient client,
        [Description("Artifact ID for a specific version (mutually exclusive with namespace+name)")] string? artifact_id = null,
        [Description("Artifact namespace (must be paired with name; returns latest version)")] string? @namespace = null,
        [Description("Artifact name (must be paired with namespace; returns latest version)")] string? name = null)
    {
        if (artifact_id is null && (@namespace is null || name is null))
            return """{"error":"Missing parameters. Provide either artifact_id, or both namespace and name."}""";
        if (artifact_id is not null && (@namespace is not null || name is not null))
            return """{"error":"Conflicting parameters. Provide either artifact_id or namespace+name, not both."}""";

        try
        {
            var result = artifact_id is not null
                ? await client.GetArtifactByIdAsync(artifact_id)
                : await client.GetArtifactByNameAsync(@namespace!, name!);

            if (result is null)
                return """{"error":"Artifact not found."}""";

            var serialized = JsonSerializer.Serialize(result, JsonOpts);
            if (serialized.Length > Limits.MaxToolResultChars)
            {
                var truncated = new
                {
                    artifact_id = result.ArtifactId,
                    @namespace = result.Namespace,
                    name = result.Name,
                    version = result.Version,
                    type = result.Type,
                    content_hash = result.ContentHash,
                    content_bytes = result.ContentBytes,
                    content_preview = result.Content[..Math.Min(Limits.MaxToolResultChars / 2, result.Content.Length)],
                    truncated = true,
                    message = $"Content preview: first {Math.Min(Limits.MaxToolResultChars / 2, result.Content.Length)} of {result.Content.Length} chars. Use artifact_get with the artifact_id to fetch specific sections.",
                };
                return JsonSerializer.Serialize(truncated, JsonOpts);
            }
            return serialized;
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "artifact_list"), Description(
        "List artifact metadata (no content) matching the given filters. Use to discover what's published before fetching with artifact_get.\n" +
        "Filter by type, repo_root, or namespace. Returns: array of { artifact_id, namespace, name, version, type, content_bytes, content_hash, ... }.")]
    public static async Task<string> List(
        BrokerClient client,
        [Description("Filter by type")] string? type = null,
        [Description("Filter by repo root")] string? repo_root = null,
        [Description("Filter by namespace")] string? @namespace = null)
    {
        try
        {
            var list = await client.ListArtifactsAsync(type, repo_root, @namespace);
            return JsonSerializer.Serialize(list, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    private static string BrokerError(BrokerApiException ex) =>
        JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode }, JsonOpts);
}
