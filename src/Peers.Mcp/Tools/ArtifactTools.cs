using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class ArtifactTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "artifact_publish"), Description("Publish a versioned artifact (types, OpenAPI spec, diff, etc). Auto-increments version per namespace+name.")]
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
        [Description("JSON array of tags")] string? tags = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        try
        {
            var resp = await client.PublishArtifactAsync(new PublishArtifactRequest
            {
                Namespace = @namespace, Name = name, Type = type, Content = content,
                RepoRoot = repo_root, Branch = branch, CommitSha = commit_sha,
                ContentType = content_type, Tags = tags, PublishedByPeerId = state.PeerId,
            });
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "artifact_get"), Description("Get an artifact by ID or by namespace+name (returns latest version).")]
    public static async Task<string> Get(
        BrokerClient client,
        [Description("Artifact ID (use this OR namespace+name)")] string? artifact_id = null,
        [Description("Artifact namespace")] string? @namespace = null,
        [Description("Artifact name")] string? name = null)
    {
        try
        {
            var result = artifact_id is not null
                ? await client.GetArtifactByIdAsync(artifact_id)
                : (@namespace is not null && name is not null)
                    ? await client.GetArtifactByNameAsync(@namespace, name)
                    : null;

            if (result is null)
                return """{"error":"Artifact not found. Provide artifact_id or both namespace and name."}""";

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

    [McpServerTool(Name = "artifact_list"), Description("List artifacts. Returns metadata only (no content). Filter by type, repo, or namespace.")]
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
