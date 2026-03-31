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
        var resp = await client.PublishArtifactAsync(new PublishArtifactRequest
        {
            Namespace = @namespace, Name = name, Type = type, Content = content,
            RepoRoot = repo_root, Branch = branch, CommitSha = commit_sha,
            ContentType = content_type, Tags = tags, PublishedByPeerId = state.PeerId,
        });
        return JsonSerializer.Serialize(resp, JsonOpts);
    }

    [McpServerTool(Name = "artifact_get"), Description("Get an artifact by ID or by namespace+name (returns latest version).")]
    public static async Task<string> Get(
        BrokerClient client,
        [Description("Artifact ID (use this OR namespace+name)")] string? artifact_id = null,
        [Description("Artifact namespace")] string? @namespace = null,
        [Description("Artifact name")] string? name = null)
    {
        var result = artifact_id is not null
            ? await client.GetArtifactByIdAsync(artifact_id)
            : (@namespace is not null && name is not null)
                ? await client.GetArtifactByNameAsync(@namespace, name)
                : null;

        return result is not null
            ? JsonSerializer.Serialize(result, JsonOpts)
            : """{"error":"Artifact not found. Provide artifact_id or both namespace and name."}""";
    }

    [McpServerTool(Name = "artifact_list"), Description("List artifacts. Returns metadata only (no content). Filter by type, repo, or namespace.")]
    public static async Task<string> List(
        BrokerClient client,
        [Description("Filter by type")] string? type = null,
        [Description("Filter by repo root")] string? repo_root = null,
        [Description("Filter by namespace")] string? @namespace = null)
    {
        var list = await client.ListArtifactsAsync(type, repo_root, @namespace);
        return JsonSerializer.Serialize(list, JsonOpts);
    }
}
