using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Peers.Mcp;
using Peers.Mcp.Lifecycle;
using Microsoft.Extensions.Logging;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class ArtifactTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
    private static readonly string[] Actions = ["publish", "get", "list"];

    [McpServerTool(Name = "artifacts"), Description(
        "Publish, get, or list versioned artifacts (diffs, specs, types): share an ID, not a pasted blob.")]
    public static async Task<string> Artifacts(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description(
            "publish (needs namespace, name, type, content): publish a versioned artifact for other peers to consume; share its artifact_id instead of pasting a large blob. " +
            "The version increments per (namespace, name). Content is hashed: identical content reuses the prior version. Limit 1 MB per artifact. " +
            "Emits an event to peers. Returns { artifact_id, namespace, name, version, content_hash, ... }.\n" +
            "get (needs artifact_id, or namespace and name): fetch an artifact's content; artifact_id gives that version, namespace+name the latest. " +
            "Returns the full artifact { artifact_id, namespace, name, version, type, content, content_hash, content_bytes } when the content fits. " +
            "On overflow or a range request: { artifact_id, namespace, name, version, type, content_hash, content_bytes, total_chars, offset, next_offset, truncated, content, message }; " +
            "to page through a large artifact, call again with offset=next_offset until next_offset is null.\n" +
            "list: artifact metadata (no content), newest first, to discover what is published before a get. " +
            "Returns an array of { artifact_id, namespace, name, version, type, content_bytes, content_hash, ... }.\n" +
            "Exact, lowercase."),
         AllowedValues("publish", "get", "list")] string action,
        [Description("get only: the artifact ID of one version. Pass this or namespace+name, not both")] string? artifact_id = null,
        [Description("publish (required): artifact namespace, for example the repo or project name. get: pair it with name for the latest version. list: filter")] string? @namespace = null,
        [Description("publish (required): artifact name, for example 'auth-api-contract'. get: pair it with namespace for the latest version")] string? name = null,
        [Description("publish (required): openapi-spec, typescript-types, diff, summary, test-report, schema, config, or other. list: filter")] string? type = null,
        [Description("publish (required): artifact content")] string? content = null,
        [Description("publish: repository root. list: filter")] string? repo_root = null,
        [Description("publish only: git branch")] string? branch = null,
        [Description("publish only: commit SHA")] string? commit_sha = null,
        [Description("publish only: content MIME type")] string? content_type = null,
        [Description("publish only: tags to attach to this artifact")] string[]? tags = null,
        [Description("get: start char offset into content (default 0). list: number of artifacts to skip, for example 50 for the second page")] int? offset = null,
        [Description("get only: max chars of content to return (default fits the 30 KB cap)")] int? max_chars = null,
        [Description("list only: max artifacts to return (default 50, newest first)")] int? limit = null)
    {
        action = ToolArgs.RequireAction(action, Actions);
        try
        {
            switch (action)
            {
                case "publish":
                    ToolArgs.RequirePayload(action, "namespace", @namespace is not null, required: true);
                    ToolArgs.RequirePayload(action, "name", name is not null, required: true);
                    ToolArgs.RequirePayload(action, "type", type is not null, required: true);
                    ToolArgs.RequirePayload(action, "content", content is not null, required: true);
                    await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory);
                    return JsonSerializer.Serialize(await client.PublishArtifactAsync(new PublishArtifactRequest
                    {
                        Namespace = @namespace!, Name = name!, Type = type!, Content = content!,
                        RepoRoot = repo_root, Branch = branch, CommitSha = commit_sha,
                        ContentType = content_type,
                        Tags = tags is null ? null : JsonSerializer.Serialize(tags),
                        PublishedByPeerId = state.PeerId!,
                    }), JsonOpts);

                case "get":
                    if (artifact_id is null && (@namespace is null || name is null))
                        throw new McpException("Missing parameters. Provide either artifact_id, or both namespace and name.");
                    if (artifact_id is not null && (@namespace is not null || name is not null))
                        throw new McpException("Conflicting parameters. Provide either artifact_id or namespace+name, not both.");
                    var result = artifact_id is not null
                        ? await client.GetArtifactByIdAsync(artifact_id)
                        : await client.GetArtifactByNameAsync(@namespace!, name!);
                    if (result is null)
                        throw new McpException("Artifact not found.");
                    return OutputShaper.ShapeArtifactGet(result, offset, max_chars, Limits.MaxToolResultChars, JsonOpts);

                default:
                    return JsonSerializer.Serialize(
                        await client.ListArtifactsAsync(type, repo_root, @namespace, limit ?? 50, offset ?? 0), JsonOpts);
            }
        }
        catch (BrokerApiException ex) { throw BrokerError(ex); }
    }

    /// <summary>Broker failures are tool errors, not payloads: the SDK turns this into isError=true.</summary>
    private static McpException BrokerError(BrokerApiException ex) => new(ex.Message, ex);
}
