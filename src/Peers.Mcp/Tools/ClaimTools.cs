using System.ComponentModel;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

/// <summary>
/// Path claims: the one question two sessions in one repo have to answer before editing, asked of the
/// broker instead of each other. A claim never blocks a write — it reports who else is already on the
/// path, which is the fact prose coordination keeps failing to carry.
///
/// Scope and identity are server-derived: a session claims inside the repo it registered from, under
/// its own peer id. Holder liveness rides the peer lease, so a crashed session's claims stop blocking
/// anyone within the lease window whether or not it ever called release.
/// </summary>
[McpServerToolType]
public sealed class ClaimTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "claim"), Description(
        "Announce that this session is working on these paths, and find out who else already is.\n" +
        "When to use: before editing, in any repo where another session is live — a granted claim is how a peer learns to stay off your files.\n" +
        "A directory claims everything under it. Re-claiming a path you hold extends it; TTL defaults to 30 minutes (60s..4h).\n" +
        "Returns: { repo_root, granted:[{path, expires_at}], conflicts:[{path, held_path, holder_peer_id, holder_role, holder_summary, expires_at}] }.\n" +
        "Conflicts wrote nothing: coordinate with the holder (send them a mention) or work elsewhere. Nothing is blocked for you — the report is the point.")]
    public static async Task<string> Claim(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Repo-relative or absolute paths inside this repository; a directory covers its contents")] string[] paths,
        [Description("Seconds to hold the claim (default 1800, clamped to 60..14400)")] int? ttl_seconds = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var resp = await client.ClaimPathsAsync(new ClaimPathsRequest
            {
                PeerId = state.PeerId!,
                Paths = paths,
                TtlSeconds = ttl_seconds,
            }, cancellationToken);
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { throw new McpException(ex.Message, ex); }
    }

    [McpServerTool(Name = "release"), Description(
        "Release path claims this session holds — call it when a phase ends, so a peer waiting on those files does not wait out the TTL.\n" +
        "Omit paths to release everything. Claims also lapse on their own with the TTL or when this session's lease expires.\n" +
        "Returns: { released } — the number of claims dropped.")]
    public static async Task<string> Release(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Paths to release; omit to release every claim this session holds")] string[]? paths = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var resp = await client.ReleaseClaimsAsync(new ReleaseClaimsRequest
            {
                PeerId = state.PeerId!,
                Paths = paths,
            }, cancellationToken);
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { throw new McpException(ex.Message, ex); }
    }

    [McpServerTool(Name = "claims"), Description(
        "Every path claim currently held in this repository, with its holder.\n" +
        "Use to see the work map before splitting a task; `claim` already reports the conflicts that concern you.\n" +
        "Returns: array of { path, repo_root, peer_id, holder_role, holder_summary, expires_at }.")]
    public static async Task<string> Claims(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var resp = await client.ListClaimsAsync(state.PeerId!, cancellationToken);
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { throw new McpException(ex.Message, ex); }
    }
}
