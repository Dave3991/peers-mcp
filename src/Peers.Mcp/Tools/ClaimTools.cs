using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
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
/// Scope and identity are server-derived: a session claims under its own peer id, in the repo it
/// registered from or, for an absolute path elsewhere, in the git working tree that holds that path.
/// Holder liveness rides the peer lease, so a crashed session's claims stop blocking anyone within the
/// lease window whether or not it ever called release.
/// </summary>
[McpServerToolType]
public sealed class ClaimTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
    private static readonly string[] Actions = ["claim", "list", "release"];

    [McpServerTool(Name = "claims"), Description(
        "Claim paths before you edit them, list this repo's claims, or release yours. Claims never block.")]
    public static async Task<string> Claims(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description(
            "claim (needs paths): announce that this session works on these paths and find out who else already does. " +
            "Use it before editing in any repo where another session is live: a granted claim is how a peer learns to stay off your files. " +
            "Returns { repo_root, granted:[{path, repo_root, expires_at}], conflicts:[{path, repo_root, held_path, holder_peer_id, holder_role, holder_summary, expires_at}] }. " +
            "A conflict wrote nothing: coordinate with the holder (send them a mention) or work elsewhere. Nothing is blocked for you; the report is the point.\n" +
            "list: every claim held in this repository, with its holder, to see the work map before you split a task (claim already reports the conflicts that concern you). " +
            "Returns { claims:[{path, repo_root, peer_id, holder_role, holder_summary, expires_at}] }.\n" +
            "release: give back claims this session holds when a phase ends, so a peer waiting on those files does not wait out the TTL. " +
            "Claims also lapse with their TTL or when this session's lease expires. Returns { released }, the number of claims dropped.\n" +
            "Exact, lowercase."),
         AllowedValues("claim", "list", "release")] string action,
        [Description(
            "claim (required): repo-relative paths in this repository, or absolute paths in any git working tree. " +
            "A directory covers its contents. An absolute path in another git working tree (for example a sibling worktree) is claimed in that tree, where its sessions see it. " +
            "release: paths as given to claim; omit to release every claim this session holds")] string[]? paths = null,
        [Description("claim only: seconds to hold the claim (default 1800, clamped to 60..14400). Claiming a path you hold again extends it")] int? ttl_seconds = null,
        CancellationToken cancellationToken = default)
    {
        action = ToolArgs.RequireAction(action, Actions);
        if (action == "claim") ToolArgs.RequirePayload(action, "paths", paths is not null, required: true);
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            return action switch
            {
                "claim" => JsonSerializer.Serialize(await client.ClaimPathsAsync(new ClaimPathsRequest
                {
                    PeerId = state.PeerId!,
                    Paths = paths!,
                    TtlSeconds = ttl_seconds,
                }, cancellationToken), JsonOpts),
                "release" => JsonSerializer.Serialize(await client.ReleaseClaimsAsync(new ReleaseClaimsRequest
                {
                    PeerId = state.PeerId!,
                    Paths = paths,
                }, cancellationToken), JsonOpts),
                _ => JsonSerializer.Serialize(await client.ListClaimsAsync(state.PeerId!, cancellationToken), JsonOpts),
            };
        }
        catch (BrokerApiException ex) { throw new McpException(ex.Message, ex); }
    }
}
