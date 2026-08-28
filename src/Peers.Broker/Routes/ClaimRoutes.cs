using Peers.Broker.Errors;
using Peers.Broker.Services;
using Peers.Broker.Validation;
using Peers.Shared.Contracts;

namespace Peers.Broker.Routes;

public static class ClaimRoutes
{
    public static void MapClaimRoutes(this WebApplication app)
    {
        app.MapPost("/claims", async (ClaimPathsRequest req, ClaimService svc, CancellationToken ct) =>
        {
            if (RequestLimits.ValidateClaim(req) is { } err) return err;
            return (await svc.ClaimAsync(req, ct)).ToHttp();
        });

        app.MapPost("/claims/release", async (ReleaseClaimsRequest req, ClaimService svc, CancellationToken ct) =>
            (await svc.ReleaseAsync(req, ct)).ToHttp());

        // Scope is the caller's own repository, resolved from its peer row — never a repo_root the
        // caller supplies, so listing and claiming can never disagree about what "this repo" means.
        app.MapGet("/claims", async (string peer_id, ClaimService svc, CancellationToken ct) =>
            (await svc.ListForPeerAsync(peer_id, ct)).ToHttp());
    }
}
