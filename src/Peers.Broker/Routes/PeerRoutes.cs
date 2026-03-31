using Peers.Broker.Services;
using Peers.Shared.Contracts;

namespace Peers.Broker.Routes;

public static class PeerRoutes
{
    public static void MapPeerRoutes(this WebApplication app)
    {
        app.MapPost("/peers", async (RegisterPeerRequest req, PeerService svc, CancellationToken ct) =>
            Results.Ok(await svc.RegisterAsync(req, ct)));

        app.MapPatch("/peers/{peerId}/heartbeat", async (string peerId, HeartbeatRequest req, PeerService svc, CancellationToken ct) =>
        {
            await svc.HeartbeatAsync(peerId, req, ct);
            return Results.Ok(new { ok = true });
        });

        app.MapGet("/peers", async (string? repo_root, string? branch, string? tool, PeerService svc, CancellationToken ct) =>
            Results.Ok(await svc.ListAsync(new ListPeersRequest { RepoRoot = repo_root, Branch = branch, Tool = tool }, ct)));

        app.MapDelete("/peers/{peerId}", async (string peerId, PeerService svc, CancellationToken ct) =>
        {
            await svc.DeregisterAsync(peerId, ct);
            return Results.Ok(new { ok = true });
        });
    }
}
