using Peers.Broker.Services;
using Peers.Broker.Validation;
using Peers.Shared.Contracts;

namespace Peers.Broker.Routes;

public static class ArtifactRoutes
{
    public static void MapArtifactRoutes(this WebApplication app)
    {
        app.MapPost("/artifacts", async (PublishArtifactRequest req, ArtifactService svc, CancellationToken ct) =>
        {
            var err = RequestLimits.ValidatePublishArtifact(req);
            if (err is not null) return err;
            return Results.Ok(await svc.PublishAsync(req, ct));
        });

        app.MapGet("/artifacts/{id}", async (string id, ArtifactService svc, CancellationToken ct) =>
        {
            var result = await svc.GetAsync(id, null, null, ct);
            return result is not null ? Results.Ok(result) : Results.NotFound();
        });

        app.MapGet("/artifacts/by-name", async (string ns, string name, ArtifactService svc, CancellationToken ct) =>
        {
            var result = await svc.GetAsync(null, ns, name, ct);
            return result is not null
                ? Results.Ok(result)
                : Results.NotFound(new { error = $"Artifact '{ns}/{name}' not found." });
        });

        app.MapGet("/artifacts", async (string? type, string? repo_root, string? ns, ArtifactService svc, CancellationToken ct) =>
            Results.Ok(await svc.ListAsync(type, repo_root, ns, ct)));
    }
}
