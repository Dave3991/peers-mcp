using Peers.Broker.Services;
using Peers.Shared.Contracts;

namespace Peers.Broker.Routes;

public static class ScratchpadRoutes
{
    public static void MapScratchpadRoutes(this WebApplication app)
    {
        app.MapPost("/scratchpads", async (CreateScratchpadRequest req, ScratchpadService svc, CancellationToken ct) =>
            Results.Ok(await svc.CreateAsync(req, ct)));

        app.MapPost("/scratchpads/{id}/entries", async (string id, AppendEntryRequest req, ScratchpadService svc, CancellationToken ct) =>
        {
            var result = await svc.AppendAsync(id, req, ct);
            return result is not null ? Results.Ok(result) : Results.Ok(new { duplicate = true });
        });

        app.MapGet("/scratchpads/{id}", async (string id, int? limit, int? offset, ScratchpadService svc, CancellationToken ct) =>
        {
            var result = await svc.ReadAsync(id, limit ?? 200, offset ?? 0, ct);
            return result is not null ? Results.Ok(result) : Results.NotFound();
        });

        app.MapGet("/scratchpads", async (string? type, string? repo_root, string? status, ScratchpadService svc, CancellationToken ct) =>
            Results.Ok(await svc.ListAsync(type, repo_root, status, ct)));

        app.MapPatch("/scratchpads/{id}/close", async (string id, CloseScratchpadRequest req, ScratchpadService svc, CancellationToken ct) =>
        {
            await svc.CloseAsync(id, req, ct);
            return Results.Ok(new { ok = true });
        });
    }
}
