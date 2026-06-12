using Peers.Broker.Errors;
using Peers.Broker.Services;
using Peers.Broker.Validation;
using Peers.Shared.Contracts;

namespace Peers.Broker.Routes;

public static class ScratchpadRoutes
{
    public static void MapScratchpadRoutes(this WebApplication app)
    {
        app.MapPost("/scratchpads", async (CreateScratchpadRequest req, ScratchpadService svc, CancellationToken ct) =>
        {
            var err = RequestLimits.ValidateScratchpadCreate(req);
            if (err is not null) return err;
            return (await svc.CreateAsync(req, ct)).ToHttp();
        });

        app.MapPost("/scratchpads/{id}/entries", async (string id, AppendEntryRequest req, ScratchpadService svc, CancellationToken ct) =>
        {
            var err = RequestLimits.ValidateAppendEntry(req);
            if (err is not null) return err;
            var result = await svc.AppendAsync(id, req, ct);
            return result.ToHttp(value => value is not null ? Results.Ok(value) : Results.Ok(new { duplicate = true }));
        });

        app.MapGet("/scratchpads/{id}", async (string id, int? limit, int? offset, ScratchpadService svc, CancellationToken ct) =>
        {
            var result = await svc.ReadAsync(id, limit ?? 200, offset ?? 0, ct);
            return result is not null ? Results.Ok(result) : Results.NotFound(new { error = $"Scratchpad '{id}' not found." });
        });

        app.MapGet("/scratchpads", async (string? type, string? repo_root, string? status, int? limit, int? offset, ScratchpadService svc, CancellationToken ct) =>
            Results.Ok(await svc.ListAsync(type, repo_root, status, limit ?? 50, offset ?? 0, ct)));

        app.MapPatch("/scratchpads/{id}/resolve", async (string id, ResolveScratchpadRequest req, ScratchpadService svc, CancellationToken ct) =>
            (await svc.ResolveAsync(id, req, ct)).ToHttp(Results.Ok(new { ok = true })));

        app.MapPatch("/scratchpads/{id}/close", async (string id, CloseScratchpadRequest req, ScratchpadService svc, CancellationToken ct) =>
            (await svc.CloseAsync(id, req, ct)).ToHttp(Results.Ok(new { ok = true })));
    }
}
