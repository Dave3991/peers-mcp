using FluentResults;
using Peers.Broker.Errors;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Shared.Contracts;

namespace Peers.Broker.Routes;

public static class PollRoutes
{
    public static void MapPollRoutes(this WebApplication app)
    {
        app.MapGet("/poll/{peerId}", async (string peerId, long? cursor, int? limit, bool? peek,
            PeerRepository peerRepo, EventService svc, CancellationToken ct) =>
        {
            var guard = await GuardAsync(peerRepo, peerId, limit, ct);
            if (guard.IsFailed) return guard.ToHttp();
            return Results.Ok(await svc.PollAsync(peerId, cursor ?? 0, limit ?? 100, ct, peek: peek ?? false));
        });

        app.MapGet("/peers/{peerId}/inbox", async (string peerId, int? limit,
            PeerRepository peerRepo, InboxRepository inbox, CancellationToken ct) =>
        {
            var guard = await GuardAsync(peerRepo, peerId, limit, ct);
            if (guard.IsFailed) return guard.ToHttp();

            var items = await inbox.GetUnackedAsync(peerId, limit ?? 100, ct);
            return Results.Ok(new InboxResponse
            {
                Items = items.Select(i => new InboxItemDto
                {
                    InboxId = i.InboxId,
                    PeerId = i.PeerId,
                    Event = i.Event,
                    CreatedAt = i.CreatedAt,
                }).ToList(),
                Count = items.Count,
            });
        });

        app.MapPost("/peers/{peerId}/inbox/ack", async (string peerId, InboxAckRequest req,
            PeerRepository peerRepo, InboxRepository inbox, CancellationToken ct) =>
        {
            var guard = await GuardAsync(peerRepo, peerId, limit: null, ct);
            if (guard.IsFailed) return guard.ToHttp();

            if (!req.All && (req.InboxIds is null || req.InboxIds.Length == 0))
                return Result.Fail(new BadRequestError("Provide either inbox_ids or all=true.")).ToHttp();

            if (req.All)
                await inbox.AckAllAsync(peerId, ct);
            else
                await inbox.AckAsync(peerId, req.InboxIds!, ct);
            return Results.Ok(new { ok = true });
        });

        app.MapGet("/health", () => Results.Ok(new { status = "ok", version = 2 }));
    }

    private static async Task<Result> GuardAsync(PeerRepository peerRepo, string peerId, int? limit, CancellationToken ct)
    {
        if (limit is < 0)
            return Result.Fail(new BadRequestError("limit must be non-negative."));
        var peer = await peerRepo.GetAsync(peerId, ct);
        return peer is null
            ? Result.Fail(new NotFoundError($"Peer '{peerId}' not found. Call peer_register first."))
            : Result.Ok();
    }
}

public sealed class InboxAckRequest
{
    public string[]? InboxIds { get; init; }
    public bool All { get; init; }
}
