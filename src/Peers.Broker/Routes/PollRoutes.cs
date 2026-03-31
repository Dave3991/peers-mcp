using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Shared.Contracts;

namespace Peers.Broker.Routes;

public static class PollRoutes
{
    public static void MapPollRoutes(this WebApplication app)
    {
        app.MapGet("/poll/{peerId}", async (string peerId, long? cursor, int? limit, EventService svc, CancellationToken ct) =>
            Results.Ok(await svc.PollAsync(peerId, cursor ?? 0, limit ?? 100, ct)));

        app.MapGet("/peers/{peerId}/inbox", async (string peerId, int? limit, InboxRepository inbox, CancellationToken ct) =>
        {
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

        app.MapPost("/peers/{peerId}/inbox/ack", async (string peerId, InboxAckRequest req, InboxRepository inbox, CancellationToken ct) =>
        {
            if (req.All)
                await inbox.AckAllAsync(peerId, ct);
            else if (req.InboxIds is { Length: > 0 })
                await inbox.AckAsync(peerId, req.InboxIds, ct);
            return Results.Ok(new { ok = true });
        });

        app.MapGet("/health", () => Results.Ok(new { status = "ok", version = 2 }));
    }
}

public sealed class InboxAckRequest
{
    public string[]? InboxIds { get; init; }
    public bool All { get; init; }
}
