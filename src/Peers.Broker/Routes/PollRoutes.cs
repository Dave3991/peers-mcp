using Peers.Broker.Services;

namespace Peers.Broker.Routes;

public static class PollRoutes
{
    public static void MapPollRoutes(this WebApplication app)
    {
        app.MapGet("/poll/{peerId}", async (string peerId, long? cursor, int? limit, EventService svc, CancellationToken ct) =>
            Results.Ok(await svc.PollAsync(peerId, cursor ?? 0, limit ?? 100, ct)));

        app.MapGet("/health", () => Results.Ok(new { status = "ok", version = 1 }));
    }
}
