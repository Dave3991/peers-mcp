using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;

namespace Peers.Broker.Services;

public sealed class EventService(EventRepository events, PeerRepository peers)
{
    private static readonly TimeSpan RetentionTtl = TimeSpan.FromHours(72);

    public async Task<PollResponse> PollAsync(string peerId, long cursor, int limit, CancellationToken ct)
    {
        var result = await events.GetSinceAsync(cursor, limit, ct);
        var newCursor = result.Count > 0 ? result[^1].EventId : cursor;

        if (result.Count > 0)
            await peers.UpdateLastPolledEventIdAsync(peerId, newCursor, ct);

        return new PollResponse { Events = result, Cursor = newCursor };
    }

    public async Task PruneAsync(CancellationToken ct)
    {
        var minCursor = await peers.GetMinActiveCursorAsync(ct);
        if (minCursor.HasValue)
            await events.PruneBeforeAsync(minCursor.Value, ct);

        // Fallback: always prune events older than TTL
        await events.PruneOlderThanAsync(RetentionTtl, ct);
    }
}
