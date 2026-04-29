using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;

namespace Peers.Broker.Services;

public sealed class EventService(EventRepository events, PeerRepository peers, InboxRepository inbox)
{
    private static readonly TimeSpan RetentionTtl = TimeSpan.FromHours(72);

    public async Task<PollResponse> PollAsync(string peerId, long cursor, int limit, CancellationToken ct, bool peek = false)
    {
        var result = await events.GetSinceAsync(cursor, limit, ct);
        var newCursor = result.Count > 0 ? result[^1].EventId : cursor;

        if (result.Count > 0 && !peek)
            await peers.UpdateLastPolledEventIdAsync(peerId, newCursor, ct);

        return new PollResponse { Events = result, Cursor = newCursor };
    }

    public async Task<long> EmitAsync(string type, string payload, CancellationToken ct, string[]? recipientPeerIds = null)
    {
        if (recipientPeerIds is not { Length: > 0 })
            return await events.InsertAsync(type, payload, ct: ct);

        // Atomic: event + inbox rows in one transaction
        return await events.Database.WriteAsync(async conn =>
        {
            using var txn = conn.BeginTransaction();
            var eventId = await events.InsertAsync(type, payload, conn, ct);
            foreach (var peerId in recipientPeerIds)
                await inbox.InsertAsync(peerId, eventId, conn, ct);
            txn.Commit();
            return eventId;
        }, ct);
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
