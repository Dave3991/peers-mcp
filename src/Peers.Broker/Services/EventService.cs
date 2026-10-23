using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

public sealed class EventService(EventRepository events, PeerRepository peers, InboxRepository inbox)
{
    private static readonly TimeSpan RetentionTtl = TimeSpan.FromHours(72);

    /// <summary>
    /// How long a peer.updated row is worth keeping. It is a re-read hint, not state — nothing
    /// reconstructs anything from it, and a missed one costs a subscriber one 5s poll of staleness.
    /// Progress reporting writes one per update, so leaving them to the min-cursor rule lets a single
    /// idle peer pin every one of them for 72h. Well past ChannelNotifier's 5s cadence, so a peer that
    /// is polling at all cannot miss a hint to this.
    /// </summary>
    private static readonly TimeSpan PeerUpdatedRetention = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan AckedInboxRetention = TimeSpan.FromHours(24);
    private const int MaxUnackedPerPeer = 500;

    public async Task<PollResponse> PollAsync(string peerId, long cursor, int limit, CancellationToken ct, bool peek = false)
    {
        var result = await events.GetSinceAsync(cursor, limit, ct);
        var newCursor = result.Count > 0 ? result[^1].EventId : cursor;

        if (result.Count > 0 && !peek)
            await peers.UpdateLastPolledEventIdAsync(peerId, newCursor, ct);

        return new PollResponse { Events = result, Cursor = newCursor };
    }

    public async Task<long> EmitAsync(string type, string payload, CancellationToken ct,
        string[]? recipientPeerIds = null, SqliteConnection? existingConn = null)
    {
        async Task<long> InsertAsync(SqliteConnection conn)
        {
            var eventId = await events.InsertAsync(type, payload, conn, ct);
            if (recipientPeerIds is not null)
                foreach (var peerId in recipientPeerIds)
                    await inbox.InsertAsync(peerId, eventId, conn, ct);
            return eventId;
        }

        if (existingConn is not null)
            return await InsertAsync(existingConn);
        if (recipientPeerIds is not { Length: > 0 })
            return await events.InsertAsync(type, payload, ct: ct);

        return await events.Database.WriteAsync(async conn =>
        {
            using var txn = conn.BeginTransaction();
            var eventId = await InsertAsync(conn);
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

        // Cursor-independent: see PeerUpdatedRetention.
        await events.PruneTypeOlderThanAsync(EventTypes.PeerUpdated, PeerUpdatedRetention, ct);
    }

    public async Task<int> PruneInboxAsync(CancellationToken ct)
    {
        await inbox.PurgeAckedBeforeAsync(AckedInboxRetention, ct);
        return await inbox.EnforceUnackedCapAsync(MaxUnackedPerPeer, ct);  // return cap-acked count for Warning logging
    }
}
