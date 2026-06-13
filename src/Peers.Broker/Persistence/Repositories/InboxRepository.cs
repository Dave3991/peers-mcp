using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence;
using Peers.Shared.Models;

namespace Peers.Broker.Persistence.Repositories;

public sealed class InboxRepository(SqliteConnectionFactory db)
{
    public async Task InsertAsync(string peerId, long eventId, SqliteConnection? existingConn = null, CancellationToken ct = default)
    {
        async Task DoInsert(SqliteConnection conn)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO peer_inbox (inbox_id, peer_id, event_id, created_at)
                VALUES (@id, @peer, @event, datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", Guid.NewGuid().ToString("N")[..16]);
            cmd.Parameters.AddWithValue("@peer", peerId);
            cmd.Parameters.AddWithValue("@event", eventId);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        if (existingConn is not null)
        {
            await DoInsert(existingConn);
            return;
        }

        await db.WriteAsync(async conn => { await DoInsert(conn); return 0; }, ct);
    }

    public async Task InsertForRecipientsAsync(long eventId, string[] recipientPeerIds, CancellationToken ct = default)
    {
        if (recipientPeerIds.Length == 0) return;

        await db.WriteAsync(async conn =>
        {
            foreach (var peerId in recipientPeerIds)
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = """
                    INSERT INTO peer_inbox (inbox_id, peer_id, event_id, created_at)
                    VALUES (@id, @peer, @event, datetime('now'))
                    """;
                cmd.Parameters.AddWithValue("@id", Guid.NewGuid().ToString("N")[..16]);
                cmd.Parameters.AddWithValue("@peer", peerId);
                cmd.Parameters.AddWithValue("@event", eventId);
                await cmd.ExecuteNonQueryAsync(ct);
            }
            return 0;
        }, ct);
    }

    public async Task<List<InboxItem>> GetUnackedAsync(string peerId, int limit = 100, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT i.inbox_id, i.peer_id, i.event_id, i.created_at,
                       e.type, e.payload, e.created_at AS event_created_at
                FROM peer_inbox i
                JOIN events e ON e.event_id = i.event_id
                WHERE i.peer_id = @peer AND i.acked_at IS NULL
                ORDER BY i.event_id
                LIMIT @limit
                """;
            cmd.Parameters.AddWithValue("@peer", peerId);
            cmd.Parameters.AddWithValue("@limit", limit);

            var items = new List<InboxItem>();
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                items.Add(new InboxItem
                {
                    InboxId = r.GetString(r.GetOrdinal("inbox_id")),
                    PeerId = r.GetString(r.GetOrdinal("peer_id")),
                    Event = new PeerEvent
                    {
                        EventId = r.GetInt64(r.GetOrdinal("event_id")),
                        Type = r.GetString(r.GetOrdinal("type")),
                        Payload = r.GetString(r.GetOrdinal("payload")),
                        CreatedAt = r.GetUtc("event_created_at"),
                    },
                    CreatedAt = r.GetUtc("created_at"),
                });
            }
            return items;
        }, ct);
    }

    public async Task AckAsync(string peerId, string[] inboxIds, CancellationToken ct = default)
    {
        if (inboxIds.Length == 0) return;

        await db.WriteAsync(async conn =>
        {
            foreach (var inboxId in inboxIds)
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "UPDATE peer_inbox SET acked_at = datetime('now') WHERE inbox_id = @id AND peer_id = @peer";
                cmd.Parameters.AddWithValue("@id", inboxId);
                cmd.Parameters.AddWithValue("@peer", peerId);
                await cmd.ExecuteNonQueryAsync(ct);
            }
            return 0;
        }, ct);
    }

    public async Task AckAllAsync(string peerId, CancellationToken ct = default)
    {
        await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE peer_inbox SET acked_at = datetime('now') WHERE peer_id = @peer AND acked_at IS NULL";
            cmd.Parameters.AddWithValue("@peer", peerId);
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    public async Task<int> PurgeAckedBeforeAsync(TimeSpan age, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                DELETE FROM peer_inbox WHERE acked_at IS NOT NULL AND datetime(acked_at) < datetime('now', @age)
                """;
            cmd.Parameters.AddWithValue("@age", $"-{(int)age.TotalSeconds} seconds");
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    public async Task<List<InboxItem>> WaitForUnackedAsync(
        string peerId, TimeSpan timeout, int limit, TimeSpan pollInterval, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var items = await GetUnackedAsync(peerId, limit, ct);
            if (items.Count > 0) return items;
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero) return items; // empty -> caller infers timed_out
            await Task.Delay(remaining < pollInterval ? remaining : pollInterval, ct);
        }
    }

    // Acks (does not delete) the OLDEST (lowest event_id) unacked rows beyond the cap for any peer;
    // keeps the audit row, drops only the event-pin. For a review backlog the oldest is the original
    // request — accepted ring-buffer default.
    public async Task<int> EnforceUnackedCapAsync(int cap, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                UPDATE peer_inbox SET acked_at = datetime('now')
                WHERE acked_at IS NULL AND inbox_id IN (
                  SELECT inbox_id FROM (
                    SELECT inbox_id, ROW_NUMBER() OVER (PARTITION BY peer_id ORDER BY event_id DESC) AS rn
                    FROM peer_inbox WHERE acked_at IS NULL
                  ) WHERE rn > @cap)
                """;
            cmd.Parameters.AddWithValue("@cap", cap);
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }
}

public sealed class InboxItem
{
    public required string InboxId { get; init; }
    public required string PeerId { get; init; }
    public required PeerEvent Event { get; init; }
    public required DateTime CreatedAt { get; init; }
}
