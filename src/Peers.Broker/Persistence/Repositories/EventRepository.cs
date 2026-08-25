using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence;
using Peers.Shared.Models;

namespace Peers.Broker.Persistence.Repositories;

public sealed class EventRepository(SqliteConnectionFactory db)
{
    public SqliteConnectionFactory Database => db;
    public async Task<long> InsertAsync(string type, string payload, SqliteConnection? existingConn = null, CancellationToken ct = default)
    {
        async Task<long> DoInsert(SqliteConnection conn)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO events (type, payload, created_at) VALUES (@type, @payload, datetime('now'));
                SELECT last_insert_rowid();
                """;
            cmd.Parameters.AddWithValue("@type", type);
            cmd.Parameters.AddWithValue("@payload", payload);
            var result = await cmd.ExecuteScalarAsync(ct);
            return Convert.ToInt64(result);
        }

        if (existingConn is not null)
            return await DoInsert(existingConn);

        return await db.WriteAsync(DoInsert, ct);
    }

    public async Task<List<PeerEvent>> GetSinceAsync(long cursor, int limit = 100, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM events WHERE event_id > @cursor ORDER BY event_id LIMIT @limit";
            cmd.Parameters.AddWithValue("@cursor", cursor);
            cmd.Parameters.AddWithValue("@limit", limit);

            var events = new List<PeerEvent>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                events.Add(new PeerEvent
                {
                    EventId = reader.GetInt64(reader.GetOrdinal("event_id")),
                    Type = reader.GetString(reader.GetOrdinal("type")),
                    Payload = reader.GetString(reader.GetOrdinal("payload")),
                    CreatedAt = reader.GetUtc("created_at"),
                });
            }
            return events;
        }, ct);
    }

    public async Task<int> PruneBeforeAsync(long eventId, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            // Keep events still referenced by un-acked inbox rows so peer_inbox can JOIN.
            cmd.CommandText = """
                DELETE FROM events
                WHERE event_id < @eid
                  AND event_id NOT IN (SELECT event_id FROM peer_inbox WHERE acked_at IS NULL)
                """;
            cmd.Parameters.AddWithValue("@eid", eventId);
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    /// <summary>
    /// Drops events of one type past a short age, WITHOUT waiting for every peer's cursor to pass them.
    /// For a type whose rows carry no durable value — a hint to re-read a resource — the min-cursor rule
    /// is pure cost: one live peer that never polls pins the whole table until the 72h TTL.
    ///
    /// Keeps the inbox guard anyway. Undirected types have no inbox rows today, but the guard is what
    /// makes this safe to point at a directed type later.
    /// </summary>
    public async Task<int> PruneTypeOlderThanAsync(string type, TimeSpan age, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                DELETE FROM events
                WHERE type = @type
                  AND created_at < datetime('now', @age)
                  AND event_id NOT IN (SELECT event_id FROM peer_inbox WHERE acked_at IS NULL)
                """;
            cmd.Parameters.AddWithValue("@type", type);
            cmd.Parameters.AddWithValue("@age", $"-{(int)age.TotalSeconds} seconds");
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    public async Task<int> PruneOlderThanAsync(TimeSpan age, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                DELETE FROM events
                WHERE created_at < datetime('now', @age)
                  AND event_id NOT IN (SELECT event_id FROM peer_inbox WHERE acked_at IS NULL)
                """;
            cmd.Parameters.AddWithValue("@age", $"-{(int)age.TotalSeconds} seconds");
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }
}
