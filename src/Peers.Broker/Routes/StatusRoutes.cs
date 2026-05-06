using Microsoft.Data.Sqlite;
using Peers.Broker.Hosting;
using Peers.Broker.Persistence;

namespace Peers.Broker.Routes;

public static class StatusRoutes
{
    public static void MapStatusRoutes(this WebApplication app)
    {
        app.MapGet("/status", async (SqliteConnectionFactory dbFactory, BrokerStatus brokerStatus, CancellationToken ct) =>
        {
            var counts = await dbFactory.ReadAsync(async conn =>
            {
                var activePeers = await ScalarLongAsync(
                    conn,
                    "SELECT COUNT(*) FROM peers WHERE expired_at IS NULL AND datetime(lease_expires_at) > datetime('now')",
                    [],
                    ct);

                var unackedInbox = await ScalarLongAsync(
                    conn,
                    "SELECT COUNT(*) FROM peer_inbox WHERE acked_at IS NULL",
                    [],
                    ct);

                var recentEvents = await ScalarLongAsync(
                    conn,
                    "SELECT COUNT(*) FROM events WHERE created_at >= datetime('now', '-5 minutes')",
                    [],
                    ct);

                return (activePeers, unackedInbox, recentEvents);
            }, ct);

            var uptime = (long)(DateTimeOffset.UtcNow - brokerStatus.ProcessStartTime).TotalSeconds;
            var lastCleanup = brokerStatus.LastCleanupAt;

            return Results.Ok(new
            {
                status = "ok",
                version = 2,
                active_peer_count = counts.activePeers,
                unacked_inbox_total = counts.unackedInbox,
                events_last_5min = counts.recentEvents,
                last_cleanup_at = lastCleanup?.ToString("O"),
                broker_uptime_seconds = uptime,
            });
        });
    }

    private static async Task<long> ScalarLongAsync(
        SqliteConnection conn,
        string sql,
        (string Name, object Value)[] parameters,
        CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in parameters)
            cmd.Parameters.AddWithValue(name, value);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is null ? 0L : Convert.ToInt64(result);
    }
}
