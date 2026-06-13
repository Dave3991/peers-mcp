using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Xunit;

namespace Peers.Broker.Tests.Services;

public sealed class EventServiceAtomicTests : IDisposable
{
    private readonly TestDb _db = new();

    private (EventService eventSvc, EventRepository events, InboxRepository inbox, PeerRepository peers) CreateServices()
    {
        var eventRepo = new EventRepository(_db.Factory);
        var peerRepo = new PeerRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var eventSvc = new EventService(eventRepo, peerRepo, inboxRepo);
        return (eventSvc, eventRepo, inboxRepo, peerRepo);
    }

    private Task InsertActivePeer(string peerId)
        => _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO peers (peer_id, tool, machine_id, repo_root, branch,
                    lease_expires_at, created_at, updated_at)
                VALUES (@id, 'claude-code', 'machine', '/repo', 'main',
                    datetime('now', '+5 minutes'), datetime('now'), datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", peerId);
            await cmd.ExecuteNonQueryAsync();
            return 0;
        }, CancellationToken.None);

    // EmitAsync with 2 recipients: exactly ONE event row + ONE inbox row per recipient, all sharing event_id.
    [Fact]
    public async Task EmitAsync_WithRecipients_CreatesOneEventAndOneInboxRowPerRecipient()
    {
        var (eventSvc, events, inbox, _) = CreateServices();

        await InsertActivePeer("emit-peer-a");
        await InsertActivePeer("emit-peer-b");

        var snapshotBefore = await events.GetSinceAsync(0, 1000, CancellationToken.None);
        var countBefore = snapshotBefore.Count;

        var eventId = await eventSvc.EmitAsync("test.emit", "{}", CancellationToken.None,
            recipientPeerIds: ["emit-peer-a", "emit-peer-b"]);

        // Exactly one new event row
        var snapshotAfter = await events.GetSinceAsync(0, 1000, CancellationToken.None);
        Assert.Equal(countBefore + 1, snapshotAfter.Count);
        Assert.Contains(snapshotAfter, e => e.EventId == eventId);

        // One inbox row per recipient, both referencing the same event_id
        var aInbox = await inbox.GetUnackedAsync("emit-peer-a", 100, CancellationToken.None);
        var bInbox = await inbox.GetUnackedAsync("emit-peer-b", 100, CancellationToken.None);

        Assert.Contains(aInbox, i => i.Event.EventId == eventId);
        Assert.Contains(bInbox, i => i.Event.EventId == eventId);
    }

    // FK ON DELETE CASCADE: deleting an event row cascades to its peer_inbox rows.
    [Fact]
    public async Task DeleteEvent_CascadesTo_InboxRows()
    {
        var (eventSvc, events, inbox, _) = CreateServices();

        await InsertActivePeer("cascade-peer");

        var eventId = await eventSvc.EmitAsync("cascade.test", "{}", CancellationToken.None,
            recipientPeerIds: ["cascade-peer"]);

        // Confirm inbox row exists
        var before = await inbox.GetUnackedAsync("cascade-peer", 100, CancellationToken.None);
        Assert.Contains(before, i => i.Event.EventId == eventId);

        // Delete the event row directly via raw SQL
        await _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM events WHERE event_id = @id";
            cmd.Parameters.AddWithValue("@id", eventId);
            await cmd.ExecuteNonQueryAsync();
            return 0;
        }, CancellationToken.None);

        // The peer_inbox row for that event should be gone (ON DELETE CASCADE)
        var afterInboxCount = await _db.Factory.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM peer_inbox WHERE event_id = @id";
            cmd.Parameters.AddWithValue("@id", eventId);
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt64(result!);
        }, CancellationToken.None);

        Assert.Equal(0, afterInboxCount);
    }

    public void Dispose() => _db.Dispose();
}
