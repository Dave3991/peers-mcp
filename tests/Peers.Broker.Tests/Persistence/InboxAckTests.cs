using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

public sealed class InboxAckTests : IDisposable
{
    private readonly TestDb _db = new();

    private (PeerRepository peers, InboxRepository inbox, EventRepository events) CreateRepos()
    {
        return (
            new PeerRepository(_db.Factory),
            new InboxRepository(_db.Factory),
            new EventRepository(_db.Factory));
    }

    private Task<Peer> InsertPeerAsync(string peerId, string machineId = "machine-x")
        => _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO peers (peer_id, tool, machine_id, repo_root, branch,
                    lease_expires_at, created_at, updated_at)
                VALUES (@id, 'claude-code', @machine, '/repo', 'main',
                    datetime('now', '+5 minutes'), datetime('now'), datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", peerId);
            cmd.Parameters.AddWithValue("@machine", machineId);
            await cmd.ExecuteNonQueryAsync();
            return new Peer
            {
                PeerId = peerId,
                Tool = "claude-code",
                MachineId = machineId,
                LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow,
            };
        }, CancellationToken.None);

    // GetUnackedAsync returns rows ordered ascending by event_id.
    [Fact]
    public async Task GetUnacked_ReturnsRows_AscendingByEventId()
    {
        var (_, inbox, events) = CreateRepos();
        await InsertPeerAsync("peer-drain");

        // Insert events and inbox rows; collect IDs in order
        var e1 = await events.InsertAsync("t", "{}", ct: CancellationToken.None);
        var e2 = await events.InsertAsync("t", "{}", ct: CancellationToken.None);
        var e3 = await events.InsertAsync("t", "{}", ct: CancellationToken.None);

        // Insert in reverse order to prove ordering is by event_id, not insertion order
        await inbox.InsertAsync("peer-drain", e3, ct: CancellationToken.None);
        await inbox.InsertAsync("peer-drain", e1, ct: CancellationToken.None);
        await inbox.InsertAsync("peer-drain", e2, ct: CancellationToken.None);

        var unacked = await inbox.GetUnackedAsync("peer-drain", 100, CancellationToken.None);
        Assert.Equal(3, unacked.Count);
        Assert.Equal(e1, unacked[0].Event.EventId);
        Assert.Equal(e2, unacked[1].Event.EventId);
        Assert.Equal(e3, unacked[2].Event.EventId);
    }

    // AckAsync with a cross-peer inbox_id does not ack the owner's row.
    [Fact]
    public async Task AckAsync_CrossPeer_DoesNotAckOwnerRow()
    {
        var (_, inbox, events) = CreateRepos();
        await InsertPeerAsync("peer-a-ack");
        await InsertPeerAsync("peer-b-ack");

        var eA = await events.InsertAsync("t", "{}", ct: CancellationToken.None);
        var eB = await events.InsertAsync("t", "{}", ct: CancellationToken.None);

        await inbox.InsertAsync("peer-a-ack", eA, ct: CancellationToken.None);
        await inbox.InsertAsync("peer-b-ack", eB, ct: CancellationToken.None);

        // Get A's inbox_id
        var aItems = await inbox.GetUnackedAsync("peer-a-ack", 100, CancellationToken.None);
        var inboxIdA = aItems.Single().InboxId;

        // Peer B tries to ack A's inbox row — should be a no-op due to peer_id check
        await inbox.AckAsync("peer-b-ack", [inboxIdA], CancellationToken.None);

        // A's row is still unacked
        var aStill = await inbox.GetUnackedAsync("peer-a-ack", 100, CancellationToken.None);
        Assert.Single(aStill);
        Assert.Equal(inboxIdA, aStill[0].InboxId);
    }

    // AckAllAsync for peer A leaves peer B's rows unacked.
    [Fact]
    public async Task AckAllAsync_ScopedToPeer_DoesNotAffectOtherPeer()
    {
        var (_, inbox, events) = CreateRepos();
        await InsertPeerAsync("peer-x-all");
        await InsertPeerAsync("peer-y-all");

        var eX1 = await events.InsertAsync("t", "{}", ct: CancellationToken.None);
        var eX2 = await events.InsertAsync("t", "{}", ct: CancellationToken.None);
        var eY = await events.InsertAsync("t", "{}", ct: CancellationToken.None);

        await inbox.InsertAsync("peer-x-all", eX1, ct: CancellationToken.None);
        await inbox.InsertAsync("peer-x-all", eX2, ct: CancellationToken.None);
        await inbox.InsertAsync("peer-y-all", eY, ct: CancellationToken.None);

        // Ack all of X's rows
        await inbox.AckAllAsync("peer-x-all", CancellationToken.None);

        // X has 0 unacked
        var xUnacked = await inbox.GetUnackedAsync("peer-x-all", 100, CancellationToken.None);
        Assert.Empty(xUnacked);

        // Y's row is untouched
        var yUnacked = await inbox.GetUnackedAsync("peer-y-all", 100, CancellationToken.None);
        Assert.Single(yUnacked);
    }

    public void Dispose() => _db.Dispose();
}
