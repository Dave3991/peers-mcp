using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

/// <summary>
/// Tests for GetMinActiveCursorAsync — verifies that:
///  - an active peer whose last_polled_event_id is NULL contributes 0 (via COALESCE),
///    so the method returns 0 rather than null when at least one active peer exists.
///  - when no active (non-expired) peers exist, the method returns null.
/// </summary>
public sealed class PeerRepositoryCursorTests : IDisposable
{
    private readonly TestDb _db = new();

    private Task<Peer> InsertActivePeerAsync(string peerId) =>
        new PeerRepository(_db.Factory).InsertAsync(new Peer
        {
            PeerId = peerId,
            Tool = "claude-code",
            MachineId = "machine-cursor",
            RepoRoot = "/repo",
            Branch = "main",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

    private Task ExpirePeerAsync(string peerId) =>
        _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE peers SET expired_at = datetime('now', '-1 minute') WHERE peer_id = @id";
            cmd.Parameters.AddWithValue("@id", peerId);
            await cmd.ExecuteNonQueryAsync();
        }, CancellationToken.None);

    [Fact]
    public async Task GetMinActiveCursor_ReturnsZero_WhenActivePeerHasNullLastPolled()
    {
        // InsertAsync does NOT set last_polled_event_id → it stays NULL.
        // COALESCE(NULL, 0) = 0, so the min over one active peer is 0, not null.
        await InsertActivePeerAsync("cursor-peer-null");

        var repo = new PeerRepository(_db.Factory);
        var cursor = await repo.GetMinActiveCursorAsync(CancellationToken.None);

        Assert.Equal(0L, cursor);
    }

    [Fact]
    public async Task GetMinActiveCursor_ReturnsNull_WhenNoActivePeers()
    {
        // Insert a peer and immediately expire it.
        await InsertActivePeerAsync("cursor-peer-expired");
        await ExpirePeerAsync("cursor-peer-expired");

        var repo = new PeerRepository(_db.Factory);
        var cursor = await repo.GetMinActiveCursorAsync(CancellationToken.None);

        Assert.Null(cursor);
    }

    public void Dispose() => _db.Dispose();
}
