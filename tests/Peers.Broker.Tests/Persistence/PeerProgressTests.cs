using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

/// <summary>
/// Progress lives on the peer row, written by the presence heartbeat. The rule these pin is that
/// progress_current is the DISCRIMINATOR: supplied, and both counts are replaced; absent, and no
/// progress column moves.
/// </summary>
public sealed class PeerProgressTests : IDisposable
{
    private readonly TestDb _db = new();

    private async Task<(PeerRepository repo, string peerId)> SeedAsync()
    {
        var repo = new PeerRepository(_db.Factory);
        var peer = new Peer
        {
            PeerId = "peer000000000001",
            Tool = "claude-code",
            MachineId = "m",
            RepoRoot = "/r",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(2),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        };
        await repo.InsertAsync(peer, CancellationToken.None);
        return (repo, peer.PeerId);
    }

    private Task<int> RenewAsync(PeerRepository repo, string peerId, int? current = null, int? total = null,
        string? summary = null) =>
        repo.RenewLeaseAsync(peerId, DateTime.UtcNow.AddMinutes(2), summary, null, null, null, null, null,
            current, total, CancellationToken.None);

    [Fact]
    public async Task RenewLease_StoresProgress_AndStampsProgressUpdatedAt()
    {
        var (repo, peerId) = await SeedAsync();

        Assert.Equal(1, await RenewAsync(repo, peerId, current: 7, total: 12));

        var peer = await repo.GetAsync(peerId, CancellationToken.None);
        Assert.NotNull(peer);
        Assert.Equal(7, peer!.ProgressCurrent);
        Assert.Equal(12, peer.ProgressTotal);
        Assert.NotNull(peer.ProgressUpdatedAt);
        Assert.Equal(DateTimeKind.Utc, peer.ProgressUpdatedAt!.Value.Kind);
    }

    // The bug this exists for: with COALESCE on both columns, "3 of an unknown number" silently
    // inherits the finished task's total and reads as 3/12.
    [Fact]
    public async Task RenewLease_CurrentWithoutTotal_ClearsTotalToIndeterminate()
    {
        var (repo, peerId) = await SeedAsync();
        await RenewAsync(repo, peerId, current: 7, total: 12);

        await RenewAsync(repo, peerId, current: 3);

        var peer = await repo.GetAsync(peerId, CancellationToken.None);
        Assert.Equal(3, peer!.ProgressCurrent);
        Assert.Null(peer.ProgressTotal);
    }

    // The 30s background heartbeat must not make stalled progress look fresh.
    [Fact]
    public async Task RenewLease_NullProgress_LeavesCountsAndTimestampUntouched()
    {
        var (repo, peerId) = await SeedAsync();
        await RenewAsync(repo, peerId, current: 7, total: 12);
        var stamped = (await repo.GetAsync(peerId, CancellationToken.None))!.ProgressUpdatedAt;

        await Task.Delay(1100); // datetime('now') has one-second resolution
        await RenewAsync(repo, peerId, summary: "still going");

        var peer = await repo.GetAsync(peerId, CancellationToken.None);
        Assert.Equal(7, peer!.ProgressCurrent);
        Assert.Equal(12, peer.ProgressTotal);
        Assert.Equal(stamped, peer.ProgressUpdatedAt);
        Assert.Equal("still going", peer.Summary);
    }

    [Fact]
    public async Task Reactivate_ClearsProgress()
    {
        var (repo, peerId) = await SeedAsync();
        await RenewAsync(repo, peerId, current: 7, total: 12);
        await repo.ExpireByIdAsync(peerId, CancellationToken.None);

        await repo.ReactivateAsync(new Peer
        {
            PeerId = peerId,
            Tool = "claude-code",
            MachineId = "m",
            RepoRoot = "/r",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(2),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

        var peer = await repo.GetAsync(peerId, CancellationToken.None);
        Assert.Null(peer!.ProgressCurrent);
        Assert.Null(peer.ProgressTotal);
        Assert.Null(peer.ProgressUpdatedAt);
    }

    public void Dispose() => _db.Dispose();
}
