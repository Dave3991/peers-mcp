using System.Diagnostics;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Tests.Helpers;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

public sealed class InboxWaitTests : IDisposable
{
    private readonly TestDb _db = new();

    private (PeerRepository peers, InboxRepository inbox, EventRepository events) CreateRepos()
    {
        var peers = new PeerRepository(_db.Factory);
        var inbox = new InboxRepository(_db.Factory);
        var events = new EventRepository(_db.Factory);
        return (peers, inbox, events);
    }

    public void Dispose() => _db.Dispose();

    [Fact]
    public async Task WaitForUnacked_ReturnsEarly_WhenItemArrives()
    {
        var (peers, inbox, events) = CreateRepos();

        await peers.InsertAsync(new Peers.Shared.Models.Peer
        {
            PeerId = "wait-peer",
            Tool = "claude-code",
            MachineId = "m",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

        var sw = Stopwatch.StartNew();
        var waitTask = inbox.WaitForUnackedAsync(
            "wait-peer",
            TimeSpan.FromSeconds(3),
            100,
            TimeSpan.FromMilliseconds(50),
            CancellationToken.None);

        // Let the polling loop run once before inserting the item
        await Task.Delay(200);

        var eid = await events.InsertAsync("test", "{}", ct: CancellationToken.None);
        await inbox.InsertAsync("wait-peer", eid, ct: CancellationToken.None);

        var items = await waitTask;
        sw.Stop();

        Assert.Single(items);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2),
            $"Expected early return, but took {sw.Elapsed.TotalSeconds:F2}s");
    }

    [Fact]
    public async Task WaitForUnacked_TimesOutEmpty_WhenNoItems()
    {
        var (peers, inbox, _) = CreateRepos();

        await peers.InsertAsync(new Peers.Shared.Models.Peer
        {
            PeerId = "wait-peer",
            Tool = "claude-code",
            MachineId = "m",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

        var sw = Stopwatch.StartNew();
        var items = await inbox.WaitForUnackedAsync(
            "wait-peer",
            TimeSpan.FromMilliseconds(400),
            100,
            TimeSpan.FromMilliseconds(50),
            CancellationToken.None);
        sw.Stop();

        Assert.Empty(items);
        Assert.True(sw.ElapsedMilliseconds >= 350,
            $"Expected to wait ~400ms, but only waited {sw.ElapsedMilliseconds}ms");
    }
}
