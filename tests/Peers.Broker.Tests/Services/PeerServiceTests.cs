using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.Broker.Tests.Services;

public sealed class PeerServiceTests : IDisposable
{
    private readonly TestDb _db = new();
    private PeerService CreateService()
    {
        var eventRepo = new EventRepository(_db.Factory);
        var peerRepo = new PeerRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var eventService = new EventService(eventRepo, peerRepo, inboxRepo);
        return new PeerService(peerRepo, eventService);
    }

    [Fact]
    public async Task Register_CreatesNewPeer()
    {
        var svc = CreateService();
        var resp = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "test-machine", RepoRoot = "/repo", Branch = "main",
        }, CancellationToken.None);

        Assert.NotEmpty(resp.PeerId);
        Assert.False(resp.Resumed);
        Assert.Equal(0, resp.LastCursor);
    }

    [Fact]
    public async Task Register_ImplicitResume_MatchesExpiredPeer()
    {
        var svc = CreateService();
        var first = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "test-machine", RepoRoot = "/repo", Branch = "main",
        }, CancellationToken.None);

        // Expire it
        await svc.DeregisterAsync(first.PeerId, CancellationToken.None);

        // Register again with same identity
        var second = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "test-machine", RepoRoot = "/repo", Branch = "main",
        }, CancellationToken.None);

        Assert.Equal(first.PeerId, second.PeerId);
        Assert.True(second.Resumed);
    }

    [Fact]
    public async Task Register_ExplicitResume_ReactivatesByPeerId()
    {
        var svc = CreateService();
        var first = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "test-machine", RepoRoot = "/repo",
        }, CancellationToken.None);

        await svc.DeregisterAsync(first.PeerId, CancellationToken.None);

        var second = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = "test-machine", RepoRoot = "/other",
            ResumePeerId = first.PeerId,
        }, CancellationToken.None);

        Assert.Equal(first.PeerId, second.PeerId);
        Assert.True(second.Resumed);
    }

    [Fact]
    public async Task List_ExcludesExpiredPeers()
    {
        var svc = CreateService();
        var resp = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "test-machine", RepoRoot = "/repo",
        }, CancellationToken.None);

        var before = await svc.ListAsync(new ListPeersRequest { RepoRoot = "/repo" }, CancellationToken.None);
        Assert.Single(before);

        await svc.DeregisterAsync(resp.PeerId, CancellationToken.None);

        var after = await svc.ListAsync(new ListPeersRequest { RepoRoot = "/repo" }, CancellationToken.None);
        Assert.Empty(after);
    }

    public void Dispose() => _db.Dispose();
}
