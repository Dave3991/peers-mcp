using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.Broker.Tests.Services;

public sealed class DiscoveryTests : IDisposable
{
    private readonly TestDb _db = new();

    private (PeerRepository peerRepo, PeerService peerSvc) CreatePeerService()
    {
        var eventRepo = new EventRepository(_db.Factory);
        var peerRepo = new PeerRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var eventService = new EventService(eventRepo, peerRepo, inboxRepo);
        return (peerRepo, new PeerService(peerRepo, eventService, CreateChannelService(peerRepo, eventService)));
    }

    private ChannelService CreateChannelService(PeerRepository peerRepo, EventService eventService) =>
        new ChannelService(new ChannelRepository(_db.Factory), peerRepo, eventService, new RecipientResolver(peerRepo));

    private ArtifactService CreateArtifactService(PeerRepository peerRepo, EventService eventService) =>
        new ArtifactService(new ArtifactRepository(_db.Factory), eventService, peerRepo);

    // ── peer_list: IncludeExpired ─────────────────────────────────────────────

    [Fact]
    public async Task PeerList_IncludeExpiredFalse_ExcludesExpiredPeer()
    {
        var (peerRepo, peerSvc) = CreatePeerService();

        var resp = await peerSvc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m1", RepoRoot = "/repo",
        }, CancellationToken.None);

        // Expire it
        await peerRepo.ExpireByIdAsync(resp.PeerId, CancellationToken.None);

        var result = await peerSvc.ListAsync(new ListPeersRequest { RepoRoot = "/repo", IncludeExpired = false }, CancellationToken.None);
        Assert.DoesNotContain(result, p => p.PeerId == resp.PeerId);
    }

    [Fact]
    public async Task PeerList_IncludeExpiredTrue_ContainsExpiredPeer()
    {
        var (peerRepo, peerSvc) = CreatePeerService();

        var resp = await peerSvc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m1", RepoRoot = "/repo",
        }, CancellationToken.None);

        // Expire it
        await peerRepo.ExpireByIdAsync(resp.PeerId, CancellationToken.None);

        var result = await peerSvc.ListAsync(new ListPeersRequest { RepoRoot = "/repo", IncludeExpired = true }, CancellationToken.None);
        Assert.Contains(result, p => p.PeerId == resp.PeerId);
    }

    // ── channels: listing + repo scoping ──────────────────────────────────────

    [Fact]
    public async Task ChannelList_LimitAndOrdering_StableNewestFirst()
    {
        var (peerRepo, peerSvc) = CreatePeerService();
        var eventRepo = new EventRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var eventService = new EventService(eventRepo, peerRepo, inboxRepo);
        var channelSvc = CreateChannelService(peerRepo, eventService);

        var peer = await peerSvc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m-channels", RepoRoot = "/repo",
        }, CancellationToken.None);

        const int total = 5;
        var ids = new List<string>();
        for (var i = 0; i < total; i++)
        {
            var result = await channelSvc.CreateAsync(new CreateChannelRequest
            {
                Name = $"chan-{i}",
                RepoRoot = "/repo",
                CreatedByPeerId = peer.PeerId,
            }, CancellationToken.None);
            Assert.True(result.IsSuccess);
            ids.Add(result.Value.ChannelId);
        }

        var all = await channelSvc.ListAsync(null, 100, CancellationToken.None);
        Assert.True(all.Channels.Length >= total);

        var limited = await channelSvc.ListAsync(null, 2, CancellationToken.None);
        Assert.Equal(2, limited.Channels.Length);

        // The limited page is a prefix of the full order (stable, newest first).
        Assert.Equal(all.Channels.Take(2).Select(c => c.ChannelId), limited.Channels.Select(c => c.ChannelId));

        // Repo scoping filters out other scopes.
        await channelSvc.CreateAsync(new CreateChannelRequest
        {
            Name = "other-scope", RepoRoot = "/other", CreatedByPeerId = peer.PeerId,
        }, CancellationToken.None);
        var repoOnly = await channelSvc.ListAsync("/repo", 100, CancellationToken.None);
        Assert.DoesNotContain(repoOnly.Channels, c => c.Name == "other-scope");
    }

    // ── artifact_list: pagination + stable order ──────────────────────────────

    [Fact]
    public async Task ArtifactList_Pagination_NewestFirstDisjointPages()
    {
        var (peerRepo, peerSvc) = CreatePeerService();
        var eventRepo = new EventRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var eventService = new EventService(eventRepo, peerRepo, inboxRepo);
        var artifactSvc = CreateArtifactService(peerRepo, eventService);

        var peer = await peerSvc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m-arts", RepoRoot = "/repo",
        }, CancellationToken.None);

        // Insert 5 artifacts with distinct names (so each is version 1, no dedup)
        const int total = 5;
        for (var i = 0; i < total; i++)
        {
            var result = await artifactSvc.PublishAsync(new PublishArtifactRequest
            {
                Namespace = "test-ns",
                Name = $"artifact-{i}",
                Type = "summary",
                Content = $"content-{i}",
                PublishedByPeerId = peer.PeerId,
            }, CancellationToken.None);
            Assert.False(result.Deduplicated);
        }

        // Full unpaginated list
        var allArtifacts = await artifactSvc.ListAsync(null, null, null, 100, 0, CancellationToken.None);
        Assert.Equal(total, allArtifacts.Count);

        // Page 0: first 2 (newest)
        var page0 = await artifactSvc.ListAsync(null, null, null, 2, 0, CancellationToken.None);
        // Page 1: next 2
        var page1 = await artifactSvc.ListAsync(null, null, null, 2, 2, CancellationToken.None);

        Assert.Equal(2, page0.Count);
        Assert.Equal(2, page1.Count);

        // Pages must be disjoint
        var page0Ids = page0.Select(a => a.ArtifactId).ToHashSet();
        var page1Ids = page1.Select(a => a.ArtifactId).ToHashSet();
        Assert.Empty(page0Ids.Intersect(page1Ids));

        // Together they equal the first 4 in full-list order
        var expected4Ids = allArtifacts.Take(4).Select(a => a.ArtifactId).ToList();
        var combined = page0.Select(a => a.ArtifactId).Concat(page1.Select(a => a.ArtifactId)).ToList();
        Assert.Equal(expected4Ids, combined);
    }

    public void Dispose() => _db.Dispose();
}
