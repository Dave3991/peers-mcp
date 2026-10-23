using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Broker.Tests.Services;

public sealed class ArtifactServiceTests : IDisposable
{
    private readonly TestDb _db = new();

    private (ArtifactService svc, EventRepository events) CreateService()
    {
        var artifactRepo = new ArtifactRepository(_db.Factory);
        var eventRepo = new EventRepository(_db.Factory);
        var peerRepo = new PeerRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var eventSvc = new EventService(eventRepo, peerRepo, inboxRepo);
        return (new ArtifactService(artifactRepo, eventSvc, peerRepo), eventRepo);
    }

    private static PublishArtifactRequest MakeRequest(string ns, string name, string content, string peerId = "peer-a")
        => new()
        {
            Namespace = ns,
            Name = name,
            Type = "text",
            Content = content,
            PublishedByPeerId = peerId,
        };

    // Publish v1, then v2 — distinct content yields new versions each time.
    [Fact]
    public async Task Publish_NewVersions_WhenContentChanges()
    {
        var (svc, _) = CreateService();

        var r1 = await svc.PublishAsync(MakeRequest("n", "a", "v1"), CancellationToken.None);
        Assert.Equal(1, r1.Version);
        Assert.False(r1.Deduplicated);

        var r2 = await svc.PublishAsync(MakeRequest("n", "a", "v2"), CancellationToken.None);
        Assert.Equal(2, r2.Version);
        Assert.False(r2.Deduplicated);
    }

    // GetLatestAsync returns the highest version with the correct content.
    [Fact]
    public async Task GetLatest_ReturnsHighestVersion()
    {
        var (svc, _) = CreateService();

        await svc.PublishAsync(MakeRequest("n", "a", "v1"), CancellationToken.None);
        await svc.PublishAsync(MakeRequest("n", "a", "v2"), CancellationToken.None);

        var latest = await svc.GetAsync(null, "n", "a", CancellationToken.None);
        Assert.NotNull(latest);
        Assert.Equal(2, latest!.Version);
        Assert.Equal("v2", latest.Content);
    }

    // Publishing identical content to the latest version deduplicates — returns Deduplicated=true
    // and the same version number — and emits NO new artifact.published event.
    [Fact]
    public async Task Publish_Deduplicates_WhenContentMatchesLatest()
    {
        var (svc, events) = CreateService();

        await svc.PublishAsync(MakeRequest("n", "a", "v1"), CancellationToken.None);
        var r2 = await svc.PublishAsync(MakeRequest("n", "a", "v2"), CancellationToken.None);
        Assert.Equal(2, r2.Version);

        // Count artifact.published events before the dedup publish
        var beforeEvents = await events.GetSinceAsync(0, 1000, CancellationToken.None);
        var beforeCount = beforeEvents.Count(e => e.Type == EventTypes.ArtifactPublished);

        // Publish same content as v2 — should dedup
        var rDedup = await svc.PublishAsync(MakeRequest("n", "a", "v2"), CancellationToken.None);
        Assert.True(rDedup.Deduplicated);
        Assert.Equal(2, rDedup.Version);

        // No new artifact.published event should have been emitted
        var afterEvents = await events.GetSinceAsync(0, 1000, CancellationToken.None);
        var afterCount = afterEvents.Count(e => e.Type == EventTypes.ArtifactPublished);
        Assert.Equal(beforeCount, afterCount);
    }

    // GetByIdAsync retrieves a specific version by its artifact_id.
    [Fact]
    public async Task GetById_ReturnsSpecificVersion()
    {
        var (svc, _) = CreateService();

        var r1 = await svc.PublishAsync(MakeRequest("n", "a", "v1"), CancellationToken.None);
        await svc.PublishAsync(MakeRequest("n", "a", "v2"), CancellationToken.None);

        var fetched = await svc.GetAsync(r1.ArtifactId, null, null, CancellationToken.None);
        Assert.NotNull(fetched);
        Assert.Equal(r1.ArtifactId, fetched!.ArtifactId);
        Assert.Equal(1, fetched.Version);
        Assert.Equal("v1", fetched.Content);
    }

    [Fact]
    public async Task Silent_publication_stores_content_without_notification_and_normal_artifacts_still_notify()
    {
        var (svc, events) = CreateService();
        var silent = await svc.PublishAsync(new PublishArtifactRequest
        {
            Namespace = "review", Name = "blob", Type = "other", Content = "raw evidence",
            RepoRoot = "/fixture", PublishedByPeerId = "driver", NotifyPeers = false,
        }, CancellationToken.None);
        Assert.Equal("raw evidence", (await svc.GetAsync(silent.ArtifactId, null, null, CancellationToken.None))!.Content);
        Assert.DoesNotContain(await events.GetSinceAsync(0, 1000, CancellationToken.None), e => e.Type == EventTypes.ArtifactPublished);
        await svc.PublishAsync(MakeRequest("normal", "log", "ordinary output"), CancellationToken.None);
        Assert.Single(await events.GetSinceAsync(0, 1000, CancellationToken.None), e => e.Type == EventTypes.ArtifactPublished);
    }

    public void Dispose() => _db.Dispose();
}
