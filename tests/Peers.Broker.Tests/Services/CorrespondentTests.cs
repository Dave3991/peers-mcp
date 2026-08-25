using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.Broker.Tests.Services;

/// <summary>
/// The push audience for a peer's presence changes. The rule is CORRESPONDENCE — who addressed whom —
/// not co-presence in a channel, because a fresh peer auto-announces itself into every channel scoped
/// to its repo, which would make everyone a member of everything.
/// </summary>
public sealed class CorrespondentTests : IDisposable
{
    private readonly TestDb _db = new();

    private (PeerService peers, ChannelService channels) Create()
    {
        var peerRepo = new PeerRepository(_db.Factory);
        var eventSvc = new EventService(new EventRepository(_db.Factory), peerRepo, new InboxRepository(_db.Factory));
        var channels = new ChannelService(new ChannelRepository(_db.Factory), peerRepo, eventSvc, new RecipientResolver(peerRepo));
        return (new PeerService(peerRepo, eventSvc, channels), channels);
    }

    private async Task<string> RegisterAsync(PeerService svc, string machine) =>
        (await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = machine, RepoRoot = "/repo", Branch = "main",
        }, CancellationToken.None)).PeerId;

    [Fact]
    public async Task Mentioning_MakesBothPeersEachOthersAudience()
    {
        var (peers, channels) = Create();
        var driver = await RegisterAsync(peers, "m-driver");
        var reviewer = await RegisterAsync(peers, "m-reviewer");

        var channel = await channels.CreateAsync(new CreateChannelRequest
        {
            Name = "review-thing", RepoRoot = "/repo", CreatedByPeerId = driver,
        }, CancellationToken.None);

        await channels.SendAsync(channel.Value.ChannelId, new SendMessageRequest
        {
            AuthorPeerId = driver, Content = "round 1", To = [reviewer], Quiet = false,
        }, CancellationToken.None);

        // Symmetric: addressing someone makes them your correspondent and you theirs.
        Assert.Contains(reviewer, await channels.CorrespondentPeerIdsAsync(driver, CancellationToken.None));
        Assert.Contains(driver, await channels.CorrespondentPeerIdsAsync(reviewer, CancellationToken.None));
    }

    // The regression the correspondence rule exists for: registering posts a join notice into every
    // repo channel, so "has posted in the same channel" would make every session everyone's audience.
    [Fact]
    public async Task JoinAnnouncements_ConferNoAudience()
    {
        var (peers, channels) = Create();
        var driver = await RegisterAsync(peers, "m-driver");

        var channel = await channels.CreateAsync(new CreateChannelRequest
        {
            Name = "review-thing", RepoRoot = "/repo", CreatedByPeerId = driver,
        }, CancellationToken.None);
        await channels.SendAsync(channel.Value.ChannelId, new SendMessageRequest
        {
            AuthorPeerId = driver, Content = "working", Quiet = false,
        }, CancellationToken.None);

        // Registers AFTER the channel exists, so the broker posts its join notice into it.
        var stranger = await RegisterAsync(peers, "m-stranger");

        Assert.DoesNotContain(stranger, await channels.CorrespondentPeerIdsAsync(driver, CancellationToken.None));
        Assert.DoesNotContain(driver, await channels.CorrespondentPeerIdsAsync(stranger, CancellationToken.None));
    }

    [Fact]
    public async Task APeerWithNoTraffic_HasNoAudience()
    {
        var (peers, channels) = Create();
        var lonely = await RegisterAsync(peers, "m-lonely");
        Assert.Empty(await channels.CorrespondentPeerIdsAsync(lonely, CancellationToken.None));
    }

    public void Dispose() => _db.Dispose();
}
