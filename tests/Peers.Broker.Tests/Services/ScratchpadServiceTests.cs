using System.Text.Json;
using Peers.Broker.Errors;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Broker.Tests.Services;

public sealed class ScratchpadServiceTests : IDisposable
{
    private readonly TestDb _db = new();
    private readonly PeerRepository _peers;
    private readonly InboxRepository _inbox;
    private readonly EventRepository _events;
    private readonly PeerService _peerService;
    private readonly ScratchpadService _pads;

    public ScratchpadServiceTests()
    {
        _peers = new PeerRepository(_db.Factory);
        _inbox = new InboxRepository(_db.Factory);
        _events = new EventRepository(_db.Factory);
        var eventService = new EventService(_events, _peers, _inbox);
        _peerService = new PeerService(_peers, eventService);
        _pads = new ScratchpadService(new ScratchpadRepository(_db.Factory), eventService, _peers, new RecipientResolver(_peers));
    }

    [Fact]
    public async Task Append_ReRoutesExpiredReviewerToLiveSessionOnSameMachine()
    {
        var implementer = await Register("impl-machine");
        var reviewerV1 = await Register("review-machine");

        var pad = await _pads.CreateAsync(new CreateScratchpadRequest
        {
            Type = "review",
            Title = "forecast review",
            RepoRoot = "/repo",
            RequestedReviewers = JsonSerializer.Serialize(new[] { reviewerV1.PeerId }),
            CreatedByPeerId = implementer.PeerId,
        }, CancellationToken.None);
        Assert.True(pad.IsSuccess);

        // Reviewer's session restarts under a fresh peer ID (no implicit resume).
        await _peerService.DeregisterAsync(reviewerV1.PeerId, CancellationToken.None);
        var reviewerV2 = await InsertLivePeer("live-reviewer-2", "review-machine");

        var append = await _pads.AppendAsync(pad.Value.ScratchpadId, new AppendEntryRequest
        {
            AuthorPeerId = implementer.PeerId,
            Content = "please re-review",
        }, CancellationToken.None);

        Assert.True(append.IsSuccess);
        var recipients = append.Value!.Recipients!;

        // The push targets the reviewer's current live session, not the stale ID.
        Assert.Contains(reviewerV2.PeerId, recipients.OnlinePeerIds);
        Assert.DoesNotContain(reviewerV1.PeerId, recipients.OnlinePeerIds);
        Assert.DoesNotContain(reviewerV1.PeerId, recipients.OfflinePeerIds);

        // And the live session actually receives the inbox entry; the stale ID gets nothing.
        var liveInbox = await _inbox.GetUnackedAsync(reviewerV2.PeerId, ct: CancellationToken.None);
        Assert.Contains(liveInbox, i => i.Event.Type == EventTypes.ScratchpadEntryAdded);

        var staleInbox = await _inbox.GetUnackedAsync(reviewerV1.PeerId, ct: CancellationToken.None);
        Assert.DoesNotContain(staleInbox, i => i.Event.Type == EventTypes.ScratchpadEntryAdded);
    }

    [Fact]
    public async Task Append_ReportsOfflineWhenNoLiveReplacementExists()
    {
        var implementer = await Register("impl-machine");
        var reviewer = await Register("review-machine");

        var pad = await _pads.CreateAsync(new CreateScratchpadRequest
        {
            Type = "review",
            Title = "forecast review",
            RepoRoot = "/repo",
            RequestedReviewers = JsonSerializer.Serialize(new[] { reviewer.PeerId }),
            CreatedByPeerId = implementer.PeerId,
        }, CancellationToken.None);
        Assert.True(pad.IsSuccess);

        // Reviewer goes away entirely — no replacement session on its machine.
        await _peerService.DeregisterAsync(reviewer.PeerId, CancellationToken.None);

        var append = await _pads.AppendAsync(pad.Value.ScratchpadId, new AppendEntryRequest
        {
            AuthorPeerId = implementer.PeerId,
            Content = "please re-review",
        }, CancellationToken.None);

        Assert.True(append.IsSuccess);
        Assert.Contains(reviewer.PeerId, append.Value!.Recipients!.OfflinePeerIds);
    }

    private async Task<Peer> Register(string machineId)
    {
        var resp = await _peerService.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = machineId, RepoRoot = "/repo", Branch = "main",
        }, CancellationToken.None);
        return (await _peers.GetAsync(resp.PeerId, CancellationToken.None))!;
    }

    [Fact]
    public async Task Close_ReturnsConflict_WhenAlreadyClosed()
    {
        var peer = await Register("machine-close");
        var createResult = await _pads.CreateAsync(new CreateScratchpadRequest
        {
            Type = "review",
            Title = "double-close test",
            RepoRoot = "/repo",
            CreatedByPeerId = peer.PeerId,
        }, CancellationToken.None);
        Assert.True(createResult.IsSuccess);
        var padId = createResult.Value.ScratchpadId;
        var req = new CloseScratchpadRequest { Resolution = "done" };

        // First close — must succeed
        var first = await _pads.CloseAsync(padId, req, CancellationToken.None);
        Assert.True(first.IsSuccess);

        // Second close — must fail with ConflictError
        var second = await _pads.CloseAsync(padId, req, CancellationToken.None);
        Assert.True(second.IsFailed);
        Assert.Contains(second.Errors, e => e is ConflictError);
    }

    [Fact]
    public async Task Close_EmitsExactlyOneClosedEvent_WhenCalledTwice()
    {
        var peer = await Register("machine-close2");
        var createResult = await _pads.CreateAsync(new CreateScratchpadRequest
        {
            Type = "review",
            Title = "single-event test",
            RepoRoot = "/repo",
            CreatedByPeerId = peer.PeerId,
        }, CancellationToken.None);
        Assert.True(createResult.IsSuccess);
        var padId = createResult.Value.ScratchpadId;
        var req = new CloseScratchpadRequest { Resolution = "done" };

        await _pads.CloseAsync(padId, req, CancellationToken.None);
        await _pads.CloseAsync(padId, req, CancellationToken.None); // second call is a no-op (returns Fail)

        // Exactly one scratchpad.closed event should exist for this pad
        var allEvents = await _events.GetSinceAsync(0, 1000, CancellationToken.None);
        var closedEvents = allEvents.Count(e => e.Type == EventTypes.ScratchpadClosed
                                                && e.Payload.Contains(padId));
        Assert.Equal(1, closedEvents);
    }

    private async Task<Peer> InsertLivePeer(string peerId, string machineId) =>
        await _peers.InsertAsync(new Peer
        {
            PeerId = peerId,
            Tool = "claude-code",
            MachineId = machineId,
            RepoRoot = "/repo",
            Branch = "main",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(2),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

    public void Dispose() => _db.Dispose();
}
