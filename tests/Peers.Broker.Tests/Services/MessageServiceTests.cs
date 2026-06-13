using System.Text.Json;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Broker.Tests.Services;

public sealed class MessageServiceTests : IDisposable
{
    private readonly TestDb _db = new();
    private readonly PeerRepository _peers;
    private readonly InboxRepository _inbox;
    private readonly EventRepository _events;
    private readonly PeerService _peerService;
    private readonly MessageService _messages;

    public MessageServiceTests()
    {
        _peers = new PeerRepository(_db.Factory);
        _inbox = new InboxRepository(_db.Factory);
        _events = new EventRepository(_db.Factory);
        var eventService = new EventService(_events, _peers, _inbox);
        _peerService = new PeerService(_peers, eventService);
        _messages = new MessageService(new RecipientResolver(_peers), eventService, _peers);
    }

    [Fact]
    public async Task Send_DeliversToPeer_EventAndInboxBothWritten()
    {
        var sender = await Register("machine-a", "/repo", "main");
        var receiver = await Register("machine-b", "/repo", "main");

        var result = await _messages.SendAsync(new SendMessageRequest
        {
            FromPeerId = sender.PeerId,
            Targets = [receiver.PeerId],
            Content = "hi",
        }, CancellationToken.None);

        Assert.True(result.IsSuccess);
        var recipients = result.Value.Recipients!;
        Assert.Contains(receiver.PeerId, recipients.OnlinePeerIds);

        // Event emitted
        var evts = await _events.GetSinceAsync(0);
        var msgEvt = evts.SingleOrDefault(e => e.Type == EventTypes.PeerMessage);
        Assert.NotNull(msgEvt);

        // Payload content matches
        var payload = JsonDocument.Parse(msgEvt.Payload).RootElement;
        Assert.Equal("hi", payload.GetProperty("content").GetString());

        // recipient_peer_ids in payload == recipients passed to EmitAsync
        var recipientIds = payload.GetProperty("recipient_peer_ids").EnumerateArray()
            .Select(e => e.GetString())
            .ToArray();
        Assert.Contains(receiver.PeerId, recipientIds!);

        // Inbox delivery
        var inbox = await _inbox.GetUnackedAsync(receiver.PeerId, ct: CancellationToken.None);
        Assert.Single(inbox, i => i.Event.Type == EventTypes.PeerMessage);
    }

    [Fact]
    public async Task Send_ExcludesSelf_WhenTargetedByExplicitId()
    {
        var sender = await Register("machine-a", "/repo", "main");

        var result = await _messages.SendAsync(new SendMessageRequest
        {
            FromPeerId = sender.PeerId,
            Targets = [sender.PeerId],
            Content = "self-message",
        }, CancellationToken.None);

        Assert.True(result.IsSuccess);
        var recipients = result.Value.Recipients!;
        Assert.DoesNotContain(sender.PeerId, recipients.OnlinePeerIds);
        Assert.DoesNotContain(sender.PeerId, recipients.OfflinePeerIds);

        // No peer.message event emitted (short-circuit)
        var evts = await _events.GetSinceAsync(0);
        Assert.DoesNotContain(evts, e => e.Type == EventTypes.PeerMessage);

        // No inbox row for sender
        var inbox = await _inbox.GetUnackedAsync(sender.PeerId, ct: CancellationToken.None);
        Assert.Empty(inbox);
    }

    [Fact]
    public async Task Send_ExcludesSelf_WhenTargetedByRepoRoot()
    {
        const string repoRoot = "/shared-repo";
        var sender = await Register("machine-a", repoRoot, "main");
        var receiver = await Register("machine-b", repoRoot, "main");

        var result = await _messages.SendAsync(new SendMessageRequest
        {
            FromPeerId = sender.PeerId,
            Targets = [repoRoot],
            Content = "repo broadcast",
        }, CancellationToken.None);

        Assert.True(result.IsSuccess);
        var recipients = result.Value.Recipients!;

        // Receiver included, sender excluded
        Assert.Contains(receiver.PeerId, recipients.OnlinePeerIds);
        Assert.DoesNotContain(sender.PeerId, recipients.OnlinePeerIds);
        Assert.DoesNotContain(sender.PeerId, recipients.OfflinePeerIds);

        // Event was emitted (receiver gets it)
        var evts = await _events.GetSinceAsync(0);
        Assert.Contains(evts, e => e.Type == EventTypes.PeerMessage);

        // Receiver inbox has the message; sender inbox does not
        var receiverInbox = await _inbox.GetUnackedAsync(receiver.PeerId, ct: CancellationToken.None);
        Assert.Contains(receiverInbox, i => i.Event.Type == EventTypes.PeerMessage);

        var senderInbox = await _inbox.GetUnackedAsync(sender.PeerId, ct: CancellationToken.None);
        Assert.DoesNotContain(senderInbox, i => i.Event.Type == EventTypes.PeerMessage);
    }

    [Fact]
    public async Task Send_EmptyTargets_ShortCircuitsNoEventEmitted()
    {
        var sender = await Register("machine-a", "/repo", "main");

        var result = await _messages.SendAsync(new SendMessageRequest
        {
            FromPeerId = sender.PeerId,
            Targets = [],
            Content = "nobody home",
        }, CancellationToken.None);

        Assert.True(result.IsSuccess);

        // No peer.message event emitted
        var evts = await _events.GetSinceAsync(0);
        Assert.DoesNotContain(evts, e => e.Type == EventTypes.PeerMessage);
    }

    private async Task<Peer> Register(string machineId, string repoRoot, string branch)
    {
        var resp = await _peerService.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code",
            MachineId = machineId,
            RepoRoot = repoRoot,
            Branch = branch,
        }, CancellationToken.None);
        return (await _peers.GetAsync(resp.PeerId, CancellationToken.None))!;
    }

    public void Dispose() => _db.Dispose();
}
