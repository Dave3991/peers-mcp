using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.Broker.Tests.Services;

/// <summary>
/// The broker-side contract of the channel model: scoped names, mention resolution, quiet-whisper
/// visibility, and the inbox rows a mention owes. Read filtering is THE quiet property — a whisper
/// must be absent from other viewers' reads, present for author and mentions.
/// </summary>
public sealed class ChannelServiceTests : IDisposable
{
    private readonly TestDb _db = new();

    private (PeerRepository peerRepo, PeerService peerSvc, EventService events, ChannelService channels) Create()
    {
        var peerRepo = new PeerRepository(_db.Factory);
        var eventRepo = new EventRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var events = new EventService(eventRepo, peerRepo, inboxRepo);
        var channels = new ChannelService(new ChannelRepository(_db.Factory), peerRepo, events, new RecipientResolver(peerRepo));
        return (peerRepo, new PeerService(peerRepo, events, channels), events, channels);
    }

    private async Task<string> RegisterAsync(PeerService svc, string machine, string repo = "/repo", string? branch = "main")
    {
        var resp = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = machine, RepoRoot = repo, Branch = branch,
        }, CancellationToken.None);
        return resp.PeerId;
    }

    [Fact]
    public async Task Create_DuplicateNameInSameScope_Fails_ButOtherScopeSucceeds()
    {
        var (_, peerSvc, _, channels) = Create();
        var me = await RegisterAsync(peerSvc, "m1");

        var first = await channels.CreateAsync(new CreateChannelRequest { Name = "deploy", RepoRoot = "/repo", CreatedByPeerId = me }, CancellationToken.None);
        Assert.True(first.IsSuccess);

        var dup = await channels.CreateAsync(new CreateChannelRequest { Name = "deploy", RepoRoot = "/repo", CreatedByPeerId = me }, CancellationToken.None);
        Assert.True(dup.IsFailed);

        var otherScope = await channels.CreateAsync(new CreateChannelRequest { Name = "deploy", RepoRoot = "/other", CreatedByPeerId = me }, CancellationToken.None);
        Assert.True(otherScope.IsSuccess);
    }

    private EventService CreateEvents() =>
        new(new EventRepository(_db.Factory), new PeerRepository(_db.Factory), new InboxRepository(_db.Factory));

    [Fact]
    public async Task Send_LoudNoMentions_VisibleToEveryViewer_NoInboxRows()
    {
        var (peerRepo, peerSvc, events, channels) = Create();
        var author = await RegisterAsync(peerSvc, "m-author");
        var other = await RegisterAsync(peerSvc, "m-other");

        var chan = (await channels.CreateAsync(new CreateChannelRequest { Name = "general", RepoRoot = "/repo", CreatedByPeerId = author }, CancellationToken.None)).Value;
        var sent = await channels.SendAsync(chan.ChannelId, new SendMessageRequest { AuthorPeerId = author, Content = "hello all" }, CancellationToken.None);
        Assert.True(sent.IsSuccess);

        var authorView = await channels.ReadAsync(chan.ChannelId, viewer: author, 0, 100, CancellationToken.None);
        var otherView = await channels.ReadAsync(chan.ChannelId, viewer: other, 0, 100, CancellationToken.None);
        var anonView = await channels.ReadAsync(chan.ChannelId, viewer: null, 0, 100, CancellationToken.None);

        Assert.Single(authorView!.Messages);
        Assert.Single(otherView!.Messages);
        Assert.Single(anonView!.Messages);

        var inbox = new InboxRepository(_db.Factory);
        Assert.Empty(await inbox.GetUnackedAsync(other, ct: CancellationToken.None));
    }

    [Fact]
    public async Task Send_QuietWhisper_HiddenFromOthers_InvisibleWithoutMention()
    {
        var (peerRepo, peerSvc, events, channels) = Create();
        var author = await RegisterAsync(peerSvc, "m-author");
        var mentioned = await RegisterAsync(peerSvc, "m-mentioned");
        var outsider = await RegisterAsync(peerSvc, "m-outsider");

        var chan = (await channels.CreateAsync(new CreateChannelRequest { Name = "general", RepoRoot = "/repo", CreatedByPeerId = author }, CancellationToken.None)).Value;

        var sent = await channels.SendAsync(chan.ChannelId, new SendMessageRequest
        {
            AuthorPeerId = author,
            Content = "psst, review my diff",
            To = [mentioned],
            Quiet = true,
        }, CancellationToken.None);
        Assert.True(sent.IsSuccess);
        var response = Assert.IsType<SendMessageResponse>(sent.Value);
        Assert.Equal([mentioned], response.Mentions);

        // Author and mentioned see it; outsider does not. Anonymous sees neither.
        Assert.Single((await channels.ReadAsync(chan.ChannelId, author, 0, 100, CancellationToken.None))!.Messages);
        Assert.Single((await channels.ReadAsync(chan.ChannelId, mentioned, 0, 100, CancellationToken.None))!.Messages);
        Assert.Empty((await channels.ReadAsync(chan.ChannelId, outsider, 0, 100, CancellationToken.None))!.Messages);
        Assert.Empty((await channels.ReadAsync(chan.ChannelId, null, 0, 100, CancellationToken.None))!.Messages);

        // The whisper landed in exactly the mentioned peer's inbox. Roster churn (peer.registered)
        // also lands there, so filter to the message event this test is about.
        var inbox = new InboxRepository(_db.Factory);
        Assert.Single(await MessagesFor(inbox, mentioned));
        Assert.Empty(await MessagesFor(inbox, outsider));
        _ = peerRepo; _ = events;
    }

    private static async Task<List<InboxItem>> MessagesFor(InboxRepository inbox, string peerId) =>
        (await inbox.GetUnackedAsync(peerId, ct: CancellationToken.None))
            .Where(i => i.Event.Type == Peers.Shared.Models.EventTypes.ChannelMessageAdded)
            .ToList();

    [Fact]
    public async Task Send_QuietWithoutMentions_Rejected()
    {
        var (_, peerSvc, _, channels) = Create();
        var author = await RegisterAsync(peerSvc, "m-q");
        var chan = (await channels.CreateAsync(new CreateChannelRequest { Name = "c", RepoRoot = "/repo", CreatedByPeerId = author }, CancellationToken.None)).Value;

        var result = await channels.SendAsync(chan.ChannelId, new SendMessageRequest
        {
            AuthorPeerId = author,
            Content = "invisible forever",
            Quiet = true,
        }, CancellationToken.None);

        Assert.True(result.IsFailed);
    }

    [Fact]
    public async Task Read_SinceSeq_PagesForwardWithoutRepeats()
    {
        var (_, peerSvc, _, channels) = Create();
        var author = await RegisterAsync(peerSvc, "m-seq");
        var chan = (await channels.CreateAsync(new CreateChannelRequest { Name = "seq", RepoRoot = "/repo", CreatedByPeerId = author }, CancellationToken.None)).Value;

        foreach (var i in Enumerable.Range(0, 3))
            await channels.SendAsync(chan.ChannelId, new SendMessageRequest { AuthorPeerId = author, Content = $"m{i}" }, CancellationToken.None);

        var page1 = await channels.ReadAsync(chan.ChannelId, author, 0, 2, CancellationToken.None);
        Assert.Equal(2, page1!.Messages.Length);
        Assert.True(page1.Truncated);

        var lastSeq = page1.Messages.Last().Seq;
        var page2 = await channels.ReadAsync(chan.ChannelId, author, lastSeq, 2, CancellationToken.None);
        var msg = Assert.Single(page2!.Messages);
        Assert.Equal("m2", msg.Content);
    }

    // A retried send is one message. The key had existed on the request since the scratchpad era while
    // nothing read it, so a provider retry posted a second review round nobody asked for.
    [Fact]
    public async Task Send_SameIdempotencyKey_ReturnsTheFirstMessage_AndDeliversOnce()
    {
        var (peerRepo, peerSvc, events, channels) = Create();
        var author = await RegisterAsync(peerSvc, "m-idem");
        var reviewer = await RegisterAsync(peerSvc, "m-idem-reviewer");
        var chan = (await channels.CreateAsync(new CreateChannelRequest { Name = "review", RepoRoot = "/repo", CreatedByPeerId = author }, CancellationToken.None)).Value;

        SendMessageRequest Round() => new()
        {
            AuthorPeerId = author,
            Content = "PLAN round 1",
            To = [reviewer],
            IdempotencyKey = "round-1",
        };

        var first = (await channels.SendAsync(chan.ChannelId, Round(), CancellationToken.None)).Value!;
        var retry = (await channels.SendAsync(chan.ChannelId, Round(), CancellationToken.None)).Value!;

        Assert.False(first.Deduplicated);
        Assert.True(retry.Deduplicated);
        Assert.Equal(first.MessageId, retry.MessageId);
        Assert.Equal(first.Seq, retry.Seq);
        // Delivery still answers the retry's real question — who is live now.
        Assert.Equal([reviewer], retry.Delivery!.OnlinePeerIds);

        // One row, one inbox item: the reviewer must not see the same round twice.
        var read = await channels.ReadAsync(chan.ChannelId, author, 0, 100, CancellationToken.None);
        Assert.Single(read!.Messages);
        Assert.Single(await MessagesFor(new InboxRepository(_db.Factory), reviewer));
    }

    [Fact]
    public async Task Send_DifferentKeys_AndNoKeyAtAll_StillPostEveryMessage()
    {
        var (_, peerSvc, _, channels) = Create();
        var author = await RegisterAsync(peerSvc, "m-keys");
        var chan = (await channels.CreateAsync(new CreateChannelRequest { Name = "rounds", RepoRoot = "/repo", CreatedByPeerId = author }, CancellationToken.None)).Value;

        await channels.SendAsync(chan.ChannelId, new SendMessageRequest { AuthorPeerId = author, Content = "r1", IdempotencyKey = "round-1" }, CancellationToken.None);
        await channels.SendAsync(chan.ChannelId, new SendMessageRequest { AuthorPeerId = author, Content = "r2", IdempotencyKey = "round-2" }, CancellationToken.None);
        await channels.SendAsync(chan.ChannelId, new SendMessageRequest { AuthorPeerId = author, Content = "chatter" }, CancellationToken.None);
        await channels.SendAsync(chan.ChannelId, new SendMessageRequest { AuthorPeerId = author, Content = "chatter" }, CancellationToken.None);

        var read = await channels.ReadAsync(chan.ChannelId, author, 0, 100, CancellationToken.None);
        Assert.Equal(["r1", "r2", "chatter", "chatter"], read!.Messages.Select(m => m.Content).ToArray());
    }

    // The key is per channel: the same round id in two review threads is two messages.
    [Fact]
    public async Task Send_SameKeyInAnotherChannel_IsItsOwnMessage()
    {
        var (_, peerSvc, _, channels) = Create();
        var author = await RegisterAsync(peerSvc, "m-scope");
        var left = (await channels.CreateAsync(new CreateChannelRequest { Name = "left", RepoRoot = "/repo", CreatedByPeerId = author }, CancellationToken.None)).Value;
        var right = (await channels.CreateAsync(new CreateChannelRequest { Name = "right", RepoRoot = "/repo", CreatedByPeerId = author }, CancellationToken.None)).Value;

        var here = (await channels.SendAsync(left.ChannelId, new SendMessageRequest { AuthorPeerId = author, Content = "r1", IdempotencyKey = "round-1" }, CancellationToken.None)).Value!;
        var there = (await channels.SendAsync(right.ChannelId, new SendMessageRequest { AuthorPeerId = author, Content = "r1", IdempotencyKey = "round-1" }, CancellationToken.None)).Value!;

        Assert.False(there.Deduplicated);
        Assert.NotEqual(here.MessageId, there.MessageId);
    }

    [Fact]
    public async Task RemoveMessages_DeletesOnlyMatchingRowsAndDeliveries_WithoutReusingSequenceOrKey()
    {
        var (_, peers, _, channels) = Create();
        var author = await RegisterAsync(peers, "remove-author");
        var recipient = await RegisterAsync(peers, "remove-recipient");
        var left = (await channels.CreateAsync(new CreateChannelRequest
            { Name = "left", CreatedByPeerId = author }, CancellationToken.None)).Value;
        var right = (await channels.CreateAsync(new CreateChannelRequest
            { Name = "right", CreatedByPeerId = author }, CancellationToken.None)).Value;
        var kept = (await channels.SendAsync(left.ChannelId, new SendMessageRequest
            { AuthorPeerId = author, Content = "keep", To = [recipient] }, CancellationToken.None)).Value!;
        var removed = (await channels.SendAsync(left.ChannelId, new SendMessageRequest
            { AuthorPeerId = author, Content = "remove", Quiet = true, To = [recipient], IdempotencyKey = "key" },
            CancellationToken.None)).Value!;
        var foreign = (await channels.SendAsync(right.ChannelId, new SendMessageRequest
            { AuthorPeerId = author, Content = "other channel", To = [recipient] }, CancellationToken.None)).Value!;
        var request = new RemoveMessagesRequest
            { MessageIds = [removed.MessageId, removed.MessageId, foreign.MessageId, "missing"] };

        Assert.Equal(1, (await channels.RemoveMessagesAsync(left.ChannelId, request, CancellationToken.None)).Value.DeletedMessages);
        Assert.Equal(0, (await channels.RemoveMessagesAsync(left.ChannelId, request, CancellationToken.None)).Value.DeletedMessages);
        Assert.Equal(kept.MessageId, Assert.Single((await channels.ReadAsync(left.ChannelId, author, 0, 100, CancellationToken.None))!.Messages).MessageId);
        Assert.Equal(foreign.MessageId, Assert.Single((await channels.ReadAsync(right.ChannelId, author, 0, 100, CancellationToken.None))!.Messages).MessageId);

        var inbox = await MessagesFor(new InboxRepository(_db.Factory), recipient);
        Assert.Equal(2, inbox.Count);
        Assert.DoesNotContain(inbox, item => item.Event.Payload.Contains(removed.MessageId));
        var events = await new EventRepository(_db.Factory).GetSinceAsync(0);
        Assert.DoesNotContain(events, evt => evt.Type == Peers.Shared.Models.EventTypes.ChannelMessageAdded
            && evt.Payload.Contains(removed.MessageId));

        var next = (await channels.SendAsync(left.ChannelId, new SendMessageRequest
            { AuthorPeerId = author, Content = "replacement", IdempotencyKey = "key" }, CancellationToken.None)).Value!;
        Assert.False(next.Deduplicated);
        Assert.True(next.Seq > removed.Seq);
        Assert.Equal(next.MessageId, Assert.Single((await channels.ReadAsync(left.ChannelId, author, removed.Seq, 100, CancellationToken.None))!.Messages).MessageId);
    }

    [Fact]
    public async Task Clear_KeepsChannel_RemovesHistoryAndInbox_AndPreservesCursor()
    {
        var (_, peers, _, channels) = Create();
        var author = await RegisterAsync(peers, "clear-author");
        var recipient = await RegisterAsync(peers, "clear-recipient");
        var channel = (await channels.CreateAsync(new CreateChannelRequest
            { Name = "clear", CreatedByPeerId = author }, CancellationToken.None)).Value;
        await channels.SendAsync(channel.ChannelId, new SendMessageRequest
            { AuthorPeerId = author, Content = "loud", To = [recipient] }, CancellationToken.None);
        var last = (await channels.SendAsync(channel.ChannelId, new SendMessageRequest
            { AuthorPeerId = author, Content = "quiet", Quiet = true, To = [recipient] }, CancellationToken.None)).Value!;

        Assert.Equal(2, (await channels.ClearAsync(channel.ChannelId, CancellationToken.None)).Value.DeletedMessages);
        Assert.Equal(0, (await channels.ClearAsync(channel.ChannelId, CancellationToken.None)).Value.DeletedMessages);
        Assert.Empty((await channels.ReadAsync(channel.ChannelId, recipient, 0, 100, CancellationToken.None))!.Messages);
        Assert.Equal(0, Assert.Single((await channels.ListAsync(null, 100, CancellationToken.None)).Channels).MessageCount);
        Assert.Empty(await MessagesFor(new InboxRepository(_db.Factory), recipient));
        Assert.DoesNotContain(await new EventRepository(_db.Factory).GetSinceAsync(0),
            evt => evt.Type == Peers.Shared.Models.EventTypes.ChannelMessageAdded);

        var next = (await channels.SendAsync(channel.ChannelId, new SendMessageRequest
            { AuthorPeerId = author, Content = "after clear" }, CancellationToken.None)).Value!;
        Assert.True(next.Seq > last.Seq);
        Assert.Equal("after clear", Assert.Single((await channels.ReadAsync(channel.ChannelId, author, last.Seq, 100, CancellationToken.None))!.Messages).Content);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Removal_FailedCommit_RetainsMessageAndInbox(bool clear)
    {
        var (_, peers, _, channels) = Create();
        var author = await RegisterAsync(peers, "rollback-author");
        var recipient = await RegisterAsync(peers, "rollback-recipient");
        var channel = (await channels.CreateAsync(new CreateChannelRequest
            { Name = "rollback", CreatedByPeerId = author }, CancellationToken.None)).Value;
        var sent = (await channels.SendAsync(channel.ChannelId, new SendMessageRequest
            { AuthorPeerId = author, Content = "retain on failure", To = [recipient] }, CancellationToken.None)).Value!;
        await _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            var type = clear ? Peers.Shared.Models.EventTypes.ChannelCleared : Peers.Shared.Models.EventTypes.ChannelMessageRemoved;
            cmd.CommandText = $"""
                CREATE TRIGGER fail_removal BEFORE INSERT ON events WHEN NEW.type = '{type}'
                BEGIN SELECT RAISE(ABORT, 'injected failure'); END;
                """;
            await cmd.ExecuteNonQueryAsync();
        });

        await Assert.ThrowsAsync<Microsoft.Data.Sqlite.SqliteException>(async () =>
        {
            if (clear)
                await channels.ClearAsync(channel.ChannelId, CancellationToken.None);
            else
                await channels.RemoveMessagesAsync(channel.ChannelId,
                    new RemoveMessagesRequest { MessageIds = [sent.MessageId] }, CancellationToken.None);
        });
        Assert.Equal(sent.MessageId, Assert.Single((await channels.ReadAsync(channel.ChannelId, author, 0, 100, CancellationToken.None))!.Messages).MessageId);
        Assert.Contains(sent.MessageId, Assert.Single(await MessagesFor(new InboxRepository(_db.Factory), recipient)).Event.Payload);
    }

    public void Dispose() => _db.Dispose();
}
