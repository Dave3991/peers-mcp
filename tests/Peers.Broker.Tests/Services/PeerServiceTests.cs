using System.Text.Json;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Broker.Tests.Services;

public sealed class PeerServiceTests : IDisposable
{
    private readonly TestDb _db = new();

    private (PeerRepository peerRepo, PeerService svc) CreateServiceWithRepo()
    {
        var eventRepo = new EventRepository(_db.Factory);
        var peerRepo = new PeerRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var eventService = new EventService(eventRepo, peerRepo, inboxRepo);
        return (peerRepo, new PeerService(peerRepo, eventService, new ChannelService(new ChannelRepository(_db.Factory), peerRepo, eventService, new RecipientResolver(peerRepo))));
    }

    private PeerService CreateService()
    {
        var eventRepo = new EventRepository(_db.Factory);
        var peerRepo = new PeerRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var eventService = new EventService(eventRepo, peerRepo, inboxRepo);
        return new PeerService(peerRepo, eventService, new ChannelService(new ChannelRepository(_db.Factory), peerRepo, eventService, new RecipientResolver(peerRepo)));
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

    // T4 — worktree_root & capabilities

    [Fact]
    public async Task Register_TwoWorktrees_CreateDistinctIdentities()
    {
        var svc = CreateService();

        var respA = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m", RepoRoot = "/r", Branch = "b", WorktreeRoot = "/wt/a",
        }, CancellationToken.None);
        var respB = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m", RepoRoot = "/r", Branch = "b", WorktreeRoot = "/wt/b",
        }, CancellationToken.None);

        Assert.NotEqual(respA.PeerId, respB.PeerId);
        Assert.False(respA.Resumed);
        Assert.False(respB.Resumed);
    }

    [Fact]
    public async Task Register_WorktreeDisambiguatesResume()
    {
        var (peerRepo, svc) = CreateServiceWithRepo();

        // Register both worktrees — creates two distinct active peers
        var respA = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m", RepoRoot = "/r", Branch = "b", WorktreeRoot = "/wt/a",
        }, CancellationToken.None);
        var respB = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m", RepoRoot = "/r", Branch = "b", WorktreeRoot = "/wt/b",
        }, CancellationToken.None);

        // Expire only peer A so that the /wt/a identity has exactly one expired match (peer A)
        // and no active match (required for implicit resume to trigger)
        await peerRepo.ExpireByIdAsync(respA.PeerId, CancellationToken.None);

        // Re-register with A's worktree identity (no ResumePeerId) — should resume A, not B
        var resumed = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m", RepoRoot = "/r", Branch = "b", WorktreeRoot = "/wt/a",
        }, CancellationToken.None);

        Assert.True(resumed.Resumed);
        Assert.Equal(respA.PeerId, resumed.PeerId);
        Assert.NotEqual(respB.PeerId, resumed.PeerId);
    }

    // A session that changes role must be visible to peers://peers subscribers, and an idle session must
    // stay silent: the background heartbeat fires every 30s with an all-null request, so emitting on every
    // heartbeat would put every peer at a permanent 2 events/min floor.
    [Fact]
    public async Task Heartbeat_EmitsPeerUpdated_OnlyWhenAStatusFieldIsSupplied()
    {
        var eventRepo = new EventRepository(_db.Factory);
        var peerRepo = new PeerRepository(_db.Factory);
        var inboxRepo = new InboxRepository(_db.Factory);
        var svc = new PeerService(peerRepo, new EventService(eventRepo, peerRepo, inboxRepo),
            new ChannelService(new ChannelRepository(_db.Factory), peerRepo, new EventService(eventRepo, peerRepo, inboxRepo), new RecipientResolver(peerRepo)));

        var resp = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m", RepoRoot = "/r", Role = "implementing",
        }, CancellationToken.None);
        var cursor = resp.LatestEventId;

        // The 30s background heartbeat: renews the lease, changes nothing, emits nothing.
        Assert.True((await svc.HeartbeatAsync(resp.PeerId, new HeartbeatRequest(), CancellationToken.None)).IsSuccess);
        Assert.Empty(await eventRepo.GetSinceAsync(cursor, 100, CancellationToken.None));

        // A deliberate status change: exactly one peer.updated.
        Assert.True((await svc.HeartbeatAsync(resp.PeerId,
            new HeartbeatRequest { Role = "reviewing" }, CancellationToken.None)).IsSuccess);

        var emitted = await eventRepo.GetSinceAsync(cursor, 100, CancellationToken.None);
        var evt = Assert.Single(emitted);
        Assert.Equal(EventTypes.PeerUpdated, evt.Type);
        Assert.Equal(resp.PeerId, JsonDocument.Parse(evt.Payload).RootElement.GetProperty("peer_id").GetString());

        // Resource-change signal, not a directed message: no inbox row, so peer_inbox stays clean.
        Assert.Empty(await inboxRepo.GetUnackedAsync(resp.PeerId, 100, CancellationToken.None));
    }

    [Fact]
    public async Task Heartbeat_CapabilitiesRoundTrip_AndCoalesceOnNull()
    {
        var (peerRepo, svc) = CreateServiceWithRepo();

        var resp = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "m", RepoRoot = "/r",
            Capabilities = JsonSerializer.Serialize(new[] { "x" }),
        }, CancellationToken.None);
        var peerId = resp.PeerId;

        // Update capabilities via heartbeat
        var hbResult = await svc.HeartbeatAsync(peerId, new HeartbeatRequest
        {
            Capabilities = JsonSerializer.Serialize(new[] { "y" }),
        }, CancellationToken.None);
        Assert.True(hbResult.IsSuccess);

        var peer = await peerRepo.GetAsync(peerId, CancellationToken.None);
        Assert.NotNull(peer);
        Assert.Equal(JsonSerializer.Serialize(new[] { "y" }), peer!.Capabilities);

        // Heartbeat with null capabilities — COALESCE should leave existing value intact
        var hbResult2 = await svc.HeartbeatAsync(peerId, new HeartbeatRequest(), CancellationToken.None);
        Assert.True(hbResult2.IsSuccess);

        var peer2 = await peerRepo.GetAsync(peerId, CancellationToken.None);
        Assert.NotNull(peer2);
        Assert.Equal(JsonSerializer.Serialize(new[] { "y" }), peer2!.Capabilities);
    }

    // Resume matrix: explicit resume with mismatched MachineId → no resume, new peer minted.
    [Fact]
    public async Task Register_ExplicitResume_WrongMachine_DoesNotResume()
    {
        var svc = CreateService();
        var first = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "machine-orig", RepoRoot = "/repo", Branch = "main",
        }, CancellationToken.None);

        await svc.DeregisterAsync(first.PeerId, CancellationToken.None);

        var second = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "DIFFERENT-machine", RepoRoot = "/repo", Branch = "main",
            ResumePeerId = first.PeerId,
        }, CancellationToken.None);

        Assert.False(second.Resumed);
        Assert.NotEqual(first.PeerId, second.PeerId);
    }

    // Resume matrix: 2 expired peers with same identity → implicit register mints new peer (count != 1).
    [Fact]
    public async Task Register_ImplicitResume_TwoExpiredCandidates_DoesNotResume()
    {
        var (peerRepo, svc) = CreateServiceWithRepo();

        const string tool = "claude-code";
        const string machineId = "machine-multi";
        const string repoRoot = "/repo/multi";
        const string branch = "main";

        // Insert two distinct expired peers with the same identity via raw SQL
        // (same approach as RetentionTests.InsertExpiredPeerAsync)
        await _db.Factory.WriteAsync(async conn =>
        {
            foreach (var peerId in new[] { "multi-expired-1", "multi-expired-2" })
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = """
                    INSERT INTO peers (peer_id, tool, machine_id, repo_root, branch,
                        lease_expires_at, expired_at, created_at, updated_at)
                    VALUES (@id, @tool, @machine, @repo, @branch,
                        datetime('now', '-1 minute'), datetime('now', '-1 hour'), datetime('now'), datetime('now'))
                    """;
                cmd.Parameters.AddWithValue("@id", peerId);
                cmd.Parameters.AddWithValue("@tool", tool);
                cmd.Parameters.AddWithValue("@machine", machineId);
                cmd.Parameters.AddWithValue("@repo", repoRoot);
                cmd.Parameters.AddWithValue("@branch", branch);
                await cmd.ExecuteNonQueryAsync();
            }
            return 0;
        }, CancellationToken.None);

        var resp = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = tool, MachineId = machineId, RepoRoot = repoRoot, Branch = branch,
        }, CancellationToken.None);

        Assert.False(resp.Resumed);
        Assert.NotEqual("multi-expired-1", resp.PeerId);
        Assert.NotEqual("multi-expired-2", resp.PeerId);
    }

    // Resume matrix: active peer with same identity → implicit register mints new peer (active session present).
    [Fact]
    public async Task Register_ImplicitResume_ActiveMatchExists_DoesNotResume()
    {
        var svc = CreateService();

        var identity = new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "machine-active", RepoRoot = "/repo/active", Branch = "main",
        };

        // Register first (stays active)
        var first = await svc.RegisterAsync(identity, CancellationToken.None);
        Assert.False(first.Resumed);

        // Register again with same identity — active match blocks resume
        var second = await svc.RegisterAsync(identity, CancellationToken.None);

        Assert.False(second.Resumed);
        Assert.NotEqual(first.PeerId, second.PeerId);
    }

    // --- Join announcements: a fresh join posts a loud row on every repo-matched / global channel
    // --- and wakes every member exactly once. Resume stays byte-identical to pre-change behavior.

    private (PeerRepository peerRepo, EventService events, ChannelService channels) CreateChannels()
    {
        var peerRepo = new PeerRepository(_db.Factory);
        var events = new EventService(new EventRepository(_db.Factory), peerRepo, new InboxRepository(_db.Factory));
        var channels = new ChannelService(new ChannelRepository(_db.Factory), peerRepo, events, new RecipientResolver(peerRepo));
        return (peerRepo, events, channels);
    }

    private PeerService SvcFor(PeerRepository peerRepo, EventService events, ChannelService channels)
        => new(peerRepo, events, channels);

    private async Task<string> ChannelAsync(ChannelService channels, string creatorId, string name, string? repoRoot)
    {
        var res = await channels.CreateAsync(new CreateChannelRequest { Name = name, RepoRoot = repoRoot, CreatedByPeerId = creatorId }, CancellationToken.None);
        Assert.True(res.IsSuccess);
        return res.Value.ChannelId;
    }

    private Task<long> CountUnackedInboxAsync(string peerId, string eventType = "peer.registered")
        => _db.Factory.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT COUNT(*) FROM peer_inbox i
                JOIN events e ON e.event_id = i.event_id
                WHERE i.peer_id = @id AND i.acked_at IS NULL AND e.type = @type
                """;
            cmd.Parameters.AddWithValue("@id", peerId);
            cmd.Parameters.AddWithValue("@type", eventType);
            return Convert.ToInt64(await cmd.ExecuteScalarAsync());
        }, CancellationToken.None);

    private Task<bool> HasInboxNamingAsync(string peerId, string otherPeerId)
        => _db.Factory.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT COUNT(*) FROM peer_inbox i
                JOIN events e ON e.event_id = i.event_id
                WHERE i.peer_id = @id AND i.acked_at IS NULL AND e.type = 'peer.registered'
                  AND e.payload LIKE '%' || @other || '%'
                """;
            cmd.Parameters.AddWithValue("@id", peerId);
            cmd.Parameters.AddWithValue("@other", otherPeerId);
            return Convert.ToInt64(await cmd.ExecuteScalarAsync()) > 0;
        }, CancellationToken.None);

    private Task<long> CountChannelMessagesAsync(string channelId)
        => _db.Factory.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM channel_messages WHERE channel_id = @id";
            cmd.Parameters.AddWithValue("@id", channelId);
            return Convert.ToInt64(await cmd.ExecuteScalarAsync());
        }, CancellationToken.None);

    [Fact]
    public async Task FreshJoin_GlobalChannel_NotifyCrossRepoLivePeer()
    {
        var (peerRepo, events, channels) = CreateChannels();
        var svc = SvcFor(peerRepo, events, channels);

        var a = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "claude-code", MachineId = "m-a", RepoRoot = "/repo-a", Branch = "main" }, CancellationToken.None);
        await ChannelAsync(channels, a.PeerId, "general", repoRoot: null);

        var b = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "codex", MachineId = "m-b", RepoRoot = "/repo-b", Branch = "main" }, CancellationToken.None);

        Assert.True(await HasInboxNamingAsync(a.PeerId, b.PeerId));
    }

    [Fact]
    public async Task FreshJoin_TwoMatchingChannels_OneWakePerPerson()
    {
        var (peerRepo, events, channels) = CreateChannels();
        var svc = SvcFor(peerRepo, events, channels);

        var member = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "claude-code", MachineId = "m-member", RepoRoot = "/other-repo", Branch = "main" }, CancellationToken.None);
        await ChannelAsync(channels, member.PeerId, "scoped", "/repo");
        await ChannelAsync(channels, member.PeerId, "global", repoRoot: null);

        var joiner = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "codex", MachineId = "m-joiner", RepoRoot = "/repo", Branch = "main" }, CancellationToken.None);
        Assert.False(joiner.Resumed);

        Assert.Equal(1, await CountUnackedInboxAsync(member.PeerId));
        Assert.Equal(0, await CountUnackedInboxAsync(joiner.PeerId));
    }

    [Fact]
    public async Task Resume_NoBoardRows_FanoutUnchanged()
    {
        var (peerRepo, events, channels) = CreateChannels();
        var svc = SvcFor(peerRepo, events, channels);

        var first = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "claude-code", MachineId = "m-x", RepoRoot = "/r", Branch = "main" }, CancellationToken.None);
        await svc.DeregisterAsync(first.PeerId, CancellationToken.None);

        var rowsBefore = await CountChannelMessagesTotal();
        var second = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "claude-code", MachineId = "m-x", RepoRoot = "/r", Branch = "main" }, CancellationToken.None);

        Assert.True(second.Resumed);
        Assert.Equal(rowsBefore, await CountChannelMessagesTotal());
    }

    [Fact]
    public async Task NoChannels_BehaviorUnchanged()
    {
        var (peerRepo, events, channels) = CreateChannels();
        var svc = SvcFor(peerRepo, events, channels);

        var a = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "claude-code", MachineId = "m-a", RepoRoot = "/repo", Branch = "main" }, CancellationToken.None);
        var b = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "codex", MachineId = "m-b", RepoRoot = "/repo", Branch = "main" }, CancellationToken.None);

        Assert.Equal(1, await CountUnackedInboxAsync(a.PeerId));
        Assert.True(await HasInboxNamingAsync(a.PeerId, b.PeerId));
    }

    [Fact]
    public async Task OtherRepoChannel_MembersUntouched()
    {
        var (peerRepo, events, channels) = CreateChannels();
        var svc = SvcFor(peerRepo, events, channels);

        var member = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "claude-code", MachineId = "m-member", RepoRoot = "/other-repo", Branch = "main" }, CancellationToken.None);
        await ChannelAsync(channels, member.PeerId, "other-board", "/other-repo");

        await svc.RegisterAsync(new RegisterPeerRequest { Tool = "codex", MachineId = "m-joiner", RepoRoot = "/repo", Branch = "main" }, CancellationToken.None);

        Assert.Equal(0, await CountUnackedInboxAsync(member.PeerId));
        Assert.Equal(0, await CountChannelMessagesAsync(await OnlyChannelId()));
    }

    [Fact]
    public async Task JoinRowSendFailure_DoesNotFailRegistration()
    {
        var (peerRepo, events, channels) = CreateChannels();
        var svc = SvcFor(peerRepo, events, channels);

        var author = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "claude-code", MachineId = "m-author", RepoRoot = "/join-repo", Branch = "main" }, CancellationToken.None);
        var victimId = await ChannelAsync(channels, author.PeerId, "victim", "/join-repo");
        var healthyId = await ChannelAsync(channels, author.PeerId, "healthy", "/join-repo");

        await _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                CREATE TRIGGER force_fail BEFORE INSERT ON channel_messages
                WHEN NEW.channel_id = '{victimId}'
                BEGIN SELECT RAISE(ABORT, 'forced'); END;
                """;
            await cmd.ExecuteNonQueryAsync();
        }, CancellationToken.None);

        var joiner = await svc.RegisterAsync(new RegisterPeerRequest { Tool = "codex", MachineId = "m-joiner", RepoRoot = "/join-repo", Branch = "main" }, CancellationToken.None);

        Assert.False(joiner.Resumed);
        Assert.Equal(1, await CountChannelMessagesAsync(healthyId));
        Assert.Equal(0, await CountChannelMessagesAsync(victimId));
        Assert.True(await HasInboxNamingAsync(author.PeerId, joiner.PeerId));
    }

    private Task<long> CountChannelMessagesTotal()
        => _db.Factory.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM channel_messages";
            return Convert.ToInt64(await cmd.ExecuteScalarAsync());
        }, CancellationToken.None);

    private Task<string> OnlyChannelId()
        => _db.Factory.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT channel_id FROM channels LIMIT 1";
            return (string)(await cmd.ExecuteScalarAsync())!;
        }, CancellationToken.None);

    public void Dispose() => _db.Dispose();

}
