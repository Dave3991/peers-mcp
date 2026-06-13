using System.Text.Json;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
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
        return (peerRepo, new PeerService(peerRepo, eventService));
    }

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

    public void Dispose() => _db.Dispose();
}
