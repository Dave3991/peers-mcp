using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Broker.Tests.Services;

public sealed class RecipientResolverTests : IDisposable
{
    private readonly TestDb _db = new();

    private (PeerRepository peers, RecipientResolver resolver) CreateResolver()
    {
        var peers = new PeerRepository(_db.Factory);
        return (peers, new RecipientResolver(peers));
    }

    private Task<Peer> InsertActivePeer(string peerId, string repoRoot = "/repo", string branch = "main", string machineId = "machine-x")
        => _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO peers (peer_id, tool, machine_id, repo_root, branch,
                    lease_expires_at, created_at, updated_at)
                VALUES (@id, 'claude-code', @machine, @repo, @branch,
                    datetime('now', '+5 minutes'), datetime('now'), datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", peerId);
            cmd.Parameters.AddWithValue("@machine", machineId);
            cmd.Parameters.AddWithValue("@repo", repoRoot);
            cmd.Parameters.AddWithValue("@branch", branch);
            await cmd.ExecuteNonQueryAsync();
            return new Peer
            {
                PeerId = peerId,
                Tool = "claude-code",
                MachineId = machineId,
                RepoRoot = repoRoot,
                Branch = branch,
                LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow,
            };
        }, CancellationToken.None);

    // Exact full id and unique prefix both resolve to the same online peer.
    [Fact]
    public async Task Resolve_ExactAndPrefixMatch_ReturnOnlinePeer()
    {
        var (_, resolver) = CreateResolver();

        // Use a unique-enough id that no prefix collision can occur
        var peer = await InsertActivePeer("resolvetest-exactprefix-abc123");

        // Exact match
        var exactResult = await resolver.ResolveAsync([peer.PeerId], null, CancellationToken.None);
        Assert.True(exactResult.IsSuccess);
        Assert.Contains(peer.PeerId, exactResult.Value.OnlinePeerIds);

        // Unique prefix (not a full peer id)
        var prefix = "resolvetest-exactprefix-";
        var prefixResult = await resolver.ResolveAsync([prefix], null, CancellationToken.None);
        Assert.True(prefixResult.IsSuccess);
        Assert.Contains(peer.PeerId, prefixResult.Value.OnlinePeerIds);
    }

    // Two active peers sharing a common prefix → GetByPrefixAsync returns null (ambiguous)
    // → the shared prefix lands in UnresolvedTargets (it's not a repo root either).
    [Fact]
    public async Task Resolve_AmbiguousPrefix_LandsInUnresolved()
    {
        var (_, resolver) = CreateResolver();

        // Two peers whose ids share the prefix "ambig-prefix-" but differ after it
        await InsertActivePeer("ambig-prefix-aaa");
        await InsertActivePeer("ambig-prefix-bbb");

        // The shared prefix is NOT a full peer id (would hit exact path) — use a distinct prefix
        var sharedPrefix = "ambig-prefix-";
        var result = await resolver.ResolveAsync([sharedPrefix], null, CancellationToken.None);
        Assert.True(result.IsSuccess);

        // Should not appear online or offline — must be unresolved
        Assert.DoesNotContain(sharedPrefix, result.Value.OnlinePeerIds);
        Assert.DoesNotContain(sharedPrefix, result.Value.OfflinePeerIds);
        Assert.Contains(sharedPrefix, result.Value.UnresolvedTargets);
    }

    // Repo-root fallback: when caller branch matches a peer's branch, that peer is preferred (online).
    [Fact]
    public async Task Resolve_RepoRootFallback_PrefersSameBranchPeer()
    {
        var (_, resolver) = CreateResolver();

        // Two peers in the same repo on different branches
        var mainPeer = await InsertActivePeer("repo-peer-main", repoRoot: "/r", branch: "main");
        var devPeer = await InsertActivePeer("repo-peer-dev", repoRoot: "/r", branch: "dev");

        // Resolve repo root with callerBranch = "main" — the main-branch peer should be preferred
        var result = await resolver.ResolveAsync(["/r"], callerBranch: "main", CancellationToken.None);
        Assert.True(result.IsSuccess);

        // The same-branch peer is online
        Assert.Contains(mainPeer.PeerId, result.Value.OnlinePeerIds);

        // The dev-branch peer is NOT in the same-branch preferred set (activeSameBranch > 0 → only those are used)
        Assert.DoesNotContain(devPeer.PeerId, result.Value.OnlinePeerIds);
    }

    public void Dispose() => _db.Dispose();
}
