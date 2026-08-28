using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.Broker.Tests.Services;

/// <summary>
/// Claims are the pre-edit answer to "is anyone else on this path". The properties that matter: a
/// conflict names the holder and writes nothing, overlap follows directory containment rather than
/// string prefixes, scope is the peer's own repo, and an abandoned session stops blocking anyone
/// without ever calling release.
/// </summary>
public sealed class ClaimServiceTests : IDisposable
{
    private readonly TestDb _db = new();

    private (PeerService peers, ClaimService claims) Create()
    {
        var peerRepo = new PeerRepository(_db.Factory);
        var events = new EventService(new EventRepository(_db.Factory), peerRepo, new InboxRepository(_db.Factory));
        var channels = new ChannelService(new ChannelRepository(_db.Factory), peerRepo, events, new RecipientResolver(peerRepo));
        return (new PeerService(peerRepo, events, channels), new ClaimService(new ClaimRepository(_db.Factory), peerRepo));
    }

    private static async Task<string> RegisterAsync(PeerService svc, string machine, string? repo = "/repo")
    {
        var resp = await svc.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = machine, RepoRoot = repo, Branch = "main",
        }, CancellationToken.None);
        return resp.PeerId;
    }

    private Task<ClaimPathsResponse> ClaimAsync(ClaimService claims, string peerId, params string[] paths) =>
        ClaimAsync(claims, peerId, null, paths);

    private static async Task<ClaimPathsResponse> ClaimAsync(ClaimService claims, string peerId, int? ttl, params string[] paths)
    {
        var result = await claims.ClaimAsync(new ClaimPathsRequest { PeerId = peerId, Paths = paths, TtlSeconds = ttl }, CancellationToken.None);
        Assert.True(result.IsSuccess, string.Join("; ", result.Errors.Select(e => e.Message)));
        return result.Value;
    }

    [Fact]
    public async Task Claim_SecondPeerOnTheSamePath_ConflictsAndNamesTheHolder()
    {
        var (peers, claims) = Create();
        var driver = await RegisterAsync(peers, "m-driver");
        var other = await RegisterAsync(peers, "m-other");

        var first = await ClaimAsync(claims, driver, "src/Peers.Mcp/Tools/ChannelTools.cs");
        Assert.Single(first.Granted);
        Assert.Empty(first.Conflicts);

        var second = await ClaimAsync(claims, other, "src/Peers.Mcp/Tools/ChannelTools.cs");

        Assert.Empty(second.Granted);
        var conflict = Assert.Single(second.Conflicts);
        Assert.Equal(driver, conflict.HolderPeerId);
        Assert.Equal("src/Peers.Mcp/Tools/ChannelTools.cs", conflict.HeldPath);
        // Nothing was written for the contested path: the holder still holds exactly one claim.
        var held = await claims.ListAsync("/repo", CancellationToken.None);
        Assert.Equal([driver], held.Claims.Select(c => c.PeerId).ToArray());
    }

    // The whole point of granting per path: one contested file must not block the three nobody wants.
    [Fact]
    public async Task Claim_PartiallyContested_GrantsTheRestAndReportsOnlyTheOverlap()
    {
        var (peers, claims) = Create();
        var driver = await RegisterAsync(peers, "m-driver");
        var other = await RegisterAsync(peers, "m-other");

        await ClaimAsync(claims, driver, "src/a.cs");
        var second = await ClaimAsync(claims, other, "src/a.cs", "src/b.cs", "tests/c.cs");

        Assert.Equal(["src/b.cs", "tests/c.cs"], second.Granted.Select(g => g.Path).Order().ToArray());
        Assert.Equal(["src/a.cs"], second.Conflicts.Select(c => c.Path).ToArray());
    }

    // Containment both ways, and NOT string prefixes: src/foo must never conflict with src/foobar.
    [Fact]
    public async Task Claim_DirectoryOverlap_ConflictsInBothDirections_ButSiblingPrefixesDoNot()
    {
        var (peers, claims) = Create();
        var holder = await RegisterAsync(peers, "m-holder");
        var other = await RegisterAsync(peers, "m-other");

        await ClaimAsync(claims, holder, "src/tools");

        var childUnderHeldDirectory = await ClaimAsync(claims, other, "src/tools/write.cs");
        Assert.Equal("src/tools", Assert.Single(childUnderHeldDirectory.Conflicts).HeldPath);

        var siblingSharingAPrefix = await ClaimAsync(claims, other, "src/toolsmith.cs");
        Assert.Empty(siblingSharingAPrefix.Conflicts);

        // And the reverse direction: a parent directory over a held file.
        await ClaimAsync(claims, holder, "lib/deep/nested/file.cs");
        var parentOverHeldFile = await ClaimAsync(claims, other, "lib");
        Assert.Equal("lib/deep/nested/file.cs", Assert.Single(parentOverHeldFile.Conflicts).HeldPath);
    }

    [Fact]
    public async Task Claim_SamePeerAgain_RenewsInsteadOfConflicting()
    {
        var (peers, claims) = Create();
        var me = await RegisterAsync(peers, "m-me");

        var first = await ClaimAsync(claims, me, 60, "src/a.cs");
        var renewed = await ClaimAsync(claims, me, 14400, "src/a.cs");

        Assert.Empty(renewed.Conflicts);
        Assert.True(
            DateTime.Parse(renewed.Granted.Single().ExpiresAt).ToUniversalTime()
            > DateTime.Parse(first.Granted.Single().ExpiresAt).ToUniversalTime(),
            "re-claiming must extend the TTL, not leave the old expiry in place");
        Assert.Single((await claims.ListAsync("/repo", CancellationToken.None)).Claims);
    }

    // Absolute paths inside the repo are the same claim as their relative spelling; one file cannot be
    // held twice under two names.
    [Fact]
    public async Task Claim_AbsolutePathInsideTheRepo_NormalizesToTheRelativeClaim()
    {
        var (peers, claims) = Create();
        var holder = await RegisterAsync(peers, "m-holder");
        var other = await RegisterAsync(peers, "m-other");

        await ClaimAsync(claims, holder, "/repo/src/a.cs");
        var relative = await ClaimAsync(claims, other, "./src/a.cs");

        Assert.Equal("src/a.cs", Assert.Single(relative.Conflicts).HeldPath);
    }

    [Fact]
    public async Task Claim_PathOutsideTheRepoOrEscapingIt_IsRejected()
    {
        var (peers, claims) = Create();
        var me = await RegisterAsync(peers, "m-me");

        var outside = await claims.ClaimAsync(
            new ClaimPathsRequest { PeerId = me, Paths = ["/elsewhere/secrets.env"] }, CancellationToken.None);
        Assert.True(outside.IsFailed);
        Assert.Contains("outside", outside.Errors[0].Message);

        var escaping = await claims.ClaimAsync(
            new ClaimPathsRequest { PeerId = me, Paths = ["../../etc/passwd"] }, CancellationToken.None);
        Assert.True(escaping.IsFailed);
        Assert.Contains("escapes", escaping.Errors[0].Message);
    }

    [Fact]
    public async Task Claim_SessionWithoutARepo_HasNoScopeToClaim()
    {
        var (peers, claims) = Create();
        var homeless = await RegisterAsync(peers, "m-norepo", repo: null);

        var result = await claims.ClaimAsync(
            new ClaimPathsRequest { PeerId = homeless, Paths = ["src/a.cs"] }, CancellationToken.None);

        Assert.True(result.IsFailed);
        Assert.Contains("not in a git repository", result.Errors[0].Message);
    }

    // THE abandonment property. A session that dies never releases anything, so holder liveness has to
    // come from the lease — otherwise a crashed peer holds its files for the whole TTL and the next
    // session's only option is to ignore claims entirely.
    [Fact]
    public async Task Claim_HolderWhoseLeaseLapsed_BlocksNobodyAndVanishesFromTheMap()
    {
        var (peers, claims) = Create();
        var dead = await RegisterAsync(peers, "m-dead");
        var live = await RegisterAsync(peers, "m-live");

        await ClaimAsync(claims, dead, 14400, "src/a.cs");
        await ExpireLease(dead);

        Assert.Empty((await claims.ListAsync("/repo", CancellationToken.None)).Claims);
        var taken = await ClaimAsync(claims, live, "src/a.cs");
        Assert.Empty(taken.Conflicts);
        Assert.Equal("src/a.cs", taken.Granted.Single().Path);
    }

    [Fact]
    public async Task Claim_ExpiredTtl_StopsHolding()
    {
        var (peers, claims) = Create();
        var holder = await RegisterAsync(peers, "m-holder");
        var other = await RegisterAsync(peers, "m-other");

        await ClaimAsync(claims, holder, "src/a.cs");
        await BackdateClaims();

        var taken = await ClaimAsync(claims, other, "src/a.cs");
        Assert.Empty(taken.Conflicts);
    }

    [Fact]
    public async Task Release_DropsNamedPaths_OrEverythingWhenNoneAreNamed()
    {
        var (peers, claims) = Create();
        var me = await RegisterAsync(peers, "m-me");
        await ClaimAsync(claims, me, "src/a.cs", "src/b.cs", "src/c.cs");

        var one = await claims.ReleaseAsync(
            new ReleaseClaimsRequest { PeerId = me, Paths = ["/repo/src/a.cs"] }, CancellationToken.None);
        Assert.Equal(1, one.Value.Released);
        Assert.Equal(["src/b.cs", "src/c.cs"], (await claims.ListAsync("/repo", CancellationToken.None)).Claims.Select(c => c.Path).Order().ToArray());

        var rest = await claims.ReleaseAsync(new ReleaseClaimsRequest { PeerId = me }, CancellationToken.None);
        Assert.Equal(2, rest.Value.Released);
        Assert.Empty((await claims.ListAsync("/repo", CancellationToken.None)).Claims);
    }

    [Fact]
    public async Task Claim_TtlIsClamped_SoNoSessionCanHoldAPathForever()
    {
        var (peers, claims) = Create();
        var me = await RegisterAsync(peers, "m-me");

        var forever = await ClaimAsync(claims, me, int.MaxValue, "src/a.cs");
        var expires = DateTime.Parse(forever.Granted.Single().ExpiresAt).ToUniversalTime();

        Assert.True(expires <= DateTime.UtcNow.AddHours(4).AddMinutes(1), $"TTL was not clamped: {expires:O}");
    }

    private Task ExpireLease(string peerId) =>
        _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE peers SET lease_expires_at = datetime('now', '-1 minute') WHERE peer_id = @id";
            cmd.Parameters.AddWithValue("@id", peerId);
            await cmd.ExecuteNonQueryAsync();
        });

    private Task BackdateClaims() =>
        _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE claims SET expires_at = datetime('now', '-1 minute')";
            await cmd.ExecuteNonQueryAsync();
        });

    public void Dispose() => _db.Dispose();
}
