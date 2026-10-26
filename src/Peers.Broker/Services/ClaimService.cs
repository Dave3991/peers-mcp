using FluentResults;
using Peers.Broker.Errors;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared;
using Peers.Shared.Contracts;

namespace Peers.Broker.Services;

/// <summary>
/// Claims answer one question before an edit rather than after it: is another live session already on
/// this path? Grants are per path and independent — a request for four files where one collides grants
/// three and names the holder of the fourth, because the alternative (all-or-nothing) makes a single
/// contested file block work nobody is contesting.
///
/// Overlap is directory containment, not string equality: claiming <c>src/</c> conflicts with
/// <c>src/foo.cs</c> in both directions. Comparison is segment-wise, so <c>src/foo</c> never conflicts
/// with <c>src/foobar</c>.
/// </summary>
public sealed class ClaimService(ClaimRepository claims, PeerRepository peers)
{
    private const int DefaultTtlSeconds = 1800;
    private const int MinTtlSeconds = 60;
    private const int MaxTtlSeconds = 14400;

    public async Task<Result<ClaimPathsResponse>> ClaimAsync(ClaimPathsRequest req, CancellationToken ct)
    {
        var peer = await peers.GetAsync(req.PeerId, ct);
        if (peer is null)
            return Result.Fail(new NotFoundError($"Peer '{req.PeerId}' not found."));
        // Server-derived scope: a session claims in the repo it registered from, or — for an absolute
        // path elsewhere — in the working tree that holds that path. A session outside Git claims nothing.
        if (string.IsNullOrWhiteSpace(peer.RepoRoot))
            return Result.Fail(new BadRequestError("This session is not in a git repository, so it has no path scope to claim."));

        var targets = new List<ClaimTarget>();
        foreach (var path in req.Paths)
        {
            var result = Resolve(path, peer.RepoRoot);
            if (result.IsFailed)
                return result.ToResult<ClaimPathsResponse>();
            targets.Add(result.Value);
        }

        var ttl = TimeSpan.FromSeconds(Math.Clamp(req.TtlSeconds ?? DefaultTtlSeconds, MinTtlSeconds, MaxTtlSeconds));
        var expiresAt = DateTime.UtcNow.Add(ttl);
        var held = new Dictionary<string, List<ClaimRow>>(StringComparer.Ordinal);

        var granted = new List<ClaimGrant>();
        var conflicts = new List<ClaimConflict>();
        foreach (var target in targets.Distinct())
        {
            if (!held.TryGetValue(target.RepoRoot, out var rows))
                held[target.RepoRoot] = rows = await claims.ListActiveAsync(target.RepoRoot, ct);
            var blocker = rows.FirstOrDefault(row => row.PeerId != req.PeerId && Overlaps(row.Path, target.Path));
            if (blocker is not null)
            {
                conflicts.Add(new ClaimConflict
                {
                    Path = target.Path,
                    RepoRoot = target.RepoRoot,
                    HeldPath = blocker.Path,
                    HolderPeerId = blocker.PeerId,
                    HolderRole = blocker.HolderRole,
                    HolderSummary = blocker.HolderSummary,
                    ExpiresAt = blocker.ExpiresAt.ToString("O"),
                });
                continue;
            }

            var storedExpiry = await claims.UpsertAsync(req.PeerId, target.RepoRoot, target.Path, expiresAt, ct);
            granted.Add(new ClaimGrant { Path = target.Path, RepoRoot = target.RepoRoot, ExpiresAt = storedExpiry });
        }

        return Result.Ok(new ClaimPathsResponse
        {
            RepoRoot = peer.RepoRoot,
            Granted = [.. granted],
            Conflicts = [.. conflicts],
        });
    }

    public async Task<Result<ReleaseClaimsResponse>> ReleaseAsync(ReleaseClaimsRequest req, CancellationToken ct)
    {
        var peer = await peers.GetAsync(req.PeerId, ct);
        if (peer is null)
            return Result.Fail(new NotFoundError($"Peer '{req.PeerId}' not found."));

        List<(string RepoRoot, string Path)>? targets = null;
        if (req.Paths is not null)
        {
            targets = [];
            foreach (var path in req.Paths)
            {
                var result = Resolve(path, peer.RepoRoot);
                if (result.IsFailed)
                    return result.ToResult<ReleaseClaimsResponse>();
                targets.Add((result.Value.RepoRoot, result.Value.Path));
            }
        }

        return Result.Ok(new ReleaseClaimsResponse { Released = await claims.DeleteAsync(req.PeerId, targets, ct) });
    }

    public async Task<ListClaimsResponse> ListAsync(string repoRoot, CancellationToken ct)
    {
        var rows = await claims.ListActiveAsync(repoRoot, ct);
        return new ListClaimsResponse
        {
            Claims = [.. rows.Select(row => new ClaimDto
            {
                Path = row.Path,
                RepoRoot = row.RepoRoot,
                PeerId = row.PeerId,
                HolderRole = row.HolderRole,
                HolderSummary = row.HolderSummary,
                ExpiresAt = row.ExpiresAt.ToString("O"),
            })],
        };
    }

    /// <summary>
    /// The claim map for the repo a peer works in. Scope comes from the peer row for the same reason it
    /// does on claim: the caller's own idea of its repo root is client state that a re-registration can
    /// leave behind, while the peer row is what every other session compares against.
    /// </summary>
    public async Task<Result<ListClaimsResponse>> ListForPeerAsync(string peerId, CancellationToken ct)
    {
        var peer = await peers.GetAsync(peerId, ct);
        if (peer is null)
            return Result.Fail(new NotFoundError($"Peer '{peerId}' not found."));
        if (string.IsNullOrWhiteSpace(peer.RepoRoot))
            return Result.Fail(new BadRequestError("This session is not in a git repository, so it has no claim scope."));

        return Result.Ok(await ListAsync(peer.RepoRoot, ct));
    }

    /// <summary>
    /// The working tree and the repo-relative, forward-slashed path a claim is keyed by, so the same
    /// file cannot be claimed twice under two spellings. A relative path, or an absolute one inside the
    /// peer's repo, belongs to that repo. Any other absolute path belongs to the git working tree that
    /// holds it — the nearest ancestor with a .git entry, as `git rev-parse --show-toplevel` reports it
    /// for the sessions working there — so a session editing a sibling worktree claims where that
    /// tree's sessions look. A path in no working tree, or one that climbs out with "..", is rejected
    /// rather than silently reinterpreted.
    /// </summary>
    internal static Result<ClaimTarget> Resolve(string path, string? repoRoot)
    {
        var trimmed = path?.Trim();
        if (string.IsNullOrEmpty(trimmed))
            return Result.Fail(new BadRequestError("A claimed path cannot be empty."));

        var unified = trimmed.Replace('\\', '/');
        // Before any root matching: "/repo/../other" must not pass as inside /repo, nor walk up to a .git above it.
        if (unified.Split('/').Contains(".."))
            return Result.Fail(new BadRequestError($"Path '{trimmed}' escapes the repository root."));
        var scope = repoRoot;
        if (Path.IsPathRooted(unified))
        {
            unified = SystemPaths.Canonicalize(unified);
            var root = repoRoot?.Replace('\\', '/').TrimEnd('/');
            if (root is null || (unified != root && !unified.StartsWith($"{root}/", StringComparison.Ordinal)))
            {
                root = scope = WorkTreeOf(unified);
                if (root is null)
                    return Result.Fail(new BadRequestError(
                        $"Path '{trimmed}' is outside this session's repository ({repoRoot ?? "none"}) and inside no git working tree " +
                        "(no .git in it or any parent directory). Claims are kept per repository: claim paths inside a git working tree."));
            }
            unified = unified.Length <= root.Length ? "" : unified[(root.Length + 1)..];
        }
        else if (scope is null)
            return Result.Fail(new BadRequestError(
                $"Path '{trimmed}' is relative, but this session is not in a git repository; pass an absolute path inside a git working tree."));

        // The repo root itself (no segments): a whole-repo claim, which Overlaps treats as containing everything.
        var segments = unified.Split('/').Where(segment => segment.Length > 0 && segment != ".");
        return Result.Ok(new ClaimTarget(scope!, string.Join('/', segments)));
    }

    /// <summary>The nearest directory at or above <paramref name="path"/> holding a .git entry (a directory, or a worktree's .git file).</summary>
    private static string? WorkTreeOf(string path)
    {
        for (var dir = path.TrimEnd('/'); !string.IsNullOrEmpty(dir); dir = Path.GetDirectoryName(dir))
            if (Directory.Exists(Path.Combine(dir, ".git")) || File.Exists(Path.Combine(dir, ".git")))
                return dir;
        return null;
    }

    /// <summary>Whether two normalized paths name the same work: equal, or one containing the other.</summary>
    internal static bool Overlaps(string left, string right)
    {
        if (left.Length == 0 || right.Length == 0)
            return true;
        if (string.Equals(left, right, StringComparison.Ordinal))
            return true;
        return Contains(left, right) || Contains(right, left);
    }

    private static bool Contains(string directory, string candidate) =>
        candidate.Length > directory.Length
        && candidate[directory.Length] == '/'
        && candidate.StartsWith(directory, StringComparison.Ordinal);
}

/// <summary>One claimable path: the working tree it belongs to and its forward-slashed path relative to that tree.</summary>
internal readonly record struct ClaimTarget(string RepoRoot, string Path);
