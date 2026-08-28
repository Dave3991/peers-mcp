using FluentResults;
using Peers.Broker.Errors;
using Peers.Broker.Persistence.Repositories;
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
        // Server-derived scope: a session claims inside the repo it registered from, or nowhere.
        if (string.IsNullOrWhiteSpace(peer.RepoRoot))
            return Result.Fail(new BadRequestError("This session is not in a git repository, so it has no path scope to claim."));

        var normalized = new List<string>();
        foreach (var path in req.Paths)
        {
            var result = Normalize(path, peer.RepoRoot);
            if (result.IsFailed)
                return result.ToResult<ClaimPathsResponse>();
            normalized.Add(result.Value);
        }

        var ttl = TimeSpan.FromSeconds(Math.Clamp(req.TtlSeconds ?? DefaultTtlSeconds, MinTtlSeconds, MaxTtlSeconds));
        var expiresAt = DateTime.UtcNow.Add(ttl);
        var held = await claims.ListActiveAsync(peer.RepoRoot, ct);

        var granted = new List<ClaimGrant>();
        var conflicts = new List<ClaimConflict>();
        foreach (var path in normalized.Distinct(StringComparer.Ordinal))
        {
            var blocker = held.FirstOrDefault(row => row.PeerId != req.PeerId && Overlaps(row.Path, path));
            if (blocker is not null)
            {
                conflicts.Add(new ClaimConflict
                {
                    Path = path,
                    HeldPath = blocker.Path,
                    HolderPeerId = blocker.PeerId,
                    HolderRole = blocker.HolderRole,
                    HolderSummary = blocker.HolderSummary,
                    ExpiresAt = blocker.ExpiresAt.ToString("O"),
                });
                continue;
            }

            var storedExpiry = await claims.UpsertAsync(req.PeerId, peer.RepoRoot, path, expiresAt, ct);
            granted.Add(new ClaimGrant { Path = path, ExpiresAt = storedExpiry });
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

        List<string>? paths = null;
        if (req.Paths is not null)
        {
            paths = [];
            foreach (var path in req.Paths)
            {
                var result = Normalize(path, peer.RepoRoot);
                if (result.IsFailed)
                    return result.ToResult<ReleaseClaimsResponse>();
                paths.Add(result.Value);
            }
        }

        return Result.Ok(new ReleaseClaimsResponse { Released = await claims.DeleteAsync(req.PeerId, paths, ct) });
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
    /// Reduces a caller's path to the repo-relative, forward-slashed form claims are keyed by, so the
    /// same file cannot be claimed twice under two spellings. An absolute path outside the peer's repo
    /// is rejected rather than silently reinterpreted, and so is any path that climbs out with "..".
    /// </summary>
    internal static Result<string> Normalize(string path, string? repoRoot)
    {
        var trimmed = path?.Trim();
        if (string.IsNullOrEmpty(trimmed))
            return Result.Fail(new BadRequestError("A claimed path cannot be empty."));

        var unified = trimmed.Replace('\\', '/');
        if (repoRoot is not null && Path.IsPathRooted(unified))
        {
            var root = repoRoot.Replace('\\', '/').TrimEnd('/');
            if (unified == root)
                unified = "";
            else if (unified.StartsWith($"{root}/", StringComparison.Ordinal))
                unified = unified[(root.Length + 1)..];
            else
                return Result.Fail(new BadRequestError($"Path '{trimmed}' is outside this session's repository ({repoRoot})."));
        }

        var segments = new List<string>();
        foreach (var segment in unified.Split('/'))
        {
            if (segment.Length == 0 || segment == ".")
                continue;
            if (segment == "..")
                return Result.Fail(new BadRequestError($"Path '{trimmed}' escapes the repository root."));
            segments.Add(segment);
        }

        // The repo root itself: a whole-repo claim, which Overlaps treats as containing everything.
        return Result.Ok(string.Join('/', segments));
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
