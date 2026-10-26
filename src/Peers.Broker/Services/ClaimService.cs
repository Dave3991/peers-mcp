using System.Diagnostics;
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
        // path — in the working tree that holds that path. A session outside Git claims nothing.
        if (string.IsNullOrWhiteSpace(peer.RepoRoot))
            return Result.Fail(new BadRequestError("This session is not in a git repository, so it has no path scope to claim."));

        var roots = new Dictionary<string, string?>(StringComparer.Ordinal);
        var targets = new List<ClaimTarget>();
        foreach (var path in req.Paths)
        {
            var result = await ResolveAsync(path, peer.RepoRoot, roots, ct);
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
            var roots = new Dictionary<string, string?>(StringComparer.Ordinal);
            List<(string RepoRoot, string Path)>? mine = null;
            foreach (var path in req.Paths)
            {
                var result = await ResolveAsync(path, peer.RepoRoot, roots, ct);
                if (result.IsSuccess)
                    targets.Add((result.Value.RepoRoot, result.Value.Path));

                // An absolute path also matches the caller's own claims spelled out in full. A removed
                // worktree no longer resolves (or now resolves to the repo around it), yet its claims are
                // still keyed by the tree they were made in.
                if (AbsolutePath(path) is not { } absolute)
                {
                    if (result.IsFailed)
                        return result.ToResult<ReleaseClaimsResponse>();
                    continue;
                }
                mine ??= await claims.ListForPeerAsync(req.PeerId, ct);
                var held = mine.Where(row => (row.Path.Length == 0 ? row.RepoRoot : $"{row.RepoRoot}/{row.Path}") == absolute).ToList();
                if (result.IsFailed && held.Count == 0)
                    return Result.Fail(new BadRequestError(
                        $"{result.Errors[0].Message} This session holds no claim on that path either; release without paths drops all of its claims."));
                targets.AddRange(held);
            }
        }

        return Result.Ok(new ReleaseClaimsResponse { Released = await claims.DeleteAsync(req.PeerId, targets?.Distinct().ToList(), ct) });
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
    /// file cannot be claimed twice under two spellings.
    ///
    /// A relative path belongs to the peer's repo. An absolute path belongs to the nearest working tree
    /// that holds it (the closest ancestor with a .git entry, so a worktree nested inside a repo wins
    /// over the repo), keyed by the root `git rev-parse --show-toplevel` reports: the physical spelling
    /// every session registers with, whatever case or symlinked folder the caller typed. Only when no
    /// .git is found does the prefix of the peer's own repo root decide. A path in no working tree, or
    /// one that climbs out with "..", is rejected rather than silently reinterpreted.
    /// </summary>
    /// <param name="roots">Toplevels already asked of git in this request, by tree directory.</param>
    internal static async Task<Result<ClaimTarget>> ResolveAsync(string path, string? repoRoot,
        Dictionary<string, string?> roots, CancellationToken ct)
    {
        var trimmed = path?.Trim();
        if (string.IsNullOrEmpty(trimmed))
            return Result.Fail(new BadRequestError("A claimed path cannot be empty."));

        var unified = trimmed.Replace('\\', '/');
        // Before any root matching: "/repo/../other" must not pass as inside /repo, nor walk up to a .git above it.
        if (unified.Split('/').Contains(".."))
            return Result.Fail(new BadRequestError($"Path '{trimmed}' escapes the repository root."));

        if (AbsolutePath(unified) is not { } absolute)
            return repoRoot is null
                ? Result.Fail(new BadRequestError(
                    $"Path '{trimmed}' is relative, but this session is not in a git repository; pass an absolute path inside a git working tree."))
                : Result.Ok(new ClaimTarget(repoRoot, Relative(unified)));

        if (WorkTreeOf(absolute) is { } tree)
        {
            if (!roots.TryGetValue(tree, out var root))
                roots[tree] = root = tree == repoRoot ? repoRoot : await GitTopLevelAsync(tree, ct);
            return root is null
                ? Result.Fail(new BadRequestError(
                    $"Path '{trimmed}' is inside '{tree}', which has a .git entry, but git reports no working tree there; claim paths inside a git working tree."))
                : Result.Ok(new ClaimTarget(root, Relative(absolute[tree.Length..])));
        }

        var own = repoRoot?.Replace('\\', '/').TrimEnd('/');
        if (own is not null && (absolute == own || absolute.StartsWith($"{own}/", StringComparison.Ordinal)))
            return Result.Ok(new ClaimTarget(repoRoot!, Relative(absolute[own.Length..])));
        return Result.Fail(new BadRequestError(
            $"Path '{trimmed}' is outside this session's repository ({repoRoot ?? "none"}) and inside no git working tree " +
            "(no .git in it or any parent directory). Claims are kept per repository: claim paths inside a git working tree."));
    }

    /// <summary>
    /// A rooted path with "//" and "/./" collapsed and the macOS /tmp, /var and /etc links resolved,
    /// without a trailing slash; null for a relative path or one with "..". Every root match and slice
    /// runs on this form, so an odd spelling above the tree root cannot shift the relative path.
    /// </summary>
    private static string? AbsolutePath(string? path)
    {
        var unified = path?.Trim().Replace('\\', '/');
        if (string.IsNullOrEmpty(unified) || !Path.IsPathRooted(unified) || unified.Split('/').Contains(".."))
            return null;
        var full = SystemPaths.Canonicalize(Path.GetFullPath(unified));
        return full.Length > 1 ? full.TrimEnd('/') : full;
    }

    /// <summary>The repo-relative claim key: forward-slashed segments, without empty or "." ones. No segments is the whole repo.</summary>
    private static string Relative(string path) =>
        string.Join('/', path.Split('/').Where(segment => segment.Length > 0 && segment != "."));

    /// <summary>The nearest directory at or above <paramref name="path"/> holding a .git entry (a directory, or a worktree's .git file).</summary>
    private static string? WorkTreeOf(string path)
    {
        for (var dir = path; !string.IsNullOrEmpty(dir); dir = Path.GetDirectoryName(dir))
            if (Directory.Exists(Path.Combine(dir, ".git")) || File.Exists(Path.Combine(dir, ".git")))
                return dir;
        return null;
    }

    /// <summary>
    /// The working tree root as `git rev-parse --show-toplevel` reports it for <paramref name="tree"/> —
    /// the physical path in its stored case, exactly what sessions register — or null when git finds no
    /// working tree there (or cannot run within 10 seconds).
    /// </summary>
    private static async Task<string?> GitTopLevelAsync(string tree, CancellationToken ct)
    {
        var start = new ProcessStartInfo("git")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var arg in new[] { "-C", tree, "rev-parse", "--show-toplevel" })
            start.ArgumentList.Add(arg);

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(TimeSpan.FromSeconds(10));
        Process? git = null;
        try
        {
            git = Process.Start(start)!;
            var output = git.StandardOutput.ReadToEndAsync(deadline.Token);
            var error = git.StandardError.ReadToEndAsync(deadline.Token);
            await git.WaitForExitAsync(deadline.Token);
            var top = (await output).TrimEnd('\r', '\n');
            await error;
            return git.ExitCode == 0 && top.Length > 0 ? top : null;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            git?.Kill(entireProcessTree: true);
            return null;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return null; // git is not installed: no working tree can be confirmed.
        }
        finally
        {
            git?.Dispose();
        }
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
