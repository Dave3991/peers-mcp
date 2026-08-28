namespace Peers.Shared.Contracts;

/// <summary>
/// A claim announces that one peer is working on one repo-relative path, for a bounded time. It is
/// advisory in the only sense that matters: the broker never blocks an edit, it reports who else is
/// already on that path so the caller can coordinate instead of discovering the collision in a diff.
///
/// Scope is server-derived — the claiming peer's own repo_root, never a value the caller supplies —
/// so a session cannot claim paths in a repo it is not working in.
/// </summary>
public sealed class ClaimPathsRequest
{
    public required string PeerId { get; init; }

    /// <summary>Repo-relative or absolute paths inside the peer's repo. A directory covers everything under it.</summary>
    public required string[] Paths { get; init; }

    /// <summary>Seconds the claim holds; clamped to [60, 14400], default 1800.</summary>
    public int? TtlSeconds { get; init; }
}

public sealed class ClaimPathsResponse
{
    public required string RepoRoot { get; init; }

    /// <summary>Paths now held by the caller. Re-claiming a path it already holds extends the TTL.</summary>
    public required ClaimGrant[] Granted { get; init; }

    /// <summary>Paths another live peer holds, or that overlap one. Nothing was written for these.</summary>
    public required ClaimConflict[] Conflicts { get; init; }
}

public sealed class ClaimGrant
{
    public required string Path { get; init; }
    public required string ExpiresAt { get; init; }
}

/// <summary>
/// <see cref="Path"/> is what the caller asked for; <see cref="HeldPath"/> is the claim that stands in
/// its way — the same path, a directory containing it, or a file inside a directory the caller asked for.
/// </summary>
public sealed class ClaimConflict
{
    public required string Path { get; init; }
    public required string HeldPath { get; init; }
    public required string HolderPeerId { get; init; }
    public string? HolderRole { get; init; }
    public string? HolderSummary { get; init; }
    public required string ExpiresAt { get; init; }
}

public sealed class ReleaseClaimsRequest
{
    public required string PeerId { get; init; }

    /// <summary>Paths to release; omit to release every claim this peer holds.</summary>
    public string[]? Paths { get; init; }
}

public sealed class ReleaseClaimsResponse
{
    public int Released { get; init; }
}

public sealed class ListClaimsResponse
{
    public required ClaimDto[] Claims { get; init; }
}

public sealed class ClaimDto
{
    public required string Path { get; init; }
    public required string RepoRoot { get; init; }
    public required string PeerId { get; init; }
    public string? HolderRole { get; init; }
    public string? HolderSummary { get; init; }
    public required string ExpiresAt { get; init; }
}
