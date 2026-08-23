namespace Peers.Shared.Contracts;

public sealed class RegisterPeerRequest
{
    public required string Tool { get; init; }
    public required string MachineId { get; init; }
    public string? RepoRoot { get; init; }
    public string? WorktreeRoot { get; init; }
    public string? Branch { get; init; }
    public string? CommitSha { get; init; }
    public string? Role { get; init; }
    public string? Summary { get; init; }
    public string? FocusPaths { get; init; }
    public string? Capabilities { get; init; }
    public string? ParentPeerId { get; init; }
    public string? ResumePeerId { get; init; }
}

public sealed class RegisterPeerResponse
{
    public required string PeerId { get; init; }
    public required long LastCursor { get; init; }
    public required bool Resumed { get; init; }

    /// <summary>
    /// Id of this registration's own peer.registered event — i.e. the broker's newest event at the moment
    /// this session came into existence. Event ids are assigned in commit order by a serialised writer, so
    /// "id &gt; LatestEventId" is exactly "happened after this session registered". Resource subscriptions
    /// start here; the push path keeps using <see cref="LastCursor"/>.
    /// </summary>
    public required long LatestEventId { get; init; }
}

public sealed class HeartbeatRequest
{
    public string? Summary { get; init; }
    public string? Role { get; init; }
    public string? Branch { get; init; }
    public string? CommitSha { get; init; }
    public string? FocusPaths { get; init; }
    public string? Capabilities { get; init; }
}

public sealed class ListPeersRequest
{
    public string? Scope { get; init; } // machine, repo, worktree, branch, tool
    public string? RepoRoot { get; init; }
    public string? Branch { get; init; }
    public string? Tool { get; init; }
    public bool IncludeExpired { get; init; }
}
