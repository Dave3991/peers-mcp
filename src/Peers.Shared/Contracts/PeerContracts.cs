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
}

public sealed class HeartbeatRequest
{
    public string? Summary { get; init; }
    public string? Role { get; init; }
    public string? Branch { get; init; }
    public string? CommitSha { get; init; }
    public string? FocusPaths { get; init; }
}

public sealed class ListPeersRequest
{
    public string? Scope { get; init; } // machine, repo, worktree, branch, tool
    public string? RepoRoot { get; init; }
    public string? Branch { get; init; }
    public string? Tool { get; init; }
}
