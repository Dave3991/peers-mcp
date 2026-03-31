namespace Peers.Shared.Models;

public sealed class Peer
{
    public required string PeerId { get; init; }
    public required string Tool { get; init; } // claude-code, codex, other
    public required string MachineId { get; init; }
    public string? RepoRoot { get; init; }
    public string? WorktreeRoot { get; init; }
    public string? Branch { get; init; }
    public string? CommitSha { get; init; }
    public string? Role { get; init; } // implementing, reviewing, debugging, spec
    public string? Summary { get; init; }
    public string? FocusPaths { get; init; } // JSON array
    public string? Capabilities { get; init; } // JSON array
    public string? ParentPeerId { get; init; }
    public long? LastPolledEventId { get; init; }
    public required DateTime LeaseExpiresAt { get; init; }
    public DateTime? ExpiredAt { get; init; }
    public required DateTime CreatedAt { get; init; }
    public required DateTime UpdatedAt { get; init; }
}
