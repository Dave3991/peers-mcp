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

    /// <summary>Units of the current long-running task completed so far; null when nothing is being tracked.</summary>
    public int? ProgressCurrent { get; init; }

    /// <summary>Total units in the current task, or null for an indeterminate count.</summary>
    public int? ProgressTotal { get; init; }

    /// <summary>When progress last actually moved. Distinct from <see cref="UpdatedAt"/>, which the 30s
    /// lease renewal bumps for every live peer and so cannot tell moving progress from stalled progress.</summary>
    public DateTime? ProgressUpdatedAt { get; init; }
    public required DateTime LeaseExpiresAt { get; init; }
    public DateTime? ExpiredAt { get; init; }
    public required DateTime CreatedAt { get; init; }
    public required DateTime UpdatedAt { get; init; }
}
