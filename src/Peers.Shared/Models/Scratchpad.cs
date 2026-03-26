namespace Peers.Shared.Models;

public sealed class Scratchpad
{
    public required string ScratchpadId { get; init; }
    public required string Type { get; init; } // review, discussion, spec, context
    public required string Title { get; init; }
    public string? RepoRoot { get; init; }
    public required string Status { get; init; } // open, resolved, closed
    public string? RequestedReviewers { get; init; } // JSON array
    public string? ArtifactRefs { get; init; } // JSON array
    public required string CreatedByPeerId { get; init; }
    public required DateTime CreatedAt { get; init; }
    public DateTime? ClosedAt { get; init; }
    public string? Resolution { get; init; }
}

public sealed class ScratchpadEntry
{
    public required string EntryId { get; init; }
    public required string ScratchpadId { get; init; }
    public required string AuthorPeerId { get; init; }
    public required string Content { get; init; }
    public string? IdempotencyKey { get; init; }
    public required int ContentBytes { get; init; }
    public required DateTime CreatedAt { get; init; }
}
