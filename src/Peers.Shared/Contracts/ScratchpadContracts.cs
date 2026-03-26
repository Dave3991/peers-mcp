using Peers.Shared.Models;

namespace Peers.Shared.Contracts;

public sealed class CreateScratchpadRequest
{
    public required string Type { get; init; }
    public required string Title { get; init; }
    public string? RepoRoot { get; init; }
    public string? RequestedReviewers { get; init; }
    public string? ArtifactRefs { get; init; }
    public required string CreatedByPeerId { get; init; }
}

public sealed class CreateScratchpadResponse
{
    public required string ScratchpadId { get; init; }
}

public sealed class AppendEntryRequest
{
    public required string AuthorPeerId { get; init; }
    public required string Content { get; init; }
    public string? IdempotencyKey { get; init; }
}

public sealed class AppendEntryResponse
{
    public required string EntryId { get; init; }
}

public sealed class ReadScratchpadResponse
{
    public required Scratchpad Scratchpad { get; init; }
    public required List<ScratchpadEntry> Entries { get; init; }
}

public sealed class CloseScratchpadRequest
{
    public string? Resolution { get; init; }
}
