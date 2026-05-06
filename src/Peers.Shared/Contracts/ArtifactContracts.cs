namespace Peers.Shared.Contracts;

public sealed class PublishArtifactRequest
{
    public required string Namespace { get; init; }
    public required string Name { get; init; }
    public required string Type { get; init; }
    public required string Content { get; init; }
    public string? RepoRoot { get; init; }
    public string? Branch { get; init; }
    public string? CommitSha { get; init; }
    public string? ContentType { get; init; }
    public string? Tags { get; init; }
    public required string PublishedByPeerId { get; init; }
}

public sealed class PublishArtifactResponse
{
    public required string ArtifactId { get; init; }
    public required int Version { get; init; }
    public bool Deduplicated { get; init; }
}
