namespace Peers.Shared.Models;

public sealed record Artifact
{
    public required string ArtifactId { get; init; }
    public required string Namespace { get; init; }
    public required string Name { get; init; }
    public required int Version { get; init; }
    public required string Type { get; init; } // openapi-spec, typescript-types, diff, summary, test-report
    public string? RepoRoot { get; init; }
    public string? Branch { get; init; }
    public string? CommitSha { get; init; }
    public string? ContentType { get; init; }
    public required string Content { get; init; }
    public required string ContentHash { get; init; }
    public required int ContentBytes { get; init; }
    public string? Tags { get; init; } // JSON array
    public required string PublishedByPeerId { get; init; }
    public required DateTime CreatedAt { get; init; }
}

public sealed class ArtifactMetadata
{
    public required string ArtifactId { get; init; }
    public required string Namespace { get; init; }
    public required string Name { get; init; }
    public required int Version { get; init; }
    public required string Type { get; init; }
    public string? RepoRoot { get; init; }
    public string? Tags { get; init; }
    public required string PublishedByPeerId { get; init; }
    public required int ContentBytes { get; init; }
    public required DateTime CreatedAt { get; init; }
}
