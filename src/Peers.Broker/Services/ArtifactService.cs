using System.Text.Json;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

public sealed class ArtifactService(ArtifactRepository artifacts, EventRepository events)
{
    public async Task<PublishArtifactResponse> PublishAsync(PublishArtifactRequest req, CancellationToken ct)
    {
        var id = Guid.NewGuid().ToString("N")[..16];
        var artifact = new Artifact
        {
            ArtifactId = id,
            Namespace = req.Namespace,
            Name = req.Name,
            Version = 0, // set by repo
            Type = req.Type,
            RepoRoot = req.RepoRoot,
            Branch = req.Branch,
            CommitSha = req.CommitSha,
            ContentType = req.ContentType,
            Content = req.Content,
            ContentHash = "", // set by repo
            ContentBytes = 0, // set by repo
            Tags = req.Tags,
            PublishedByPeerId = req.PublishedByPeerId,
            CreatedAt = DateTime.UtcNow,
        };

        var (saved, version) = await artifacts.PublishAsync(artifact, ct);
        await events.InsertAsync(EventTypes.ArtifactPublished,
            JsonSerializer.Serialize(new { artifact_id = id, ns = req.Namespace, name = req.Name, version }), ct: ct);

        return new PublishArtifactResponse { ArtifactId = id, Version = version };
    }

    public async Task<Artifact?> GetAsync(string? artifactId, string? ns, string? name, CancellationToken ct)
    {
        if (artifactId is not null)
            return await artifacts.GetByIdAsync(artifactId, ct);
        if (ns is not null && name is not null)
            return await artifacts.GetLatestAsync(ns, name, ct);
        return null;
    }

    public async Task<List<ArtifactMetadata>> ListAsync(string? type, string? repoRoot, string? ns, CancellationToken ct)
    {
        return await artifacts.ListAsync(type, repoRoot, ns, ct);
    }
}
