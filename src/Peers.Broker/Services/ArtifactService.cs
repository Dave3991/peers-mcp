using System.Text.Json;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

public sealed class ArtifactService(ArtifactRepository artifacts, EventService eventService, PeerRepository peers)
{
    public async Task<PublishArtifactResponse> PublishAsync(PublishArtifactRequest req, CancellationToken ct)
    {
        var artifact = new Artifact
        {
            ArtifactId = Guid.NewGuid().ToString("N")[..16],
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

        var (saved, version, isNew) = await artifacts.PublishAsync(artifact, ct);

        if (isNew)
        {
            var activePeers = await peers.ListAsync(repoRoot: req.RepoRoot, includeExpired: false, ct: ct);
            var recipientPeerIds = activePeers
                .Where(p => p.PeerId != req.PublishedByPeerId)
                .Select(p => p.PeerId)
                .ToArray();

            await eventService.EmitAsync(EventTypes.ArtifactPublished,
                JsonSerializer.Serialize(new { artifact_id = saved.ArtifactId, ns = req.Namespace, name = req.Name, version, published_by_peer_id = req.PublishedByPeerId, recipient_peer_ids = recipientPeerIds }),
                ct, recipientPeerIds);
        }

        return new PublishArtifactResponse { ArtifactId = saved.ArtifactId, Version = version, Deduplicated = !isNew };
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
