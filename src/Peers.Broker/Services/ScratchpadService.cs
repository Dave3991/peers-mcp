using System.Text;
using System.Text.Json;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

public sealed class ScratchpadService(ScratchpadRepository pads, EventRepository events, PeerRepository peers)
{
    public async Task<CreateScratchpadResponse> CreateAsync(CreateScratchpadRequest req, CancellationToken ct)
    {
        var id = Guid.NewGuid().ToString("N")[..16];
        var recipientPeerIds = await ResolveReviewerPeerIdsAsync(req.RequestedReviewers, ct);
        var pad = new Scratchpad
        {
            ScratchpadId = id,
            Type = req.Type,
            Title = req.Title,
            RepoRoot = req.RepoRoot,
            Status = "open",
            RequestedReviewers = req.RequestedReviewers,
            ArtifactRefs = req.ArtifactRefs,
            CreatedByPeerId = req.CreatedByPeerId,
            CreatedAt = DateTime.UtcNow,
        };

        await pads.InsertAsync(pad, ct);
        await events.InsertAsync(EventTypes.ScratchpadCreated,
            JsonSerializer.Serialize(new
            {
                scratchpad_id = id,
                type = req.Type,
                title = req.Title,
                created_by_peer_id = req.CreatedByPeerId,
                recipient_peer_ids = recipientPeerIds,
            }), ct: ct);

        return new CreateScratchpadResponse { ScratchpadId = id };
    }

    public async Task<AppendEntryResponse?> AppendAsync(string scratchpadId, AppendEntryRequest req, CancellationToken ct)
    {
        var pad = await pads.GetAsync(scratchpadId, ct)
            ?? throw new InvalidOperationException($"Scratchpad '{scratchpadId}' not found.");
        var recipientPeerIds = await ResolveConversationPeerIdsAsync(pad, req.AuthorPeerId, ct);

        var entryId = Guid.NewGuid().ToString("N")[..16];
        var entry = new ScratchpadEntry
        {
            EntryId = entryId,
            ScratchpadId = scratchpadId,
            AuthorPeerId = req.AuthorPeerId,
            Content = req.Content,
            IdempotencyKey = req.IdempotencyKey,
            ContentBytes = Encoding.UTF8.GetByteCount(req.Content),
            CreatedAt = DateTime.UtcNow,
        };

        var result = await pads.AppendEntryAsync(entry, ct);
        if (result is null) return null; // idempotent duplicate

        await events.InsertAsync(EventTypes.ScratchpadEntryAdded,
            JsonSerializer.Serialize(new
            {
                scratchpad_id = scratchpadId,
                entry_id = entryId,
                title = pad.Title,
                author_peer_id = req.AuthorPeerId,
                content = req.Content,
                created_by_peer_id = pad.CreatedByPeerId,
                recipient_peer_ids = recipientPeerIds,
            }), ct: ct);

        return new AppendEntryResponse { EntryId = entryId };
    }

    public async Task<ReadScratchpadResponse?> ReadAsync(string scratchpadId, int limit, int offset, CancellationToken ct)
    {
        var pad = await pads.GetAsync(scratchpadId, ct);
        if (pad is null) return null;

        var entries = await pads.GetEntriesAsync(scratchpadId, limit: limit, offset: offset, ct: ct);
        return new ReadScratchpadResponse { Scratchpad = pad, Entries = entries };
    }

    public async Task<List<Scratchpad>> ListAsync(string? type, string? repoRoot, string? status, CancellationToken ct)
    {
        return await pads.ListAsync(type, repoRoot, status, ct);
    }

    public async Task CloseAsync(string scratchpadId, CloseScratchpadRequest req, CancellationToken ct)
    {
        var pad = await pads.GetAsync(scratchpadId, ct)
            ?? throw new InvalidOperationException($"Scratchpad '{scratchpadId}' not found.");

        await pads.CloseAsync(scratchpadId, req.Resolution, ct);
        var closedPad = await pads.GetAsync(scratchpadId, ct) ?? pad;
        var recipientPeerIds = await ResolveConversationPeerIdsAsync(closedPad, authorPeerId: null, ct);

        await events.InsertAsync(EventTypes.ScratchpadClosed,
            JsonSerializer.Serialize(new
            {
                scratchpad_id = scratchpadId,
                title = closedPad.Title,
                resolution = req.Resolution,
                recipient_peer_ids = recipientPeerIds,
            }), ct: ct);
    }

    private async Task<string[]> ResolveReviewerPeerIdsAsync(string? requestedReviewers, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(requestedReviewers))
            return [];

        List<string>? targets;
        try
        {
            targets = JsonSerializer.Deserialize<List<string>>(requestedReviewers);
        }
        catch (JsonException)
        {
            return [];
        }

        if (targets is null || targets.Count == 0)
            return [];

        var recipientPeerIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var target in targets.Where(target => !string.IsNullOrWhiteSpace(target)))
        {
            var activePeer = await peers.GetAsync(target, ct);
            if (activePeer is not null && activePeer.ExpiredAt is null)
            {
                recipientPeerIds.Add(activePeer.PeerId);
                continue;
            }

            var repoPeers = await peers.ListAsync(repoRoot: target, includeExpired: false, ct: ct);
            foreach (var repoPeer in repoPeers)
                recipientPeerIds.Add(repoPeer.PeerId);
        }

        return recipientPeerIds.ToArray();
    }

    private async Task<string[]> ResolveConversationPeerIdsAsync(Scratchpad pad, string? authorPeerId, CancellationToken ct)
    {
        var recipientPeerIds = new HashSet<string>(await ResolveReviewerPeerIdsAsync(pad.RequestedReviewers, ct), StringComparer.Ordinal)
        {
            pad.CreatedByPeerId,
        };

        if (!string.IsNullOrWhiteSpace(authorPeerId))
            recipientPeerIds.Remove(authorPeerId);

        return recipientPeerIds.ToArray();
    }
}
