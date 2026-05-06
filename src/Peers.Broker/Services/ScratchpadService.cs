using System.Text;
using System.Text.Json;
using FluentResults;
using Peers.Broker.Errors;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

public sealed class ScratchpadService(ScratchpadRepository pads, EventService eventService, PeerRepository peers)
{
    public async Task<Result<CreateScratchpadResponse>> CreateAsync(CreateScratchpadRequest req, CancellationToken ct)
    {
        var id = Guid.NewGuid().ToString("N")[..16];
        var callerPeer = await peers.GetAsync(req.CreatedByPeerId, ct);
        var resolveResult = await TryResolveReviewerPeerIdsAsync(req.RequestedReviewers, callerPeer?.Branch, ct);
        if (resolveResult.IsFailed) return resolveResult.ToResult<CreateScratchpadResponse>();
        var resolved = resolveResult.Value;
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
        var recipientIds = resolved.EventRecipientPeerIds();
        await eventService.EmitAsync(EventTypes.ScratchpadCreated,
            JsonSerializer.Serialize(new
            {
                scratchpad_id = id,
                type = req.Type,
                title = req.Title,
                created_by_peer_id = req.CreatedByPeerId,
                recipient_peer_ids = recipientIds,
            }), ct, recipientIds);

        return Result.Ok(new CreateScratchpadResponse
        {
            ScratchpadId = id,
            Recipients = resolved.ToStatus(),
        });
    }

    public async Task<Result<AppendEntryResponse?>> AppendAsync(string scratchpadId, AppendEntryRequest req, CancellationToken ct)
    {
        var pad = await pads.GetAsync(scratchpadId, ct);
        if (pad is null)
            return Result.Fail(new NotFoundError($"Scratchpad '{scratchpadId}' not found."));
        if (pad.Status == "closed")
            return Result.Fail(new ConflictError($"Scratchpad '{scratchpadId}' is closed and cannot accept new entries."));

        var conversationResult = await TryResolveConversationPeerIdsAsync(pad, req.AuthorPeerId, ct);
        if (conversationResult.IsFailed) return conversationResult.ToResult<AppendEntryResponse?>();
        var resolved = conversationResult.Value;

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
        if (result is null) return Result.Ok<AppendEntryResponse?>(null); // idempotent duplicate

        var recipientIds = resolved.EventRecipientPeerIds();
        await eventService.EmitAsync(EventTypes.ScratchpadEntryAdded,
            JsonSerializer.Serialize(new
            {
                scratchpad_id = scratchpadId,
                entry_id = entryId,
                title = pad.Title,
                author_peer_id = req.AuthorPeerId,
                content = req.Content,
                created_by_peer_id = pad.CreatedByPeerId,
                recipient_peer_ids = recipientIds,
                quiet = req.Quiet,
            }), ct, recipientIds);

        return Result.Ok<AppendEntryResponse?>(new AppendEntryResponse
        {
            EntryId = entryId,
            Recipients = resolved.ToStatus(),
        });
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

    public async Task<Result> ResolveAsync(string scratchpadId, ResolveScratchpadRequest req, CancellationToken ct)
    {
        var pad = await pads.GetAsync(scratchpadId, ct);
        if (pad is null)
            return Result.Fail(new NotFoundError($"Scratchpad '{scratchpadId}' not found."));
        if (pad.Status == "closed")
            return Result.Fail(new ConflictError($"Scratchpad '{scratchpadId}' is closed; cannot resolve."));

        await pads.ResolveAsync(scratchpadId, req.Resolution, ct);
        var conversationResult = await TryResolveConversationPeerIdsAsync(pad, authorPeerId: null, ct);
        if (conversationResult.IsFailed) return conversationResult.ToResult();
        var resolved = conversationResult.Value;

        var recipientIds = resolved.EventRecipientPeerIds();
        await eventService.EmitAsync(EventTypes.ScratchpadResolved,
            JsonSerializer.Serialize(new
            {
                scratchpad_id = scratchpadId,
                title = pad.Title,
                resolution = req.Resolution,
                recipient_peer_ids = recipientIds,
            }), ct, recipientIds);
        return Result.Ok();
    }

    public async Task<Result> CloseAsync(string scratchpadId, CloseScratchpadRequest req, CancellationToken ct)
    {
        var pad = await pads.GetAsync(scratchpadId, ct);
        if (pad is null)
            return Result.Fail(new NotFoundError($"Scratchpad '{scratchpadId}' not found."));

        await pads.CloseAsync(scratchpadId, req.Resolution, ct);
        var closedPad = await pads.GetAsync(scratchpadId, ct) ?? pad;
        var conversationResult = await TryResolveConversationPeerIdsAsync(closedPad, authorPeerId: null, ct);
        if (conversationResult.IsFailed) return conversationResult.ToResult();
        var resolved = conversationResult.Value;

        var recipientIds = resolved.EventRecipientPeerIds();
        await eventService.EmitAsync(EventTypes.ScratchpadClosed,
            JsonSerializer.Serialize(new
            {
                scratchpad_id = scratchpadId,
                title = closedPad.Title,
                resolution = req.Resolution,
                recipient_peer_ids = recipientIds,
            }), ct, recipientIds);
        return Result.Ok();
    }

    private async Task<Result<ResolvedRecipients>> TryResolveReviewerPeerIdsAsync(string? requestedReviewers, string? callerBranch, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(requestedReviewers))
            return Result.Ok(ResolvedRecipients.Empty);

        List<string>? targets;
        try
        {
            targets = JsonSerializer.Deserialize<List<string>>(requestedReviewers);
        }
        catch (JsonException ex)
        {
            return Result.Fail(new BadRequestError($"requested_reviewers is not valid JSON array of strings: {ex.Message}"));
        }

        if (targets is null || targets.Count == 0)
            return Result.Ok(ResolvedRecipients.Empty);

        var online = new HashSet<string>(StringComparer.Ordinal);
        var offline = new HashSet<string>(StringComparer.Ordinal);
        var unresolved = new List<string>();

        foreach (var target in targets.Where(target => !string.IsNullOrWhiteSpace(target)))
        {
            // Try exact or prefix match on peer ID
            var peer = await peers.GetByPrefixAsync(target, ct);
            if (peer is not null)
            {
                if (peer.ExpiredAt is null)
                    online.Add(peer.PeerId);
                else
                    offline.Add(peer.PeerId);
                continue;
            }

            // Fall back to repo root match — prefer same-branch peers
            var repoPeers = await peers.ListAsync(repoRoot: target, includeExpired: true, ct: ct);
            if (repoPeers.Count == 0)
            {
                unresolved.Add(target);
                continue;
            }

            // Prefer active same-branch peers; fall back to all repo peers
            var activeSameBranch = repoPeers.Where(p => p.Branch == callerBranch && callerBranch is not null && p.ExpiredAt is null).ToList();
            var candidates = activeSameBranch.Count > 0 ? activeSameBranch : repoPeers;

            foreach (var repoPeer in candidates)
            {
                if (repoPeer.ExpiredAt is null)
                    online.Add(repoPeer.PeerId);
                else
                    offline.Add(repoPeer.PeerId);
            }
        }

        // A peer found online via one target shouldn't also appear offline from another
        offline.ExceptWith(online);

        return Result.Ok(new ResolvedRecipients(
            [.. online],
            [.. offline],
            [.. unresolved]));
    }

    private async Task<Result<ResolvedRecipients>> TryResolveConversationPeerIdsAsync(Scratchpad pad, string? authorPeerId, CancellationToken ct)
    {
        var callerPeer = authorPeerId is not null ? await peers.GetAsync(authorPeerId, ct) : null;
        var reviewerResult = await TryResolveReviewerPeerIdsAsync(pad.RequestedReviewers, callerPeer?.Branch, ct);
        if (reviewerResult.IsFailed) return reviewerResult;
        var resolved = reviewerResult.Value;

        var online = new HashSet<string>(resolved.OnlinePeerIds, StringComparer.Ordinal);
        var offline = new HashSet<string>(resolved.OfflinePeerIds, StringComparer.Ordinal);

        // Classify the scratchpad creator
        var creator = await peers.GetAsync(pad.CreatedByPeerId, ct);
        if (creator is not null)
        {
            if (creator.ExpiredAt is null)
                online.Add(creator.PeerId);
            else
                offline.Add(creator.PeerId);
        }

        // Remove author from all sets (don't notify yourself)
        if (!string.IsNullOrWhiteSpace(authorPeerId))
        {
            online.Remove(authorPeerId);
            offline.Remove(authorPeerId);
        }

        offline.ExceptWith(online);

        return Result.Ok(new ResolvedRecipients(
            [.. online],
            [.. offline],
            resolved.UnresolvedTargets));
    }

    private sealed record ResolvedRecipients(
        string[] OnlinePeerIds,
        string[] OfflinePeerIds,
        string[] UnresolvedTargets)
    {
        public static readonly ResolvedRecipients Empty = new([], [], []);

        public string[] EventRecipientPeerIds() =>
            [.. OnlinePeerIds.Concat(OfflinePeerIds)];

        public RecipientDeliveryStatus ToStatus() => new()
        {
            OnlinePeerIds = OnlinePeerIds,
            OfflinePeerIds = OfflinePeerIds,
            UnresolvedTargets = UnresolvedTargets,
        };
    }
}
