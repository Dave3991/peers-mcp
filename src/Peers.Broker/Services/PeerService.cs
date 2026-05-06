using System.Text.Json;
using FluentResults;
using Peers.Broker.Errors;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;
using Peers.Shared.Ids;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

public sealed class PeerService(PeerRepository peers, EventService eventService)
{
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromMinutes(2);

    public async Task<RegisterPeerResponse> RegisterAsync(RegisterPeerRequest req, CancellationToken ct)
    {
        var resumedPeer = await TryFindResumablePeerAsync(req, ct);
        var id = resumedPeer?.PeerId ?? PeerId.New().Value;
        var peer = new Peer
        {
            PeerId = id,
            Tool = req.Tool,
            MachineId = req.MachineId,
            RepoRoot = req.RepoRoot,
            WorktreeRoot = req.WorktreeRoot,
            Branch = req.Branch,
            CommitSha = req.CommitSha,
            Role = req.Role,
            Summary = req.Summary,
            FocusPaths = req.FocusPaths,
            Capabilities = req.Capabilities,
            ParentPeerId = req.ParentPeerId,
            LeaseExpiresAt = DateTime.UtcNow.Add(LeaseDuration),
            CreatedAt = resumedPeer?.CreatedAt ?? DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        };

        if (resumedPeer is null)
            await peers.InsertAsync(peer, ct);
        else
            await peers.ReactivateAsync(peer, ct);

        var otherPeers = await peers.ListAsync(repoRoot: req.RepoRoot, includeExpired: false, ct: ct);
        // Prefer same-branch peers, fall back to all repo peers
        var sameBranch = otherPeers.Where(p => p.PeerId != id && p.Branch == req.Branch && req.Branch is not null).ToList();
        var recipients = sameBranch.Count > 0 ? sameBranch : otherPeers.Where(p => p.PeerId != id).ToList();
        var recipientPeerIds = recipients.Select(p => p.PeerId).ToArray();

        await eventService.EmitAsync(EventTypes.PeerRegistered,
            JsonSerializer.Serialize(new { peer_id = id, tool = req.Tool, repo_root = req.RepoRoot, recipient_peer_ids = recipientPeerIds }),
            ct, recipientPeerIds);

        return new RegisterPeerResponse
        {
            PeerId = id,
            LastCursor = resumedPeer?.LastPolledEventId ?? 0,
            Resumed = resumedPeer is not null,
        };
    }

    public async Task<Result> HeartbeatAsync(string peerId, HeartbeatRequest req, CancellationToken ct)
    {
        var rows = await peers.RenewLeaseAsync(peerId, DateTime.UtcNow.Add(LeaseDuration),
            req.Summary, req.Role, req.Branch, req.CommitSha, req.FocusPaths, ct);
        return rows == 0
            ? Result.Fail(new NotFoundError($"Peer '{peerId}' not found. Re-register before sending heartbeats."))
            : Result.Ok();
    }

    public async Task<List<Peer>> ListAsync(ListPeersRequest req, CancellationToken ct)
    {
        return await peers.ListAsync(req.RepoRoot, req.Branch, req.Tool, includeExpired: false, ct);
    }

    public async Task DeregisterAsync(string peerId, CancellationToken ct)
    {
        var peer = await peers.GetAsync(peerId, ct);
        if (peer is null || peer.ExpiredAt is not null)
            return;

        await peers.ExpireByIdAsync(peerId, ct);

        var activePeers = await peers.ListAsync(includeExpired: false, ct: ct);
        var recipientPeerIds = activePeers.Select(p => p.PeerId).ToArray();

        await eventService.EmitAsync(EventTypes.PeerExpired,
            JsonSerializer.Serialize(new { expired_count = 1, peer_id = peerId, recipient_peer_ids = recipientPeerIds }),
            ct, recipientPeerIds);
    }

    public async Task<int> ExpireStaleAsync(CancellationToken ct)
    {
        var count = await peers.ExpireStaleAsync(ct);
        if (count > 0)
        {
            var activePeers = await peers.ListAsync(includeExpired: false, ct: ct);
            var recipientPeerIds = activePeers.Select(p => p.PeerId).ToArray();

            await eventService.EmitAsync(EventTypes.PeerExpired,
                JsonSerializer.Serialize(new { expired_count = count, recipient_peer_ids = recipientPeerIds }),
                ct, recipientPeerIds);
        }
        return count;
    }

    private async Task<Peer?> TryFindResumablePeerAsync(RegisterPeerRequest req, CancellationToken ct)
    {
        // Explicit resume: caller specifies the exact peer ID to reactivate.
        // Always require MachineId match — resuming another machine's session
        // would let any process steal a peer ID.
        if (!string.IsNullOrWhiteSpace(req.ResumePeerId))
        {
            var target = await peers.GetAsync(req.ResumePeerId, ct);
            if (target is null) return null;
            return target.MachineId == req.MachineId ? target : null;
        }

        // Implicit resume: match on stable identity fields only
        var candidates = await peers.ListAsync(req.RepoRoot, req.Branch, req.Tool, includeExpired: true, ct);
        var matching = candidates
            .Where(peer => MatchesIdentity(peer, req))
            .OrderByDescending(peer => peer.UpdatedAt)
            .ToList();

        // If an active peer matches, the session is still alive — don't resume
        if (matching.Any(peer => peer.ExpiredAt is null))
            return null;

        var expiredMatches = matching
            .Where(peer => peer.ExpiredAt is not null)
            .ToList();

        return expiredMatches.Count == 1 ? expiredMatches[0] : null;
    }

    /// <summary>
    /// Only compare stable identity fields — role, summary, focusPaths, and capabilities
    /// change between sessions and should not prevent resumption.
    /// </summary>
    private static bool MatchesIdentity(Peer peer, RegisterPeerRequest req) =>
        peer.Tool == req.Tool &&
        peer.MachineId == req.MachineId &&
        peer.RepoRoot == req.RepoRoot &&
        peer.WorktreeRoot == req.WorktreeRoot &&
        peer.Branch == req.Branch;
}
