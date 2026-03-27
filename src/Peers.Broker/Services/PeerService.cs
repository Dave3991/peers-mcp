using System.Text.Json;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;
using Peers.Shared.Ids;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

public sealed class PeerService(PeerRepository peers, EventRepository events)
{
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromMinutes(2);

    public async Task<RegisterPeerResponse> RegisterAsync(RegisterPeerRequest req, CancellationToken ct)
    {
        var id = PeerId.New().Value;
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
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        };

        await peers.InsertAsync(peer, ct);

        var otherPeers = await peers.ListAsync(repoRoot: req.RepoRoot, includeExpired: false, ct: ct);
        var recipientPeerIds = otherPeers
            .Where(p => p.PeerId != id)
            .Select(p => p.PeerId)
            .ToArray();

        await events.InsertAsync(EventTypes.PeerRegistered,
            JsonSerializer.Serialize(new { peer_id = id, tool = req.Tool, repo_root = req.RepoRoot, recipient_peer_ids = recipientPeerIds }), ct: ct);

        return new RegisterPeerResponse { PeerId = id };
    }

    public async Task HeartbeatAsync(string peerId, HeartbeatRequest req, CancellationToken ct)
    {
        await peers.RenewLeaseAsync(peerId, DateTime.UtcNow.Add(LeaseDuration),
            req.Summary, req.Role, req.Branch, req.CommitSha, req.FocusPaths, ct);
    }

    public async Task<List<Peer>> ListAsync(ListPeersRequest req, CancellationToken ct)
    {
        return await peers.ListAsync(req.RepoRoot, req.Branch, req.Tool, includeExpired: false, ct);
    }

    public async Task<int> ExpireStaleAsync(CancellationToken ct)
    {
        var count = await peers.ExpireStaleAsync(ct);
        if (count > 0)
        {
            var activePeers = await peers.ListAsync(includeExpired: false, ct: ct);
            var recipientPeerIds = activePeers.Select(p => p.PeerId).ToArray();

            await events.InsertAsync(EventTypes.PeerExpired,
                JsonSerializer.Serialize(new { expired_count = count, recipient_peer_ids = recipientPeerIds }), ct: ct);
        }
        return count;
    }
}
