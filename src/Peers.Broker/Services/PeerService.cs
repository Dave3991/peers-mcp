using System.Text.Json;
using FluentResults;
using Microsoft.Extensions.Logging;
using Peers.Broker.Errors;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared;
using Peers.Shared.Contracts;
using Peers.Shared.Ids;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

public sealed class PeerService(PeerRepository peers, EventService eventService, ChannelService channelService, ILogger<PeerService>? logger = null)
{
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromMinutes(2);
    // 7d (not 24h) so ScratchpadService.FindActiveReplacementAsync can still reroute a restarted reviewer to their live session across multi-day review pads.
    private static readonly TimeSpan ExpiredPeerRetention = TimeSpan.FromDays(7);

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

        if (resumedPeer is null)
        {
            // Fresh joins announce themselves on every channel scoped to this repo plus global channels.
            // ListAsync(null) returns ALL channels (@repo IS NULL short-circuits the WHERE clause).
            var matchedChannels = (await channelService.ListAsync(repoRoot: null, 100, ct)).Channels
                .Where(c => c.RepoRoot is null || c.RepoRoot == req.RepoRoot)
                .ToList();

            if (matchedChannels.Count > 0)
            {
                // Combined wake recipients FIRST, board rows NEXT, wake LAST — so rows are readable
                // before anyone is woken. Net-new recipients: live members of any repo-matched
                // channel, or everyone when a global channel matched. Union-distinct: EmitAsync
                // inserts one inbox row per array entry.
                var livePeers = await peers.ListAsync(includeExpired: false, ct: ct);
                var netNew = livePeers
                    .Where(p => p.PeerId != id)
                    .Where(p => matchedChannels.Any(c => c.RepoRoot == p.RepoRoot) ||
                                matchedChannels.Any(c => c.RepoRoot is null))
                    .Select(p => p.PeerId);

                recipientPeerIds = recipientPeerIds.Union(netNew).ToArray();

                foreach (var channel in matchedChannels)
                {
                    Result<SendMessageResponse?> sendResult;
                    try
                    {
                        sendResult = await channelService.SendAsync(channel.ChannelId, new SendMessageRequest
                        {
                            AuthorPeerId = id,
                            Content = $"peer {id[..8]} ({req.Tool}) joined {req.RepoRoot ?? "global"}",
                            Quiet = false,
                            To = null,
                        }, ct);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        logger?.LogWarning(ex, "Join announcement to channel {ChannelId} failed for peer {PeerId}", channel.ChannelId, id);
                        continue;
                    }

                    if (sendResult.IsFailed)
                    {
                        logger?.LogWarning("Join announcement to channel {ChannelId} failed for peer {PeerId}: {Errors}",
                            channel.ChannelId, id, string.Join("; ", sendResult.Errors.Select(e => e.Message)));
                        continue;
                    }
                }
            }
        }

        var eventId = await eventService.EmitAsync(EventTypes.PeerRegistered,
            JsonSerializer.Serialize(new { peer_id = id, tool = req.Tool, repo_root = req.RepoRoot, recipient_peer_ids = recipientPeerIds }),
            ct, recipientPeerIds);

        return new RegisterPeerResponse
        {
            PeerId = id,
            LastCursor = resumedPeer?.LastPolledEventId ?? 0,
            Resumed = resumedPeer is not null,
            LatestEventId = eventId,
        };
    }

    public async Task<Result> HeartbeatAsync(string peerId, HeartbeatRequest req, CancellationToken ct)
    {
        var rows = await peers.RenewLeaseAsync(peerId, DateTime.UtcNow.Add(LeaseDuration),
            req.Summary, req.Role, req.Branch, req.CommitSha, req.FocusPaths, req.Capabilities,
            req.ProgressCurrent, req.ProgressTotal, ct);
        if (rows == 0)
            return Result.Fail(new NotFoundError($"Peer '{peerId}' not found. Re-register before sending heartbeats."));

        // Only a heartbeat that actually carries a status field changes what peers://peers shows, and only
        // that one may emit. The 30s background heartbeat sends an all-null request purely to renew the
        // lease, so an idle peer stays at 0 events/min — a per-peer 2/min event floor would be a bigger
        // regression than the stale-resource bug this fixes.
        //
        // No recipients: this is a resource-change signal, not a directed message. Recipients would put a
        // row in every peer's durable inbox and surface as unread traffic in peer_inbox.
        //
        // ponytail: no rate gate on progress. The PUSH side is already coalesced — ChannelNotifier polls
        // at 5s and SubscriptionRegistry unions a batch into one hint per URI — so a gate here would be a
        // second copy of that in the wrong process, and PeerService is scoped so it would need static
        // state. What stays uncapped is one small events row per progress call, and note these are NOT
        // "pruned within seconds": PruneAsync prunes below the MINIMUM active cursor, so one live peer
        // whose cursor never advances pins the table until the 72h TTL. The real rate ceiling is LLM
        // tool-call latency. Revisit only if /status events_last_5min says otherwise.
        if (Changes(req))
            await eventService.EmitAsync(EventTypes.PeerUpdated,
                JsonSerializer.Serialize(new { peer_id = peerId, progress = ProgressText(req) }), ct);

        return Result.Ok();
    }

    /// <summary>True when the heartbeat supplied at least one status field. RenewLeaseAsync COALESCEs nulls,
    /// so an all-null request provably cannot change a single column other than the lease.</summary>
    private static bool Changes(HeartbeatRequest req) =>
        req.Summary is not null || req.Role is not null || req.Branch is not null ||
        req.CommitSha is not null || req.FocusPaths is not null || req.Capabilities is not null ||
        req.ProgressCurrent is not null;

    /// <summary>
    /// Rendered counts for the event payload, or null when this heartbeat carried no progress.
    ///
    /// This exists for ONE consumer: the CLI's `events --follow`, whose SummarizePayload cherry-picks
    /// string keys and would otherwise print a bare peer_id for every progress tick. Nothing
    /// reconstructs state from here — the resource hint still means "re-read peers://peers". A null
    /// serialises as "progress": null, which that renderer skips.
    /// </summary>
    private static string? ProgressText(HeartbeatRequest req) =>
        req.ProgressCurrent is null ? null : ProgressFormat.Counts(req.ProgressCurrent, req.ProgressTotal);

    public async Task<List<Peer>> ListAsync(ListPeersRequest req, CancellationToken ct)
    {
        return await peers.ListAsync(req.RepoRoot, req.Branch, req.Tool, includeExpired: req.IncludeExpired, ct);
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

    public async Task<int> PurgeExpiredAsync(CancellationToken ct)
        => await peers.PurgeExpiredBeforeAsync(DateTime.UtcNow - ExpiredPeerRetention, ct);

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

        // If an active peer matches, the session is still alive — don't resume.
        // (Multiple active peers per identity is INTENTIONAL: concurrent sessions in the same
        // repo/branch/worktree get distinct ephemeral peer_ids; resume only reclaims EXPIRED
        // identities. Concurrent same-identity registers yielding two rows is by design, not a race.)
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
