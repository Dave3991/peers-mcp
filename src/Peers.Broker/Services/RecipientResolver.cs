using FluentResults;
using Peers.Broker.Errors;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;

namespace Peers.Broker.Services;

public sealed record ResolvedRecipients(
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

public sealed class RecipientResolver(PeerRepository peers)
{
    public async Task<Result<ResolvedRecipients>> ResolveAsync(IReadOnlyList<string> targets, string? callerBranch, CancellationToken ct)
    {
        if (targets.Count == 0)
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
                {
                    online.Add(peer.PeerId);
                }
                else
                {
                    // The reviewer's session expired. requested_reviewers binds to an
                    // ephemeral per-session peer ID, so a restarted reviewer that
                    // re-registered under a fresh ID would silently stop receiving
                    // pushes/inbox delivery. Re-route to their current live session on
                    // the same machine if one exists; otherwise report it offline.
                    var replacement = await peers.FindActiveReplacementAsync(peer, ct);
                    if (replacement is not null)
                        online.Add(replacement.PeerId);
                    else
                        offline.Add(peer.PeerId);
                }
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
}
