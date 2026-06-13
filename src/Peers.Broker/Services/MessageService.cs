using System.Text.Json;
using FluentResults;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

public sealed class MessageService(RecipientResolver resolver, EventService eventService, PeerRepository peers)
{
    public async Task<Result<SendMessageResponse>> SendAsync(SendMessageRequest req, CancellationToken ct)
    {
        var caller = await peers.GetAsync(req.FromPeerId, ct);
        var resolveResult = await resolver.ResolveAsync(req.Targets ?? [], caller?.Branch, ct);
        if (resolveResult.IsFailed) return resolveResult.ToResult<SendMessageResponse>();
        var resolved = resolveResult.Value;

        // Exclude the sender from recipients (don't DM yourself), via repo-match OR explicit id.
        var online = resolved.OnlinePeerIds.Where(id => id != req.FromPeerId).ToArray();
        var offline = resolved.OfflinePeerIds.Where(id => id != req.FromPeerId).ToArray();
        var exclSelf = new ResolvedRecipients(online, offline, resolved.UnresolvedTargets);
        var recipientIds = exclSelf.EventRecipientPeerIds();

        // Short-circuit empty sends — don't emit an orphan recipient-less event.
        if (recipientIds.Length == 0)
            return Result.Ok(new SendMessageResponse { Recipients = exclSelf.ToStatus() });

        await eventService.EmitAsync(EventTypes.PeerMessage,
            JsonSerializer.Serialize(new
            {
                from_peer_id = req.FromPeerId,
                content = req.Content,
                recipient_peer_ids = recipientIds,
                quiet = req.Quiet,
            }), ct, recipientIds);

        return Result.Ok(new SendMessageResponse { Recipients = exclSelf.ToStatus() });
    }
}
