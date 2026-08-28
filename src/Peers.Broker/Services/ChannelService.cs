using System.Text.Json;
using FluentResults;
using Peers.Broker.Errors;
using Peers.Broker.Persistence.Repositories;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Broker.Services;

/// <summary>
/// Channels are named message boards scoped optionally to a repository. Mentions are explicit — the
/// <c>To</c> list on a send — and resolved against the live roster with the same resolver review
/// requests used before channels existed. Quiet sends are whispers: delivered to mentions only, hidden
/// from every other participant's reads, notifications, and subscription fan-out.
/// </summary>
public sealed class ChannelService(
    ChannelRepository channels,
    PeerRepository peers,
    EventService eventService,
    RecipientResolver resolver)
{
    /// <summary>Peers this one corresponds with — the audience for its presence changes.</summary>
    public Task<List<string>> CorrespondentPeerIdsAsync(string peerId, CancellationToken ct = default)
        => channels.GetCorrespondentPeerIdsAsync(peerId, ct);

    public async Task<Result<CreateChannelResponse>> CreateAsync(CreateChannelRequest req, CancellationToken ct)
    {
        if (await channels.GetByNameAsync(req.Name, req.RepoRoot, ct) is { } existing)
            return Result.Fail(new BadRequestError(
                $"Channel '{req.Name}' already exists in this scope ({existing.RepoRoot ?? "global"})."));

        var id = Guid.NewGuid().ToString("N")[..16];
        var channel = new ChannelRow
        {
            ChannelId = id,
            Name = req.Name,
            RepoRoot = req.RepoRoot,
            CreatedByPeerId = req.CreatedByPeerId,
            CreatedAt = DateTime.UtcNow.ToString("O"),
        };
        await channels.InsertAsync(channel, ct);

        await eventService.EmitAsync(EventTypes.ChannelCreated, JsonSerializer.Serialize(new
        {
            channel_id = id,
            name = req.Name,
            repo_root = req.RepoRoot,
            created_by_peer_id = req.CreatedByPeerId,
        }), ct);

        return Result.Ok(new CreateChannelResponse
        {
            ChannelId = id,
            Name = req.Name,
            RepoRoot = req.RepoRoot,
        });
    }

    public async Task<Result<SendMessageResponse?>> SendAsync(string channelId, SendMessageRequest req, CancellationToken ct)
    {
        var channel = await channels.GetAsync(channelId, ct);
        if (channel is null)
            return Result.Fail(new NotFoundError($"Channel '{channelId}' not found."));

        var callerBranch = (await peers.GetAsync(req.AuthorPeerId, ct))?.Branch;
        var resolveResult = await resolver.ResolveAsync(req.To ?? [], callerBranch, req.AuthorPeerId, ct);
        if (resolveResult.IsFailed)
            return resolveResult.ToResult<SendMessageResponse?>();
        var resolved = resolveResult.Value;

        // A quiet message nobody is mentioned in would be invisible to every future reader forever.
        var mentions = resolved.EventRecipientPeerIds();
        if (req.Quiet && mentions.Length == 0)
            return Result.Fail(new BadRequestError(
                "A quiet message requires at least one mention in 'to' — otherwise no one could ever see it."));

        // A retried send is one message. The caller's key is checked after resolution (which the
        // response needs either way) and before the write, so a transport retry re-reads its own prior
        // message instead of posting a second review round. Mentions come from the stored row — the
        // message is immutable — while delivery is resolved now, because "did it reach anyone" is a
        // question about the present, not about the call that happened to win.
        if (req.IdempotencyKey is { Length: > 0 } key
            && await channels.GetByIdempotencyKeyAsync(channelId, key, ct) is { } existing)
        {
            return Result.Ok<SendMessageResponse?>(new SendMessageResponse
            {
                MessageId = existing.MessageId,
                ChannelId = channelId,
                Seq = existing.Seq,
                Mentions = existing.Mentions,
                Delivery = resolved.ToStatus(),
                Deduplicated = true,
            });
        }

        var messageId = Guid.NewGuid().ToString("N")[..16];
        var row = await channels.AppendMessageAsync(new MessageRow
        {
            MessageId = messageId,
            ChannelId = channelId,
            AuthorPeerId = req.AuthorPeerId,
            Content = req.Content,
            Mentions = mentions,
            Quiet = req.Quiet,
            IdempotencyKey = req.IdempotencyKey,
            CreatedAt = DateTime.UtcNow.ToString("O"),
        }, ct);

        await eventService.EmitAsync(EventTypes.ChannelMessageAdded, JsonSerializer.Serialize(new
        {
            channel_id = channelId,
            channel_name = channel.Name,
            repo_root = channel.RepoRoot,
            message_id = messageId,
            seq = row.Seq,
            author_peer_id = req.AuthorPeerId,
            content = req.Content,
            mentions = mentions,
            quiet = req.Quiet,
            recipient_peer_ids = mentions,
        }), ct, mentions.Length == 0 ? null : mentions);

        return Result.Ok<SendMessageResponse?>(new SendMessageResponse
        {
            MessageId = messageId,
            ChannelId = channelId,
            Seq = row.Seq,
            Mentions = mentions,
            Delivery = resolved.ToStatus(),
        });
    }

    public async Task<ReadMessagesResponse?> ReadAsync(
        string channelId, string? viewer, long sinceSeq, int limit, CancellationToken ct)
    {
        if (await channels.GetAsync(channelId, ct) is null) return null;

        // +1 so the caller can tell "exactly limit" from "more remain" without a second round trip.
        var rows = await channels.ReadMessagesAsync(channelId, viewer, sinceSeq, limit + 1, ct);
        return new ReadMessagesResponse
        {
            ChannelId = channelId,
            Messages = rows.Take(limit).Select(ToDto).ToArray(),
            Truncated = rows.Count > limit,
        };
    }

    public async Task<ListChannelsResponse> ListAsync(string? repoRoot, int limit, CancellationToken ct)
    {
        var list = await channels.ListAsync(repoRoot, limit, ct);
        return new ListChannelsResponse { Channels = list.ToArray() };
    }

    private static MessageDto ToDto(MessageRow row) => new()
    {
        MessageId = row.MessageId,
        ChannelId = row.ChannelId,
        Seq = row.Seq,
        AuthorPeerId = row.AuthorPeerId,
        Content = row.Content,
        Mentions = row.Mentions,
        Quiet = row.Quiet,
        CreatedAt = row.CreatedAt,
    };
}
