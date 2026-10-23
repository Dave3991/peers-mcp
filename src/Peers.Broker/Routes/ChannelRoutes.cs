using FluentResults;
using Peers.Broker.Errors;
using Peers.Broker.Validation;
using Peers.Broker.Services;
using Peers.Shared.Contracts;

namespace Peers.Broker.Routes;

public static class ChannelRoutes
{
    public static void MapChannelRoutes(this WebApplication app)
    {
        app.MapPost("/channels", async (CreateChannelRequest req, ChannelService svc, CancellationToken ct) =>
        {
            if (RequestLimits.ValidateChannelCreate(req) is { } err) return err;
            return (await svc.CreateAsync(req, ct)).ToHttp();
        });

        app.MapGet("/channels", async (string? repo_root, int? limit, ChannelService svc, CancellationToken ct) =>
            Results.Ok(await svc.ListAsync(repo_root, limit ?? 100, ct)));

        app.MapPost("/channels/{id}/messages", async (string id, SendMessageRequest req, ChannelService svc, CancellationToken ct) =>
        {
            if (RequestLimits.ValidateSendMessage(req) is { } err) return err;
            return (await svc.SendAsync(id, req, ct)).ToHttp();
        });

        // viewer controls quiet-message visibility: pass the reading peer's id; omit it to see only
        // broadcast messages. since_seq pages; the response says whether more remain.
        app.MapGet("/channels/{id}/messages", async (string id, string? viewer, long? since_seq, int? limit,
            ChannelService svc, CancellationToken ct) =>
        {
            var result = await svc.ReadAsync(id, viewer, since_seq ?? 0, limit ?? 200, ct);
            return result is not null ? Results.Ok(result) : Results.NotFound(new { error = $"Channel '{id}' not found." });
        });

        app.MapDelete("/channels/{id}/messages", async (string id, ChannelService svc, CancellationToken ct) =>
            (await svc.ClearAsync(id, ct)).ToHttp());

        app.MapPost("/channels/{id}/messages/remove", async (string id, RemoveMessagesRequest req,
            ChannelService svc, CancellationToken ct) =>
        {
            if (RequestLimits.ValidateRemoveMessages(req) is { } err) return err;
            return (await svc.RemoveMessagesAsync(id, req, ct)).ToHttp();
        });
    }
}
