using Peers.Broker.Errors;
using Peers.Broker.Services;
using Peers.Broker.Validation;
using Peers.Shared.Contracts;

namespace Peers.Broker.Routes;

public static class MessageRoutes
{
    public static void MapMessageRoutes(this WebApplication app)
    {
        app.MapPost("/messages", async (SendMessageRequest req, MessageService svc, CancellationToken ct) =>
        {
            var err = RequestLimits.ValidateSendMessage(req);
            if (err is not null) return err;
            return (await svc.SendAsync(req, ct)).ToHttp();
        });
    }
}
