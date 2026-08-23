using Peers.Broker.Errors;
using Peers.Broker.Persistence;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Routes;
using Peers.Broker.Services;

namespace Peers.Broker.Hosting;

public static class BrokerHost
{
    public static SqliteConnectionFactory AddBrokerServices(WebApplicationBuilder builder, string dbPath)
    {
        var dbFactory = new SqliteConnectionFactory(dbPath);
        using (var conn = dbFactory.CreateConnection())
        {
            conn.Open();
            Schema.Migrate(conn);
        }

        builder.Services.AddSingleton(dbFactory);

        // Repositories (Scoped)
        builder.Services.AddScoped<PeerRepository>();
        builder.Services.AddScoped<EventRepository>();
        builder.Services.AddScoped<ScratchpadRepository>();
        builder.Services.AddScoped<ArtifactRepository>();
        builder.Services.AddScoped<InboxRepository>();

        // Services (Scoped)
        builder.Services.AddScoped<PeerService>();
        builder.Services.AddScoped<RecipientResolver>();
        builder.Services.AddScoped<ScratchpadService>();
        builder.Services.AddScoped<ArtifactService>();
        builder.Services.AddScoped<EventService>();

        // Background / status
        builder.Services.AddSingleton<BrokerStatus>();
        builder.Services.AddHostedService<CleanupBackgroundService>();

        return dbFactory;
    }

    public static void MapBrokerRoutes(WebApplication app)
    {
        app.UseMiddleware<BrokerExceptionMiddleware>();

        app.MapPeerRoutes();
        app.MapScratchpadRoutes();
        app.MapArtifactRoutes();
        app.MapPollRoutes();
        app.MapStatusRoutes();
    }
}
