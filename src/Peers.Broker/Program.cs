using Peers.Broker.Hosting;
using Peers.Broker.Persistence;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Routes;
using Peers.Broker.Services;
using Peers.Shared;

var builder = WebApplication.CreateBuilder(args);

var dbPath = PeersPaths.GetDbPath();

var port = BrokerRuntime.GetPort();

builder.WebHost.UseUrls($"http://127.0.0.1:{port}");

// Persistence
var dbFactory = new SqliteConnectionFactory(dbPath);
using (var conn = dbFactory.CreateConnection())
{
    conn.Open();
    Schema.Migrate(conn);
}

builder.Services.AddSingleton(dbFactory);
builder.Services.AddScoped<PeerRepository>();
builder.Services.AddScoped<EventRepository>();
builder.Services.AddScoped<ScratchpadRepository>();
builder.Services.AddScoped<ArtifactRepository>();

// Services
builder.Services.AddScoped<PeerService>();
builder.Services.AddScoped<ScratchpadService>();
builder.Services.AddScoped<ArtifactService>();
builder.Services.AddScoped<EventService>();

// Background
builder.Services.AddHostedService<CleanupBackgroundService>();

var app = builder.Build();

app.MapPeerRoutes();
app.MapScratchpadRoutes();
app.MapArtifactRoutes();
app.MapPollRoutes();

var discoveryDir = PeersPaths.GetStateDirectory();
var discoveryFile = PeersPaths.GetDiscoveryFile();
Directory.CreateDirectory(discoveryDir);

await app.StartAsync();

try
{
    try
    {
        await WriteDiscoveryFileAsync(discoveryFile, port);
    }
    catch
    {
        await app.StopAsync();
        throw;
    }

    await app.WaitForShutdownAsync();
}
finally
{
    try { File.Delete(discoveryFile); } catch { }
    await app.DisposeAsync();
}

static async Task WriteDiscoveryFileAsync(string discoveryFile, int port)
{
    var discovery = System.Text.Json.JsonSerializer.Serialize(new
    {
        port,
        pid = Environment.ProcessId,
        started_at = DateTime.UtcNow.ToString("O"),
        schema_version = 1,
    });

    var tempFile = $"{discoveryFile}.{Environment.ProcessId}.tmp";

    try
    {
        await File.WriteAllTextAsync(tempFile, discovery);
        File.Move(tempFile, discoveryFile, overwrite: true);
    }
    finally
    {
        try
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
        catch
        {
        }
    }
}
