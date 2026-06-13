using Peers.Broker.Hosting;
using Peers.Broker.Persistence;
using Peers.Shared;

var builder = WebApplication.CreateBuilder(args);

var dbPath = PeersPaths.GetDbPath();

var port = BrokerRuntime.GetPort();

builder.WebHost.UseUrls($"http://127.0.0.1:{port}");

BrokerHost.AddBrokerServices(builder, dbPath);

var app = builder.Build();

BrokerHost.MapBrokerRoutes(app);

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
    try
    {
        File.Delete(discoveryFile);
    }
    catch (Exception ex)
    {
        app.Logger.LogWarning("Failed to delete discovery file {DiscoveryFile}: {Error}", discoveryFile, ex.Message);
    }
    await app.DisposeAsync();
}

static async Task WriteDiscoveryFileAsync(string discoveryFile, int port)
{
    var discovery = System.Text.Json.JsonSerializer.Serialize(new
    {
        port,
        pid = Environment.ProcessId,
        started_at = DateTime.UtcNow.ToString("O"),
        schema_version = Schema.CurrentVersion,
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
