using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Peers.Mcp;
using Peers.Mcp.Lifecycle;
var client = await BrokerDiscovery.EnsureBrokerAsync();
var sessionState = new SessionState();

var services = new ServiceCollection();
services.AddLogging(builder =>
{
    builder.AddConsole(o => o.LogToStandardErrorThreshold = LogLevel.Trace);
    builder.SetMinimumLevel(LogLevel.Information);
});
services.AddSingleton(client);
services.AddSingleton(sessionState);
services.AddSingleton(new Peers.Mcp.Reviews.ReviewStorageOptions(Peers.Shared.PeersPaths.GetStateDirectory()));

using var serviceProvider = services.BuildServiceProvider();

var options = ServerOptionsFactory.Create(serviceProvider);

var transport = new StdioServerTransport(options, loggerFactory: null);
// No `await using`: McpHost.RunUntilTransportClosedAsync takes ownership and disposes it, and disposal
// must happen BEFORE the run task is awaited — only server disposal releases an in-flight handler, so
// awaiting RunAsync first could deadlock stdio exit. See McpHost's pending-verification note.
var server = McpServer.Create(transport, options, loggerFactory: null, serviceProvider);

try
{
    await McpHost.RunUntilTransportClosedAsync(server, transport);
}
finally
{
    // RunAsync returned, so the transport is already CLOSED, not closing: nothing here can put a frame on
    // the wire, and nothing tries to. This stops the heartbeat and poll loops, then releases the lease.
    await sessionState.DisposeAsync();
    if (sessionState.PeerId is not null)
        await client.DeregisterAsync(sessionState.PeerId);
    client.Dispose();
}
