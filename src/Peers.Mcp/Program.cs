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

using var serviceProvider = services.BuildServiceProvider();

var options = ServerOptionsFactory.Create(serviceProvider);

var transport = new StdioServerTransport(options, loggerFactory: null);
// No `await using`: McpHost.RunUntilTransportClosedAsync takes ownership and disposes it, and it has to —
// disposal is what releases a parked subscriptions/listen, so it happens BEFORE the run task is awaited,
// not after it returns.
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
