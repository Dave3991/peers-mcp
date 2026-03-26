using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Peers.Mcp;
using Peers.Mcp.Lifecycle;
using Peers.Mcp.Tools;

var client = await BrokerDiscovery.EnsureBrokerAsync();
var sessionState = new SessionState();

var services = new ServiceCollection();
services.AddSingleton(client);
services.AddSingleton(sessionState);

using var serviceProvider = services.BuildServiceProvider();

var options = new McpServerOptions
{
    ServerInfo = new() { Name = "peers", Version = "1.0.0" },
    ToolCollection = [],
    Capabilities = new ServerCapabilities
    {
        Tools = new() { ListChanged = false },
        Logging = new()
    }
};

RegisterTools<PeerTools>(options, serviceProvider);
RegisterTools<ScratchpadTools>(options, serviceProvider);
RegisterTools<ArtifactTools>(options, serviceProvider);
RegisterTools<PollTools>(options, serviceProvider);
RegisterTools<ContextTools>(options, serviceProvider);

await using var server = McpServer.Create(
    new StdioServerTransport(options, loggerFactory: null),
    options,
    loggerFactory: null,
    serviceProvider);

try
{
    await server.RunAsync();
}
finally
{
    await sessionState.DisposeAsync();
    client.Dispose();
}

static void RegisterTools<TToolType>(McpServerOptions options, IServiceProvider services)
{
    var toolCollection = options.ToolCollection ??= [];
    var toolMethods = typeof(TToolType)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(method => method.GetCustomAttribute<McpServerToolAttribute>() is not null);

    foreach (var method in toolMethods)
    {
        if (!method.IsStatic)
            throw new InvalidOperationException($"Tool method '{method.Name}' on '{typeof(TToolType).Name}' must be static.");

        toolCollection.Add(McpServerTool.Create(method, target: null, new()
        {
            Services = services
        }));
    }
}
