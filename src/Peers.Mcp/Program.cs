using System.Reflection;
using System.Text.Json;
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
    ServerInstructions = """
        Peers coordinates multiple AI coding sessions through a shared local broker.

        Message delivery modes:
        - Push mode: Codex sessions receive best-effort standard MCP `notifications/message` pushes. Claude Code can also receive direct channel pushes if started with `--dangerously-load-development-channels server:peers`.
        - Pull mode: always available via `peer_inbox`, which drains buffered directed messages captured by this session.

        Usage guidance:
        - Call `peer_register` at startup.
        - Call `peer_inbox` at startup, after completing a task chunk, when the user asks about peer activity, and before finishing.
        - Use `peer_poll` only when you need raw broker events or cursor-based debugging.
        - Use scratchpads for replies and longer async collaboration threads.
        """,
    ToolCollection = [],
    Capabilities = new ServerCapabilities
    {
        Tools = new() { ListChanged = false },
        Logging = new(),
        Experimental = new Dictionary<string, object>
        {
            ["claude/channel"] = JsonDocument.Parse("{}").RootElement.Clone()
        }
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
    if (sessionState.PeerId is not null)
        await client.DeregisterAsync(sessionState.PeerId);
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
