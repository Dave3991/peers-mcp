using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Peers.Mcp.Reviews;

namespace Peers.Mcp.Tests;

internal static class ProductionServer
{
    /// <summary>The tools exactly as production registers them — descriptions and schemas included, so a
    /// test can hold the advertised contract against the behaviour.</summary>
    internal static IReadOnlyCollection<McpServerTool> Tools(bool allTools)
    {
        var collection = new ServiceCollection();
        // Production registers logging (Program.cs); without it the SDK treats loggerFactory as a
        // declared parameter and leaks it into the schemas the tests pin.
        collection.AddLogging();
        collection.AddSingleton(new BrokerClient(new HttpClient { BaseAddress = new Uri("http://localhost") }));
        collection.AddSingleton(new SessionState());
        collection.AddSingleton(new ReviewStorageOptions(Path.Combine(Path.GetTempPath(), "peers-review-tools-" + Guid.NewGuid().ToString("N"))));
        return ServerOptionsFactory.Create(collection.BuildServiceProvider(), allTools).ToolCollection!;
    }
}
