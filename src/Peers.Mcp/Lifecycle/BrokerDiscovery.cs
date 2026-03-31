using Peers.Shared;

namespace Peers.Mcp.Lifecycle;

public static class BrokerDiscovery
{
    public static async Task<BrokerClient> EnsureBrokerAsync(CancellationToken ct = default)
    {
        var broker = await BrokerRuntime.EnsureBrokerRunningAsync(ct);
        return new BrokerClient($"http://127.0.0.1:{broker.Port}");
    }
}
