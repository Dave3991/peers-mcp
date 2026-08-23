using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Peers.Mcp;

/// <summary>
/// The session's run loop. Lives here rather than in Program.cs for the same reason
/// <see cref="ServerOptionsFactory"/> does: the integration tests drive the real sequence instead of a
/// second copy that passes while production deadlocks.
/// </summary>
internal static class McpHost
{
    /// <summary>
    /// Runs until the transport closes, then ends the session. TAKES OWNERSHIP of <paramref name="server"/>
    /// and disposes it — callers must not also wrap it in an <c>await using</c>.
    ///
    /// NOT just <c>await server.RunAsync()</c>, and the difference is a hang, not a nicety. Measured
    /// against SDK 2.2.0: RunAsync waits for in-flight request handlers, and a parked
    /// <c>subscriptions/listen</c> is released only when the SERVER IS DISPOSED — transport EOF alone does
    /// not cancel its token. Awaiting RunAsync first therefore deadlocks stdio exit for any session with an
    /// open subscription: the disposal that would release the handler sits behind the handler.
    ///
    /// The transport's inbound channel completing IS stdin EOF, and it is observable without consuming the
    /// channel. WhenAny rather than awaiting it alone, so a RunAsync that faults on its own still gets here
    /// and still surfaces its exception from the join below.
    /// </summary>
    public static async Task RunUntilTransportClosedAsync(McpServer server, ITransport transport)
    {
        var run = server.RunAsync();
        await Task.WhenAny(run, transport.MessageReader.Completion);
        // finally: DisposeAsync releases the parked handler that lets run complete. If disposal itself
        // throws, run must still be joined below or its exception goes unobserved, masked by the
        // disposal exception.
        try { await server.DisposeAsync(); }
        finally { await run; }
    }
}
