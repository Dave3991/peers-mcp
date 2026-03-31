namespace Peers.Mcp;

public static class Limits
{
    public static readonly int MaxToolResultChars =
        int.TryParse(Environment.GetEnvironmentVariable("PEERS_MAX_TOOL_RESULT_CHARS"), out var v) && v > 0
            ? v
            : 30_000;
}
