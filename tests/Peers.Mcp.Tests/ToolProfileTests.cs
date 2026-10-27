using Xunit;

namespace Peers.Mcp.Tests;

// PEERS_MCP_TOOL_PROFILE mirrors ALETHEIA_MCP_TOOL_PROFILE: only "all" (any case, trimmed) adds the
// opt-in tools; unset, blank, "core", or an unknown value keeps the core profile.
public class ToolProfileTests
{
    [Theory]
    [InlineData(null, false)]
    [InlineData("", false)]
    [InlineData("  ", false)]
    [InlineData("core", false)]
    [InlineData("everything", false)]
    [InlineData("all", true)]
    [InlineData(" ALL ", true)]
    public void OnlyAllSelectsEveryTool(string? profile, bool expected) =>
        Assert.Equal(expected, ServerOptionsFactory.AllToolsRequested(profile));
}
