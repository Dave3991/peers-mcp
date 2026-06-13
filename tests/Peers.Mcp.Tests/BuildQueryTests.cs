using Peers.Mcp;
using Xunit;

namespace Peers.Mcp.Tests;

public sealed class BuildQueryTests
{
    [Fact]
    public void BuildQuery_EscapesSpecialCharactersInValues()
    {
        var result = BrokerClient.BuildQuery(
            ("a", "x&y"),
            ("b", "c#d"),
            ("e", "f g"));

        Assert.Contains("a=x%26y", result);
        Assert.Contains("b=c%23d", result);
        Assert.Contains("e=f%20g", result);
    }

    [Fact]
    public void BuildQuery_OmitsNullValues()
    {
        var result = BrokerClient.BuildQuery(
            ("present", "value"),
            ("absent", null));

        Assert.Contains("present=value", result);
        Assert.DoesNotContain("absent", result);
    }

    [Fact]
    public void BuildQuery_ReturnsEmptyString_WhenAllValuesNull()
    {
        var result = BrokerClient.BuildQuery(("a", null), ("b", null));
        Assert.Equal("", result);
    }

    [Fact]
    public void BuildQuery_PrefixesQuestionMark_WhenAnyValuePresent()
    {
        var result = BrokerClient.BuildQuery(("k", "v"));
        Assert.StartsWith("?", result);
    }
}
