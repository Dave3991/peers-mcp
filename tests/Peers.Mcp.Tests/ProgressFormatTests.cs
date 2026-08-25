using Peers.Shared;
using Xunit;

namespace Peers.Mcp.Tests;

/// <summary>
/// The shared rendering used by the wait ticker and the CLI. Lives here rather than in a CLI test
/// project because the CLI is a debug tool with no test project of its own — extracting the pure
/// function is what makes both surfaces testable at all.
/// </summary>
public sealed class ProgressFormatTests
{
    [Theory]
    [InlineData(7, 12, "7/12")]
    [InlineData(0, 12, "0/12")]
    [InlineData(12, 12, "12/12")]
    [InlineData(7, null, "7")]      // indeterminate: counting up with no known end
    [InlineData(null, 12, "")]      // total alone is never stored, so it never renders
    [InlineData(null, null, "")]
    public void Counts_RendersTheKnownShapes(int? current, int? total, string expected) =>
        Assert.Equal(expected, ProgressFormat.Counts(current, total));

    // A run that discovers more work mid-flight beats its estimate. Clamping is the honest read of
    // "at or past the estimate"; rejecting it would 400 in the middle of someone's task.
    [Fact]
    public void Counts_ClampsAnOverCount() =>
        Assert.Equal("12/12", ProgressFormat.Counts(15, 12));

    [Fact]
    public void IsFresh_TracksTheWindow()
    {
        var now = new DateTime(2026, 8, 24, 12, 0, 0, DateTimeKind.Utc);
        Assert.True(ProgressFormat.IsFresh(now - TimeSpan.FromSeconds(30), now));
        Assert.True(ProgressFormat.IsFresh(now - ProgressFormat.FreshnessWindow, now));
        Assert.False(ProgressFormat.IsFresh(now - ProgressFormat.FreshnessWindow - TimeSpan.FromSeconds(1), now));
        Assert.False(ProgressFormat.IsFresh(null, now));
    }

    [Fact]
    public void CountsWithAge_AnnotatesOnlyStaleProgress()
    {
        var now = new DateTime(2026, 8, 24, 12, 0, 0, DateTimeKind.Utc);

        Assert.Equal("7/12", ProgressFormat.CountsWithAge(7, 12, now - TimeSpan.FromSeconds(30), now));
        Assert.Equal("7/12 (14m ago)", ProgressFormat.CountsWithAge(7, 12, now - TimeSpan.FromMinutes(14), now));
        Assert.Equal("7 (3h ago)", ProgressFormat.CountsWithAge(7, null, now - TimeSpan.FromHours(3), now));
        // Nothing tracked stays empty rather than becoming a bare age.
        Assert.Equal("", ProgressFormat.CountsWithAge(null, null, now - TimeSpan.FromHours(3), now));
    }
}
