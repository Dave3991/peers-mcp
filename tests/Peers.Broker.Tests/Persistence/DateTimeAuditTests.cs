using System.Text.RegularExpressions;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

/// <summary>
/// Regression gate: asserts that no C# source file under src/Peers.Broker uses
/// kindless datetime-read patterns, except for the single sanctioned site in DbTime.cs.
/// </summary>
public sealed class DateTimeAuditTests
{
    private static readonly Regex[] ForbiddenPatterns =
    [
        new Regex(@"DateTime\.Parse", RegexOptions.Compiled),
        new Regex(@"\.GetDateTime\(", RegexOptions.Compiled),
        new Regex(@"Convert\.ToDateTime", RegexOptions.Compiled),
        new Regex(@"\.ParseExact", RegexOptions.Compiled),
    ];

    [Fact]
    public void BrokerSource_HasNoKindlessDateTimeParsing_ExceptDbTime()
    {
        // Walk up from the test assembly's output directory to find the repo root.
        // The assembly lives at:  <repo>/tests/Peers.Broker.Tests/bin/Debug/net10.0/
        // So we need to go up 5 levels to reach the repo root, then into src/Peers.Broker.
        var baseDir = AppContext.BaseDirectory;
        var dir = new DirectoryInfo(baseDir);

        // Walk up until we find a directory that contains "src" and "tests" children
        // (i.e. the repo root) or until we hit the filesystem root.
        DirectoryInfo? repoRoot = null;
        for (var d = dir; d != null; d = d.Parent)
        {
            if (Directory.Exists(Path.Combine(d.FullName, "src")) &&
                Directory.Exists(Path.Combine(d.FullName, "tests")))
            {
                repoRoot = d;
                break;
            }
        }

        if (repoRoot is null)
            Assert.Fail($"Could not locate repo root by walking up from '{baseDir}'. " +
                        "Expected a directory containing both 'src' and 'tests' subdirectories.");

        var brokerSrcDir = Path.Combine(repoRoot.FullName, "src", "Peers.Broker");
        if (!Directory.Exists(brokerSrcDir))
            Assert.Fail($"Could not find broker source directory: '{brokerSrcDir}'");

        var violations = new List<string>();

        foreach (var file in Directory.EnumerateFiles(brokerSrcDir, "*.cs", SearchOption.AllDirectories))
        {
            var fileName = Path.GetFileName(file);

            // DbTime.cs is the one sanctioned parse site — skip it.
            if (string.Equals(fileName, "DbTime.cs", StringComparison.OrdinalIgnoreCase))
                continue;

            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                foreach (var pattern in ForbiddenPatterns)
                {
                    if (pattern.IsMatch(lines[i]))
                    {
                        var relativePath = Path.GetRelativePath(repoRoot.FullName, file);
                        violations.Add($"{relativePath}:{i + 1}: {lines[i].Trim()}");
                    }
                }
            }
        }

        if (violations.Count > 0)
        {
            Assert.Fail(
                $"Found {violations.Count} forbidden datetime-read pattern(s) in broker source " +
                $"(all except DbTime.cs must use r.GetUtc / r.GetUtcOrNull):\n" +
                string.Join("\n", violations));
        }
    }
}
