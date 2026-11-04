using ModelContextProtocol;
using Peers.Mcp.Reviews;
using Xunit;

namespace Peers.Mcp.Tests;

/// <summary>
/// Plan and evidence inputs never follow symlinks, except the macOS system links /tmp, /var and /etc
/// to /private, which every temp file sits behind (mktemp, $TMPDIR). Rejecting those made agents
/// relocate every plan file; a symlink anywhere else must still fail.
/// </summary>
public sealed class ReviewInputPathTests : IDisposable
{
    // Under $TMPDIR: /var/folders/... on macOS, itself behind the /var link.
    private readonly string _dir = Directory.CreateTempSubdirectory("peers-input-").FullName;

    [Fact]
    public void Files_under_tmp_and_TMPDIR_are_accepted()
    {
        var underTmpDir = Path.Combine(_dir, "plan.md");
        File.WriteAllText(underTmpDir, "plan");
        using (var input = ReviewWorkflow.ExplicitFile("request.plan_path", underTmpDir))
            Assert.Equal(4, input.Length);

        var underTmp = $"/tmp/peers-input-{Guid.NewGuid():N}.log";
        File.WriteAllText(underTmp, "evidence");
        try
        {
            using var input = ReviewWorkflow.ExplicitFile("request.evidence[0].path", underTmp);
            Assert.Equal(8, input.Length);
        }
        finally { File.Delete(underTmp); }
    }

    [Fact]
    public void A_symlinked_directory_inside_the_path_is_rejected()
    {
        var real = Directory.CreateDirectory(Path.Combine(_dir, "real")).FullName;
        File.WriteAllText(Path.Combine(real, "plan.md"), "plan");
        var link = Path.Combine(_dir, "link");
        Directory.CreateSymbolicLink(link, real);

        var error = Assert.Throws<McpException>(() => ReviewWorkflow.ExplicitFile("request.plan_path", Path.Combine(link, "plan.md")));

        Assert.Contains("request.plan_path", error.Message);
        Assert.Contains("link\" is a symlink to", error.Message);
    }

    [Fact]
    public void A_symlinked_file_is_rejected()
    {
        var real = Path.Combine(_dir, "real.log");
        File.WriteAllText(real, "evidence");
        var link = Path.Combine(_dir, "evidence.log");
        File.CreateSymbolicLink(link, real);

        var error = Assert.Throws<McpException>(() => ReviewWorkflow.ExplicitFile("request.evidence[0].path", link));

        Assert.Contains("request.evidence[0].path", error.Message);
        Assert.Contains("a symlink to", error.Message);
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);
}
