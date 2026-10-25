using System.Text;
using ModelContextProtocol;
using Peers.Mcp.Reviews;
using Xunit;

namespace Peers.Mcp.Tests;

public sealed class ReviewSnapshotTests
{
    [Fact]
    public async Task Captures_dirty_staged_binary_and_symlink_bytes_without_refreshing_index()
    {
        using var fixture = new ReviewGitFixture();
        await fixture.InitializeAsync();
        var baseline = await ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD");
        await File.WriteAllTextAsync(Path.Combine(fixture.Root, "value.txt"), "staged\n");
        await fixture.Git("add", "value.txt");
        await File.WriteAllTextAsync(Path.Combine(fixture.Root, "value.txt"), "working\n");
        var binary = new byte[] { 0, 255, 1, 128, 0, 42 };
        await File.WriteAllBytesAsync(Path.Combine(fixture.Root, "binary.bin"), binary);
        File.CreateSymbolicLink(Path.Combine(fixture.Root, "link"), "outside-target");
        var index = Path.Combine(fixture.Root, ".git", "index");
        var indexBefore = await File.ReadAllBytesAsync(index);
        var snapshot = await ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD");
        Assert.NotEqual(baseline.RevisionSha256, snapshot.RevisionSha256);
        Assert.Contains("+working", Encoding.UTF8.GetString(snapshot.TrackedDiff));
        Assert.Equal(binary, snapshot.Untracked.Single(f => f.Path == "binary.bin").Bytes);
        var link = snapshot.Untracked.Single(f => f.Path == "link");
        Assert.Equal("symlink", link.Kind);
        Assert.Equal("outside-target", Encoding.UTF8.GetString(link.Bytes));
        Assert.Equal(indexBefore, await File.ReadAllBytesAsync(index));
        Assert.False(File.Exists(index + ".lock"));
        File.SetLastWriteTimeUtc(Path.Combine(fixture.Root, "stable.txt"), DateTime.UtcNow.AddMinutes(1));
        Assert.Equal(snapshot.RevisionSha256, (await ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD")).RevisionSha256);
        Assert.Equal(indexBefore, await File.ReadAllBytesAsync(index));
        await File.WriteAllBytesAsync(Path.Combine(fixture.Root, "binary.bin"), [0, 254]);
        Assert.NotEqual(snapshot.RevisionSha256, (await ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD")).RevisionSha256);
    }

    [Fact]
    public async Task Ignores_ignored_files_and_rejects_non_repository()
    {
        using var fixture = new ReviewGitFixture();
        await Assert.ThrowsAsync<McpException>(() => ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD"));
        await fixture.InitializeAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Root, ".gitignore"), "ignored\n");
        var before = await ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD");
        await File.WriteAllTextAsync(Path.Combine(fixture.Root, "ignored"), "not evidence");
        Assert.Equal(before.RevisionSha256, (await ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD")).RevisionSha256);
    }

    [Fact]
    public async Task Store_serializes_role_mutations_and_rejects_corruption()
    {
        using var fixture = new ReviewGitFixture();
        var options = new ReviewStorageOptions(Path.Combine(fixture.Root, "state"));
        var driver = new ReviewStore(options, "channel", "driver");
        Assert.False(Directory.Exists(driver.DirectoryPath));
        using (driver.Acquire())
        {
            driver.Save(new ReviewLedger { ChannelId = "channel", State = "approved" });
            var other = new ReviewStore(options, "channel", "driver");
            Assert.Equal("Review operation already in progress; retry after it settles.", Assert.Throws<McpException>(() => other.Acquire()).Message);
            var observer = new ReviewStore(options, "channel", "observer-" + ReviewStore.Hash("other"));
            using (observer.Acquire()) observer.Save(new ReviewPeerState());
        }
        using (driver.Acquire()) Assert.Equal("approved", driver.Load<ReviewLedger>()!.State);
        var bytes = Encoding.UTF8.GetBytes("Čeština 🧪");
        var path = await driver.PutBlobAsync(bytes);
        Assert.Equal(bytes, await File.ReadAllBytesAsync(path));
        await File.WriteAllTextAsync(Path.Combine(driver.DirectoryPath, "state.json"), "broken");
        using (driver.Acquire()) Assert.Throws<McpException>(() => driver.Load<ReviewLedger>());
    }

    [Fact]
    public async Task Nested_repository_is_explicitly_rejected_instead_of_silently_shortened()
    {
        using var fixture = new ReviewGitFixture();
        await fixture.InitializeAsync();
        var nested = Path.Combine(fixture.Root, "nested");
        Directory.CreateDirectory(nested);
        await ReviewSnapshot.RunGitAsync(nested, ["init", "-q"]);
        await File.WriteAllTextAsync(Path.Combine(nested, "source.txt"), "nested source");
        await Assert.ThrowsAsync<McpException>(() => ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD"));
    }

    // Captured untracked bytes stay in the broker database for good, so a file over the per-file
    // limit fails the capture by name instead of being uploaded. Exactly at the limit still passes.
    [Fact]
    public async Task Rejects_an_untracked_file_over_the_per_file_limit_and_names_it()
    {
        using var fixture = new ReviewGitFixture();
        await fixture.InitializeAsync();
        await File.WriteAllBytesAsync(Path.Combine(fixture.Root, "at-limit.bin"), new byte[ReviewSnapshot.MaxUntrackedFileBytes]);
        Assert.Equal(ReviewSnapshot.MaxUntrackedFileBytes,
            Assert.Single((await ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD")).Untracked).Bytes.LongLength);

        await File.WriteAllBytesAsync(Path.Combine(fixture.Root, "data.bin"), new byte[ReviewSnapshot.MaxUntrackedFileBytes + 1]);
        var error = await Assert.ThrowsAsync<McpException>(() => ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD"));
        Assert.Contains($"Untracked file 'data.bin' is {ReviewSnapshot.MaxUntrackedFileBytes + 1} bytes", error.Message);
        Assert.Contains($"limit of {ReviewSnapshot.MaxUntrackedFileBytes} bytes (1 MiB) per untracked file", error.Message);
    }

    // Many files under the per-file limit can still add up; the file that crosses the total is named.
    [Fact]
    public async Task Rejects_untracked_files_over_the_total_limit_and_names_the_file_that_crosses_it()
    {
        using var fixture = new ReviewGitFixture();
        await fixture.InitializeAsync();
        var files = (int)(ReviewSnapshot.MaxUntrackedTotalBytes / ReviewSnapshot.MaxUntrackedFileBytes);
        for (var i = 0; i < files; i++)
            await File.WriteAllBytesAsync(Path.Combine(fixture.Root, $"part-{i}.bin"), new byte[ReviewSnapshot.MaxUntrackedFileBytes]);
        Assert.Equal(ReviewSnapshot.MaxUntrackedTotalBytes,
            (await ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD")).Untracked.Sum(f => f.Bytes.LongLength));

        await File.WriteAllTextAsync(Path.Combine(fixture.Root, "zz-last.txt"), "x");
        var error = await Assert.ThrowsAsync<McpException>(() => ReviewSnapshot.CaptureAsync(fixture.Root, "HEAD"));
        Assert.Contains($"Untracked file 'zz-last.txt' brings the captured untracked total to {ReviewSnapshot.MaxUntrackedTotalBytes + 1} bytes", error.Message);
        Assert.Contains($"limit of {ReviewSnapshot.MaxUntrackedTotalBytes} bytes (4 MiB) for all untracked files together", error.Message);
    }

    [Fact]
    public async Task Store_rejects_digest_generation_and_symlink_conflicts_with_private_modes()
    {
        using var fixture = new ReviewGitFixture();
        var store = new ReviewStore(new ReviewStorageOptions(Path.Combine(fixture.Root, "cache")), "channel", "driver");
        using (store.Acquire())
        {
            var ledger = new ReviewLedger { ChannelId = "channel" };
            store.Save(ledger);
            Assert.Throws<McpException>(() => store.Save(new ReviewLedger { ChannelId = "different" }, restoredGeneration: store.Generation));
            Assert.Throws<McpException>(() => store.Save(ledger, restoredGeneration: store.Generation - 1));
            if (!OperatingSystem.IsWindows())
            {
                Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute, File.GetUnixFileMode(store.DirectoryPath));
                Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(Path.Combine(store.DirectoryPath, "state.json")));
            }
        }
        var statePath = Path.Combine(store.DirectoryPath, "state.json");
        var envelope = System.Text.Json.Nodes.JsonNode.Parse(await File.ReadAllTextAsync(statePath))!;
        envelope["payload_sha256"] = new string('0', 64);
        await File.WriteAllTextAsync(statePath, envelope.ToJsonString());
        using (store.Acquire()) Assert.Throws<McpException>(() => store.Load<ReviewLedger>());
        var blobs = Path.Combine(store.DirectoryPath, "blobs");
        Directory.Delete(blobs);
        Directory.CreateSymbolicLink(blobs, fixture.Root);
        await Assert.ThrowsAsync<McpException>(() => store.PutBlobAsync([1, 2, 3]));
        Assert.Throws<McpException>(() => store.Acquire());
    }
}

internal sealed class ReviewGitFixture : IDisposable
{
    internal string Root { get; } = Path.Combine(Path.GetTempPath(), "peers-review-test-" + Guid.NewGuid().ToString("N"));
    internal ReviewGitFixture() => Directory.CreateDirectory(Root);
    internal async Task InitializeAsync()
    {
        await Git("init", "-q");
        await Git("config", "user.email", "review@example.invalid");
        await Git("config", "user.name", "Review fixture");
        await File.WriteAllTextAsync(Path.Combine(Root, "value.txt"), "baseline\n");
        await File.WriteAllTextAsync(Path.Combine(Root, "stable.txt"), "unchanged\n");
        await Git("add", ".");
        await Git("commit", "-qm", "baseline");
    }
    internal Task<byte[]> Git(params string[] args) => ReviewSnapshot.RunGitAsync(Root, args);
    public void Dispose() => Directory.Delete(Root, recursive: true);
}
