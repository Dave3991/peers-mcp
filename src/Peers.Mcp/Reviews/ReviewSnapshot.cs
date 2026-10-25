using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ModelContextProtocol;

namespace Peers.Mcp.Reviews;

internal sealed record ReviewSourceFile(string Path, string Kind, bool Executable, byte[] Bytes, string Sha256);
internal sealed record ReviewSourceSnapshot(string SourceRoot, string GitDir, string BaseSha, string HeadSha,
    string RevisionSha256, byte[] TrackedDiff, IReadOnlyList<ReviewSourceFile> Untracked);

internal static class ReviewSnapshot
{
    /// <summary>
    /// Capture limits for untracked files (symlink target text counts too). Every captured byte is
    /// uploaded into the broker database, which never prunes artifacts, and each status/sync capture
    /// reads them all again. Untracked review input is new source, so 1 MiB per file and 4 MiB in total
    /// leave room for real work while a data dump or a stray secrets file fails loudly instead.
    /// </summary>
    internal const long MaxUntrackedFileBytes = 1024 * 1024;
    internal const long MaxUntrackedTotalBytes = 4 * 1024 * 1024;

    private static void RequireCapturable(string path, long length, long capturedSoFar)
    {
        const string Remedy = "Remove it or exclude it (.gitignore or .git/info/exclude); captured untracked bytes stay in the broker database permanently.";
        if (length > MaxUntrackedFileBytes)
            throw new McpException($"Untracked file '{path}' is {length} bytes, over the review capture limit of {MaxUntrackedFileBytes} bytes (1 MiB) per untracked file. {Remedy}");
        if (capturedSoFar + length > MaxUntrackedTotalBytes)
            throw new McpException($"Untracked file '{path}' brings the captured untracked total to {capturedSoFar + length} bytes, over the review capture limit of {MaxUntrackedTotalBytes} bytes (4 MiB) for all untracked files together. {Remedy}");
    }

    internal static async Task<byte[]> RunGitAsync(string root, IEnumerable<string> args, CancellationToken ct = default)
    {
        var start = new ProcessStartInfo("git")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true, RedirectStandardError = true,
        };
        start.Environment["GIT_OPTIONAL_LOCKS"] = "0";
        start.ArgumentList.Add("-C");
        start.ArgumentList.Add(root);
        foreach (var arg in args) start.ArgumentList.Add(arg);
        Process process;
        try { process = Process.Start(start) ?? throw new McpException("Cannot start Git."); }
        catch (System.ComponentModel.Win32Exception ex) { throw new McpException("Cannot start Git: " + ex.Message, ex); }
        using var ownedProcess = process;
        using var output = new MemoryStream();
        var stdout = process.StandardOutput.BaseStream.CopyToAsync(output, ct);
        var stderr = process.StandardError.ReadToEndAsync(ct);
        try
        {
            await Task.WhenAll(stdout, stderr, process.WaitForExitAsync(ct));
        }
        catch
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
            throw;
        }
        if (process.ExitCode != 0) throw new McpException($"Git failed ({process.ExitCode}): {await stderr}");
        return output.ToArray();
    }

    internal static async Task<string> GitTextAsync(string root, params string[] args) =>
        Encoding.UTF8.GetString(await RunGitAsync(root, args)).TrimEnd('\r', '\n');

    internal static async Task<ReviewSourceSnapshot> CaptureAsync(string sourceRoot, string baseSha, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(baseSha)) throw new McpException("base_sha is required.");
        sourceRoot = Path.GetFullPath(sourceRoot);
        async Task<string> Text(params string[] args) =>
            new UTF8Encoding(false, true).GetString(await RunGitAsync(sourceRoot, args, ct)).TrimEnd('\r', '\n');
        if (await Text("rev-parse", "--is-inside-work-tree") != "true")
            throw new McpException("source_root must be a Git working tree.");
        var root = Path.GetFullPath(await Text("rev-parse", "--show-toplevel"));
        var gitDir = Path.GetFullPath(await Text("rev-parse", "--absolute-git-dir"));
        var head = await Text("rev-parse", "--verify", "HEAD^{commit}");
        var baseline = await Text("rev-parse", "--verify", "--end-of-options", baseSha + "^{commit}");
        var diff = await RunGitAsync(root,
            ["-c", "diff.autoRefreshIndex=false", "diff", "--binary", "--full-index", "--no-ext-diff", "--no-textconv", baseline, "--"], ct);
        var paths = new UTF8Encoding(false, true).GetString(await RunGitAsync(root,
            ["ls-files", "--others", "--exclude-standard", "-z"], ct)).Split('\0', StringSplitOptions.RemoveEmptyEntries);
        var files = new List<ReviewSourceFile>();
        long captured = 0;
        foreach (var path in paths.Order(StringComparer.Ordinal))
        {
            ct.ThrowIfCancellationRequested();
            if (path.EndsWith('/'))
                throw new McpException($"Untracked nested repository cannot be archived as a file: {path}. Select its explicit source_root, register a submodule, or explicitly ignore it; capture never scans another repository implicitly.");
            var absolute = Path.GetFullPath(Path.Combine(root, path));
            if (!absolute.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                throw new McpException("Git returned an unsafe untracked path.");
            var info = new FileInfo(absolute);
            var target = info.LinkTarget;
            byte[] bytes;
            bool executable;
            string kind;
            if (target is not null)
            {
                kind = "symlink"; bytes = Encoding.UTF8.GetBytes(target); executable = false;
            }
            else
            {
                if (!info.Exists || (info.Attributes & (FileAttributes.Directory | FileAttributes.ReparsePoint)) != 0)
                    throw new McpException($"Untracked path is not a regular file: {path}");
                kind = "file";
                // Checked before reading, so an oversized file is never loaded into memory.
                RequireCapturable(path, info.Length, captured);
                try { bytes = await File.ReadAllBytesAsync(absolute, ct); }
                catch (UnauthorizedAccessException ex) { throw new McpException($"Cannot read untracked file: {path}", ex); }
                executable = !OperatingSystem.IsWindows() &&
                    (File.GetUnixFileMode(absolute) & (UnixFileMode.UserExecute | UnixFileMode.GroupExecute | UnixFileMode.OtherExecute)) != 0;
            }
            RequireCapturable(path, bytes.LongLength, captured); // The file may have grown since the check.
            captured += bytes.LongLength;
            files.Add(new(path, kind, executable, bytes, ReviewJson.Hash(bytes)));
        }
        var revision = ReviewJson.Bytes(new
        {
            protocol_version = 1, source_root = root, git_dir = gitDir, base_sha = baseline, head_sha = head,
            tracked_diff_sha256 = ReviewJson.Hash(diff),
            untracked = files.Select(f => new { path = f.Path, kind = f.Kind, executable = f.Executable,
                byte_length = f.Bytes.LongLength, sha256 = f.Sha256 }).ToArray(),
        });
        return new(root, gitDir, baseline, head, ReviewJson.Hash(revision), diff, files);
    }
}
