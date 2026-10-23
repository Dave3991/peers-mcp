using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using ModelContextProtocol;

namespace Peers.Mcp.Reviews;

internal sealed record ReviewStorageOptions(string Root);

internal sealed class ReviewStore
{
    private readonly string _root;
    private FileStream? _lease;
    internal string DirectoryPath { get; }
    internal long Generation { get; private set; }
    internal string? PayloadDigest { get; private set; }
    internal string? MirrorId { get; private set; }
    private sealed record Envelope(long Generation, string PayloadSha256, JsonElement Payload, string? MirrorId);

    internal ReviewStore(ReviewStorageOptions options, string channelId, string role)
    {
        ReviewJson.Require(role == "driver" || Regex.IsMatch(role, "^(reviewer|observer)-[0-9a-f]{64}$"), "Invalid review cache role.");
        _root = Path.GetFullPath(options.Root);
        DirectoryPath = Path.Combine(_root, "review-workflows", Hash(channelId), role);
    }
    internal static string Hash(string value) => ReviewJson.Hash(value);

    private static void PrivateDirectory(string path, bool preserveExistingMode = false)
    {
        var info = new DirectoryInfo(path);
        if (info.LinkTarget is not null || (info.Exists && (info.Attributes & FileAttributes.ReparsePoint) != 0))
            throw new McpException($"Refusing symlinked review cache directory: {path}");
        Directory.CreateDirectory(path);
        if (!OperatingSystem.IsWindows() && (!info.Exists || !preserveExistingMode))
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
    }
    private void EnsureDirectories()
    {
        PrivateDirectory(_root, preserveExistingMode: true);
        PrivateDirectory(Path.Combine(_root, "review-workflows"));
        PrivateDirectory(Path.GetDirectoryName(DirectoryPath)!);
        PrivateDirectory(DirectoryPath);
        PrivateDirectory(Path.Combine(DirectoryPath, "blobs"));
    }
    private static void PrivateFile(string path)
    {
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
    }
    private static void RefuseLink(string path)
    {
        if (new FileInfo(path).LinkTarget is not null) throw new McpException("Refusing symlinked review cache file.");
    }
    internal IDisposable Acquire()
    {
        EnsureDirectories();
        var path = Path.Combine(DirectoryPath, "lock");
        RefuseLink(path);
        try { _lease = new FileStream(path, NewPrivateFile(FileMode.OpenOrCreate)); }
        catch (IOException ex) { throw new McpException("Review operation already in progress; retry after it settles.", ex); }
        PrivateFile(path);
        return new Lease(this);
    }
    private sealed class Lease(ReviewStore store) : IDisposable
    {
        public void Dispose() { store._lease?.Dispose(); store._lease = null; }
    }
    internal T? Load<T>() where T : class
    {
        ReviewJson.Require(_lease is not null, "Review state requires a mutation lease.");
        var path = Path.Combine(DirectoryPath, "state.json");
        RefuseLink(path);
        if (!File.Exists(path)) return null;
        try
        {
            var envelope = ReviewJson.Parse<Envelope>(File.ReadAllBytes(path));
            var bytes = Encoding.UTF8.GetBytes(envelope.Payload.GetRawText());
            ReviewJson.Require(envelope.Generation > 0 && ReviewJson.Hash(bytes) == envelope.PayloadSha256,
                "Review state digest is invalid; recover from verified channel evidence.");
            Generation = envelope.Generation; PayloadDigest = envelope.PayloadSha256; MirrorId = envelope.MirrorId;
            return envelope.Payload.Deserialize<T>(ReviewJson.Options) ?? throw new JsonException("Null review state.");
        }
        catch (Exception ex) when (ex is JsonException or IOException)
        {
            throw new McpException("Malformed review state; recover from verified checkpoint and channel facts.", ex);
        }
    }
    private static FileStreamOptions NewPrivateFile(FileMode mode = FileMode.CreateNew)
    {
        var options = new FileStreamOptions { Mode = mode, Access = FileAccess.ReadWrite, Share = FileShare.None };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        return options;
    }
    internal void Save<T>(T payload, string? mirrorId = null, long? restoredGeneration = null)
    {
        ReviewJson.Require(_lease is not null, "Review state requires a mutation lease.");
        var bytes = ReviewJson.Bytes(payload);
        var digest = ReviewJson.Hash(bytes);
        var next = restoredGeneration ?? checked(Generation + 1);
        ReviewJson.Require(next > Generation || (next == Generation && digest == PayloadDigest), "Conflicting review checkpoint generation/digest.");
        using var document = JsonDocument.Parse(bytes);
        var envelope = new Envelope(next, digest, document.RootElement.Clone(), mirrorId ?? MirrorId);
        var path = Path.Combine(DirectoryPath, "state.json");
        RefuseLink(path);
        var temporary = Path.Combine(DirectoryPath, "state-" + Guid.NewGuid().ToString("N") + ".tmp");
        try
        {
            using (var file = new FileStream(temporary, NewPrivateFile()))
            {
                file.Write(ReviewJson.Bytes(envelope)); file.Flush(flushToDisk: true);
            }
            File.Move(temporary, path, overwrite: true);
            PrivateFile(path);
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
        Generation = next; PayloadDigest = digest; MirrorId = envelope.MirrorId;
    }
    internal Task<string> PutBlobAsync(byte[] bytes, CancellationToken ct = default) =>
        PutBlobAsync(ReviewJson.Hash(bytes), bytes.LongLength, (stream, token) => stream.WriteAsync(bytes, token).AsTask(), ct);

    internal async Task<string> PutBlobAsync(string digest, long byteLength,
        Func<Stream, CancellationToken, Task> writeVerifiedBytes, CancellationToken ct = default)
    {
        ReviewJson.Require(Regex.IsMatch(digest, "^[0-9a-f]{64}$") && byteLength >= 0, "Invalid immutable cache descriptor.");
        EnsureDirectories();
        var path = Path.Combine(DirectoryPath, "blobs", digest);
        RefuseLink(path);
        if (File.Exists(path))
        {
            await using var existing = File.OpenRead(path);
            if (existing.Length == byteLength && Convert.ToHexStringLower(await SHA256.HashDataAsync(existing, ct)) == digest)
            {
                await writeVerifiedBytes(Stream.Null, ct); // A cache hit never substitutes for complete broker verification.
                return path;
            }
            // Broker-verified bytes can repair a corrupt local copy; remote evidence is still immutable.
        }
        var temporary = Path.Combine(DirectoryPath, "blobs", Guid.NewGuid().ToString("N") + ".tmp");
        try
        {
            await using (var stream = new FileStream(temporary, NewPrivateFile()))
            {
                await writeVerifiedBytes(stream, ct);
                ReviewJson.Require(stream.Length == byteLength, "Materialized review byte count mismatch.");
                stream.Flush(true);
            }
            RefuseLink(path);
            File.Move(temporary, path, overwrite: true);
            PrivateFile(path);
            return path;
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }
}
