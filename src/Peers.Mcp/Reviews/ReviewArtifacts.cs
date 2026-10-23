using System.Security.Cryptography;
using System.Text;
using ModelContextProtocol;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Mcp.Reviews;

internal sealed class ReviewArtifacts(BrokerClient client, string channelId, string peerId)
{
    internal const int PartBytes = 524_288;
    internal const int ArtifactBytes = 1_048_576;
    internal string Namespace => "peers-review-" + ReviewJson.Hash(channelId);

    internal static void VerifyArtifact(Artifact artifact)
    {
        var bytes = Encoding.UTF8.GetBytes(artifact.Content);
        ReviewJson.Require(bytes.Length <= ArtifactBytes && artifact.ContentBytes == bytes.Length &&
            artifact.ContentHash == ReviewJson.Hash(bytes), "Review artifact hash/UTF-8 byte count mismatch.");
    }
    internal async Task<Artifact> GetAsync(string id)
    {
        Artifact artifact;
        try { artifact = await client.GetArtifactByIdAsync(id) ?? throw new McpException($"Missing review artifact: {id}"); }
        catch (BrokerApiException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        { throw new McpException($"Missing review artifact: {id}", ex); }
        ReviewJson.Require(artifact.ArtifactId == id && artifact.Namespace == Namespace, "Review artifact identity/namespace mismatch.");
        VerifyArtifact(artifact);
        return artifact;
    }
    internal async Task<Artifact> PublishAsync(string name, string type, string content, string sourceRoot)
    {
        ReviewJson.Require(Encoding.UTF8.GetByteCount(content) <= ArtifactBytes, "Review artifact exceeds the 1,048,576-byte bound; packet was not truncated.");
        var response = await client.PublishArtifactAsync(new PublishArtifactRequest
        {
            Namespace = Namespace, Name = name, Type = type, Content = content,
            RepoRoot = sourceRoot, PublishedByPeerId = peerId, NotifyPeers = false,
        });
        var artifact = await GetAsync(response.ArtifactId);
        ReviewJson.Require(artifact.Name == name && artifact.Version == response.Version && artifact.Content == content &&
            artifact.RepoRoot == sourceRoot, "Published review artifact does not match its confirmed stored version.");
        return artifact;
    }
    internal async Task<ReviewBlob> PublishBlobAsync(byte[] bytes, string sourceRoot,
        Func<ReviewPart, Task>? confirmed = null, CancellationToken ct = default)
    {
        using var stream = new MemoryStream(bytes, writable: false);
        return await PublishBlobAsync(stream, sourceRoot, confirmed, ct);
    }
    internal async Task<ReviewBlob> PublishBlobAsync(Stream stream, string sourceRoot,
        Func<ReviewPart, Task>? confirmed = null, CancellationToken ct = default)
    {
        var parts = new List<ReviewPart>();
        var buffer = new byte[PartBytes];
        using var digest = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        long total = 0;
        while (true)
        {
            var length = 0;
            while (length < buffer.Length)
            {
                var count = await stream.ReadAsync(buffer.AsMemory(length), ct);
                if (count == 0) break;
                length += count;
            }
            if (length == 0) break;
            digest.AppendData(buffer, 0, length); total = checked(total + length);
            var content = Convert.ToBase64String(buffer, 0, length);
            var hash = ReviewJson.Hash(buffer.AsSpan(0, length));
            var artifact = await PublishAsync("blob-" + hash, "other", content, sourceRoot);
            var part = new ReviewPart(artifact.ArtifactId, artifact.Version, artifact.ContentHash, length);
            parts.Add(part);
            if (confirmed is not null) await confirmed(part);
        }
        return new("base64", Convert.ToHexStringLower(digest.GetHashAndReset()), total, parts.ToArray());
    }
    internal async Task<byte[]> ReadBlobAsync(ReviewBlob blob, CancellationToken ct = default)
    {
        using var output = new MemoryStream();
        await ReadBlobToAsync(blob, output, ct);
        return output.ToArray();
    }
    internal async Task ReadBlobToAsync(ReviewBlob blob, Stream output, CancellationToken ct = default)
    {
        ReviewJson.Require(blob.Encoding == "base64" && blob.ByteLength >= 0, "Invalid review blob encoding/length.");
        ReviewJson.Require(blob.Parts is not null && blob.Parts.LongLength == blob.ByteLength / PartBytes + (blob.ByteLength % PartBytes == 0 ? 0 : 1),
            "Review blob part count/order mismatch.");
        using var digest = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        for (var index = 0; index < blob.Parts.Length; index++)
        {
            ct.ThrowIfCancellationRequested();
            var part = blob.Parts[index];
            var expected = (int)Math.Min(PartBytes, blob.ByteLength - (long)index * PartBytes);
            ReviewJson.Require(part.ByteLength == expected, "Review blob part size/order mismatch.");
            var artifact = await GetAsync(part.ArtifactId);
            ReviewJson.Require(artifact.Version == part.Version && artifact.ContentHash == part.ContentHash,
                "Review blob pinned part mismatch.");
            byte[] bytes;
            try { bytes = Convert.FromBase64String(artifact.Content); }
            catch (FormatException ex) { throw new McpException("Invalid base64 review part.", ex); }
            ReviewJson.Require(bytes.Length == expected && artifact.Content == Convert.ToBase64String(bytes) &&
                artifact.Name == "blob-" + ReviewJson.Hash(bytes), "Review blob decoded part mismatch.");
            digest.AppendData(bytes);
            await output.WriteAsync(bytes, ct);
        }
        ReviewJson.Require(Convert.ToHexStringLower(digest.GetHashAndReset()) == blob.Sha256,
            "Review blob digest mismatch; missing/reordered/corrupt evidence.");
    }
    internal async Task<ReviewMaterialization> MaterializeAsync(string packetId, ReviewStore store,
        string? expectedDigest = null, CancellationToken ct = default)
    {
        var root = await GetAsync(packetId);
        var rootBytes = Encoding.UTF8.GetBytes(root.Content);
        var digest = ReviewJson.Hash(rootBytes);
        ReviewJson.Require(root.Name == "packet-" + digest && (expectedDigest is null || digest == expectedDigest),
            "Review root manifest digest mismatch.");
        var manifest = ReviewJson.Parse<ReviewManifest>(rootBytes);
        ReviewJson.Require(manifest.ProtocolVersion == 1 && manifest.ChannelId == channelId && manifest.Round > 0 &&
            manifest.Phase is "PLAN" or "CODE" && root.RepoRoot == manifest.SourceRoot &&
            manifest.DriverPeerId != manifest.ReviewerPeerId, "Invalid review root manifest.");
        var index = ReviewJson.Parse<ReviewPayloadIndex>(await ReadBlobAsync(manifest.PayloadIndex, ct));
        ReviewJson.Require(index.ProtocolVersion == 1 && index.Files is not null, "Invalid review payload index.");
        var paths = new HashSet<string>(StringComparer.Ordinal);
        var files = new List<ReviewMaterializedFile>();
        foreach (var entry in index.Files)
        {
            ReviewJson.Require(paths.Add(entry.Path) && entry.Path.Length > 0 && !entry.Path.Contains('\0'), "Duplicate/invalid logical review path.");
            var local = await store.PutBlobAsync(entry.Blob.Sha256, entry.Blob.ByteLength,
                (stream, token) => ReadBlobToAsync(entry.Blob, stream, token), ct);
            files.Add(new(entry.Path, local, entry.Kind, entry.Evidence, entry.Blob));
        }
        ReviewJson.Require(paths.Contains("task.txt") && paths.Contains("tracked.diff") &&
            (manifest.Phase != "PLAN" || paths.Contains("plan.md")), "Review packet lacks required complete payloads.");
        var task = index.Files.Single(f => f.Path == "task.txt");
        ReviewJson.Require(task.Blob.Sha256 == manifest.TaskSha256, "Review task digest mismatch.");
        var manifestPath = await store.PutBlobAsync(rootBytes, ct);
        var map = await store.PutBlobAsync(ReviewJson.Bytes(new { manifest_path = manifestPath, files }), ct);
        return new(manifest, index, digest, manifestPath, map, files.ToArray());
    }
}
