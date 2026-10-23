using System.Net;
using System.Text;
using System.Text.Json;
using ModelContextProtocol;
using Peers.Mcp.Reviews;
using Peers.Shared.Contracts;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Mcp.Tests;

public sealed class ReviewWorkflowTests
{
    [Fact]
    public async Task Multipart_binary_and_unicode_round_trip_exactly_and_pinned_versions_ignore_latest()
    {
        using var broker = new ReviewArtifactHandler();
        using var client = new BrokerClient(new HttpClient(broker) { BaseAddress = new Uri("http://fixture") });
        var artifacts = new ReviewArtifacts(client, "channel", "driver");
        var bytes = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("Příliš žluťoučký kůň 🧪\n", 50000)));
        var blob = await artifacts.PublishBlobAsync(bytes, "/fixture");
        Assert.True(blob.Parts.Length >= 3);
        Assert.Equal(bytes, await artifacts.ReadBlobAsync(blob));
        var binary = Enumerable.Range(0, 700000).Select(i => (byte)(i % 256)).ToArray();
        var binaryBlob = await artifacts.PublishBlobAsync(binary, "/fixture");
        Assert.Equal(binary, await artifacts.ReadBlobAsync(binaryBlob));
        var first = broker.Artifacts[blob.Parts[0].ArtifactId];
        await client.PublishArtifactAsync(new PublishArtifactRequest { Namespace = first.Namespace, Name = first.Name,
            Content = "a different latest version", Type = "other", PublishedByPeerId = "someone" });
        Assert.Equal(bytes, await artifacts.ReadBlobAsync(blob));
    }

    [Fact]
    public async Task Missing_reordered_and_corrupt_parts_cannot_materialize_as_valid_evidence()
    {
        using var broker = new ReviewArtifactHandler();
        using var client = new BrokerClient(new HttpClient(broker) { BaseAddress = new Uri("http://fixture") });
        var artifacts = new ReviewArtifacts(client, "channel", "driver");
        var bytes = new byte[ReviewArtifacts.PartBytes * 2];
        Array.Fill(bytes, (byte)1, 0, ReviewArtifacts.PartBytes);
        Array.Fill(bytes, (byte)2, ReviewArtifacts.PartBytes, ReviewArtifacts.PartBytes);
        var blob = await artifacts.PublishBlobAsync(bytes, "/fixture");
        await Assert.ThrowsAsync<McpException>(() => artifacts.ReadBlobAsync(blob with { Parts = blob.Parts.Reverse().ToArray() }));
        var first = broker.Artifacts[blob.Parts[0].ArtifactId];
        broker.Artifacts[first.ArtifactId] = first with { Content = "corrupt", ContentBytes = 7 };
        await Assert.ThrowsAsync<McpException>(() => artifacts.ReadBlobAsync(blob));
        broker.Artifacts.Remove(first.ArtifactId);
        await Assert.ThrowsAsync<McpException>(() => artifacts.ReadBlobAsync(blob));
    }

    [Fact]
    public async Task Valid_UTF8_metadata_cannot_hide_different_decoded_bytes()
    {
        using var broker = new ReviewArtifactHandler();
        using var client = new BrokerClient(new HttpClient(broker) { BaseAddress = new Uri("http://fixture") });
        var artifacts = new ReviewArtifacts(client, "channel", "driver");
        var blob = await artifacts.PublishBlobAsync([0, 255, 128, 42], "/fixture");
        var artifact = broker.Artifacts[blob.Parts[0].ArtifactId];
        var content = Convert.ToBase64String(new byte[] { 0, 254, 128, 42 });
        broker.Artifacts[artifact.ArtifactId] = artifact with { Content = content, ContentHash = ReviewJson.Hash(content), ContentBytes = Encoding.UTF8.GetByteCount(content) };
        await Assert.ThrowsAsync<McpException>(() => artifacts.ReadBlobAsync(blob with { Parts = [blob.Parts[0] with { ContentHash = ReviewJson.Hash(content) }] }));
    }
}

internal sealed class ReviewArtifactHandler : HttpMessageHandler
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true };
    internal Dictionary<string, Artifact> Artifacts { get; } = new(StringComparer.Ordinal);
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        if (request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath == "/artifacts")
        {
            var publication = JsonSerializer.Deserialize<PublishArtifactRequest>(await request.Content!.ReadAsStringAsync(ct), Json)!;
            var prior = Artifacts.Values.Where(x => x.Namespace == publication.Namespace && x.Name == publication.Name).OrderByDescending(x => x.Version).FirstOrDefault();
            if (prior?.Content == publication.Content) return Response(new PublishArtifactResponse { ArtifactId = prior.ArtifactId, Version = prior.Version, Deduplicated = true });
            var bytes = Encoding.UTF8.GetBytes(publication.Content);
            var artifact = new Artifact { ArtifactId = Guid.NewGuid().ToString("N"), Namespace = publication.Namespace, Name = publication.Name,
                Version = (prior?.Version ?? 0) + 1, Type = publication.Type, Content = publication.Content, ContentHash = ReviewJson.Hash(bytes),
                ContentBytes = bytes.Length, PublishedByPeerId = publication.PublishedByPeerId, RepoRoot = publication.RepoRoot, CreatedAt = DateTime.UtcNow };
            Artifacts.Add(artifact.ArtifactId, artifact);
            return Response(new PublishArtifactResponse { ArtifactId = artifact.ArtifactId, Version = artifact.Version });
        }
        var id = request.RequestUri!.AbsolutePath.Split('/').Last();
        return Artifacts.TryGetValue(id, out var result) ? Response(result) : new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new StringContent("Missing artifact") };
    }
    private static HttpResponseMessage Response<T>(T value) => new(HttpStatusCode.OK) { Content = new StringContent(JsonSerializer.Serialize(value, Json), Encoding.UTF8, "application/json") };
}
