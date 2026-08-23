using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Peers.Mcp;
using Peers.Mcp.Tools;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Peers.Mcp.Tests;

public class ContextBridgeTests
{
    private sealed class ScriptedHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _handler;
        public readonly List<(string Method, string Path, string Body)> Requests = new();

        public ScriptedHandler(Func<HttpRequestMessage, HttpResponseMessage> handler)
        {
            _handler = handler;
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var body = request.Content is not null
                ? await request.Content.ReadAsStringAsync(cancellationToken)
                : string.Empty;
            lock (Requests)
            {
                Requests.Add((request.Method.Method, request.RequestUri!.AbsolutePath, body));
            }
            return _handler(request);
        }
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string json)
    {
        return new HttpResponseMessage(status)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
    }

    // Handler that returns empty lists for all list endpoints
    private static ScriptedHandler EmptyHandler() => new(req =>
    {
        var path = req.RequestUri!.AbsolutePath;
        if (path == "/peers") return Json(HttpStatusCode.OK, "[]");
        if (path == "/channels") return Json(HttpStatusCode.OK, """{"channels":[]}""");
        if (path == "/artifacts") return Json(HttpStatusCode.OK, "[]");
        return Json(HttpStatusCode.OK, "[]");
    });

    private static BrokerClient EmptyClient() =>
        new(new HttpClient(EmptyHandler()) { BaseAddress = new Uri("http://localhost") });

    private static SessionState DefaultState() =>
        new() { PeerId = "p", Tool = "claude-code" };

    // Test 1: Escaping round-trip — quotes, newlines, colons in task survive export→import intact
    [Fact]
    public async Task EscapingRoundTrip_QuotesNewlinesColons()
    {
        var weirdTask = "Weird: \"quoted\"\nand: colon";

        var exported = await ContextTools.Export(
            EmptyClient(), DefaultState(), null!, NullLoggerFactory.Instance,
            include_peers: false, include_channels: false, include_artifacts: false,
            task: weirdTask);

        var imported = await ContextTools.Import(exported);

        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("task", out var taskProp), $"No 'task' key in: {imported}");
        var titleProp = taskProp.GetProperty("title");
        Assert.Equal(weirdTask, titleProp.GetString());
    }

    // Test 2: Code-fence round-trip — a ```code``` fence inside a value survives export→import
    [Fact]
    public async Task CodeFenceRoundTrip_FenceInTaskTitle()
    {
        var fenceTask = "Here is ```code``` in title";

        var exported = await ContextTools.Export(
            EmptyClient(), DefaultState(), null!, NullLoggerFactory.Instance,
            include_peers: false, include_channels: false, include_artifacts: false,
            task: fenceTask);

        var imported = await ContextTools.Import(exported);

        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("task", out var taskProp), $"No 'task' key in: {imported}");
        var titleProp = taskProp.GetProperty("title");
        Assert.Equal(fenceTask, titleProp.GetString());
    }

    // Test 3: Typed sections — peers/channels/artifacts are JSON arrays of objects with expected keys
    [Fact]
    public async Task TypedSections_PeerChannelArtifactAreObjectArrays()
    {
        var handler = new ScriptedHandler(req =>
        {
            var path = req.RequestUri!.AbsolutePath;
            if (path == "/peers")
                return Json(HttpStatusCode.OK, """
                    [{"peerId":"peer-1","tool":"codex","machineId":"m1","repoRoot":"/repo","branch":"main","summary":"working on feature","leaseExpiresAt":"2099-01-01T00:00:00Z","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"}]
                    """);
            if (path == "/channels")
                return Json(HttpStatusCode.OK, """
                    {"channels":[{"channelId":"ch-1","name":"deploy","repoRoot":"/repo","createdByPeerId":"peer-1","createdAt":"2026-01-01T00:00:00Z","messageCount":3}]}
                    """);
            if (path == "/artifacts")
                return Json(HttpStatusCode.OK, """
                    [{"artifactId":"art-1","namespace":"myns","name":"my-artifact","version":1,"type":"summary","publishedByPeerId":"peer-1","contentBytes":10,"createdAt":"2026-01-01T00:00:00Z"}]
                    """);
            return Json(HttpStatusCode.OK, "[]");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        var exported = await ContextTools.Export(
            client, DefaultState(), null!, NullLoggerFactory.Instance,
            include_peers: true, include_channels: true, include_artifacts: true);

        var imported = await ContextTools.Import(exported);

        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        // peers is array of objects with peer_id
        Assert.True(root.TryGetProperty("peers", out var peers));
        Assert.Equal(JsonValueKind.Array, peers.ValueKind);
        Assert.Equal(1, peers.GetArrayLength());
        Assert.Equal("peer-1", peers[0].GetProperty("peer_id").GetString());

        // channels is array of objects with the id/name/repo_root/message_count shape
        Assert.True(root.TryGetProperty("channels", out var channels));
        Assert.Equal(JsonValueKind.Array, channels.ValueKind);
        Assert.Equal(1, channels.GetArrayLength());
        Assert.Equal("ch-1", channels[0].GetProperty("id").GetString());
        Assert.Equal("deploy", channels[0].GetProperty("name").GetString());
        Assert.Equal("/repo", channels[0].GetProperty("repo_root").GetString());
        Assert.Equal(3, channels[0].GetProperty("message_count").GetInt64());

        // artifacts is array of objects with id
        Assert.True(root.TryGetProperty("artifacts", out var artifacts));
        Assert.Equal(JsonValueKind.Array, artifacts.ValueKind);
        Assert.Equal(1, artifacts.GetArrayLength());
        Assert.Equal("art-1", artifacts[0].GetProperty("id").GetString());
    }

    // Test 4: Old-format back-compat — legacy yaml packet imports without throwing
    [Fact]
    public async Task LegacyYamlPacket_ImportsSuccessfully()
    {
        var legacyPacket = "<!-- peers-context:v1 -->\n```yaml\ntask:\n  title: \"Legacy\"\npeers:\n  - peer_id: abc\n```";

        var imported = await ContextTools.Import(legacyPacket);

        // Must not throw; should contain task
        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("task", out var taskProp));
        Assert.Equal("Legacy", taskProp.GetString());
    }

    // Test 5: No block — returns raw + warning
    [Fact]
    public async Task NoBlock_ReturnsRawAndWarning()
    {
        var result = await ContextTools.Import("just some markdown");

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("raw", out _), $"Expected 'raw' key in: {result}");
        Assert.True(root.TryGetProperty("warning", out _), $"Expected 'warning' key in: {result}");
    }

    // Test 6a: notes — valid JSON array round-trips identically
    [Fact]
    public async Task Notes_ValidJsonArrayRoundTrips()
    {
        var notesInput = """["a \"quoted\" note", "line1\nline2"]""";

        var exported = await ContextTools.Export(
            EmptyClient(), DefaultState(), null!, NullLoggerFactory.Instance,
            include_peers: false, include_channels: false, include_artifacts: false,
            notes: notesInput);

        var imported = await ContextTools.Import(exported);

        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("notes", out var notes));
        Assert.Equal(JsonValueKind.Array, notes.ValueKind);
        Assert.Equal(2, notes.GetArrayLength());
        Assert.Equal("a \"quoted\" note", notes[0].GetString());
        Assert.Equal("line1\nline2", notes[1].GetString());
    }

    // Test 6b: notes — invalid JSON doesn't throw; single-element fallback
    [Fact]
    public async Task Notes_InvalidJsonFallsBackToSingleElement()
    {
        var notesInput = "not json";

        // Must not throw
        var exported = await ContextTools.Export(
            EmptyClient(), DefaultState(), null!, NullLoggerFactory.Instance,
            include_peers: false, include_channels: false, include_artifacts: false,
            notes: notesInput);

        var imported = await ContextTools.Import(exported);

        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("notes", out var notes));
        Assert.Equal(JsonValueKind.Array, notes.ValueKind);
        Assert.Equal(1, notes.GetArrayLength());
        Assert.Equal("not json", notes[0].GetString());
    }

}
