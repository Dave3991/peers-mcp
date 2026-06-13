using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Peers.Mcp;
using Peers.Mcp.Tools;
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
        if (path == "/scratchpads") return Json(HttpStatusCode.OK, "[]");
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
            EmptyClient(), DefaultState(),
            include_peers: false, include_scratchpads: false, include_artifacts: false,
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
            EmptyClient(), DefaultState(),
            include_peers: false, include_scratchpads: false, include_artifacts: false,
            task: fenceTask);

        var imported = await ContextTools.Import(exported);

        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("task", out var taskProp), $"No 'task' key in: {imported}");
        var titleProp = taskProp.GetProperty("title");
        Assert.Equal(fenceTask, titleProp.GetString());
    }

    // Test 3: Typed sections — peers/scratchpads/artifacts are JSON arrays of objects with expected keys
    [Fact]
    public async Task TypedSections_PeerScratchpadArtifactAreObjectArrays()
    {
        var handler = new ScriptedHandler(req =>
        {
            var path = req.RequestUri!.AbsolutePath;
            if (path == "/peers")
                return Json(HttpStatusCode.OK, """
                    [{"peerId":"peer-1","tool":"codex","machineId":"m1","repoRoot":"/repo","branch":"main","summary":"working on feature","leaseExpiresAt":"2099-01-01T00:00:00Z","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"}]
                    """);
            if (path == "/scratchpads")
                return Json(HttpStatusCode.OK, """
                    [{"scratchpadId":"sp-1","type":"review","title":"My Review","status":"open","createdByPeerId":"peer-1","createdAt":"2026-01-01T00:00:00Z"}]
                    """);
            if (path == "/artifacts")
                return Json(HttpStatusCode.OK, """
                    [{"artifactId":"art-1","namespace":"myns","name":"my-artifact","version":1,"type":"summary","publishedByPeerId":"peer-1","contentBytes":10,"createdAt":"2026-01-01T00:00:00Z"}]
                    """);
            return Json(HttpStatusCode.OK, "[]");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        var exported = await ContextTools.Export(
            client, DefaultState(),
            include_peers: true, include_scratchpads: true, include_artifacts: true);

        var imported = await ContextTools.Import(exported);

        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        // peers is array of objects with peer_id
        Assert.True(root.TryGetProperty("peers", out var peers));
        Assert.Equal(JsonValueKind.Array, peers.ValueKind);
        Assert.Equal(1, peers.GetArrayLength());
        Assert.Equal("peer-1", peers[0].GetProperty("peer_id").GetString());

        // scratchpads is array of objects with id
        Assert.True(root.TryGetProperty("scratchpads", out var scratchpads));
        Assert.Equal(JsonValueKind.Array, scratchpads.ValueKind);
        Assert.Equal(1, scratchpads.GetArrayLength());
        Assert.Equal("sp-1", scratchpads[0].GetProperty("id").GetString());

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
            EmptyClient(), DefaultState(),
            include_peers: false, include_scratchpads: false, include_artifacts: false,
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
            EmptyClient(), DefaultState(),
            include_peers: false, include_scratchpads: false, include_artifacts: false,
            notes: notesInput);

        var imported = await ContextTools.Import(exported);

        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("notes", out var notes));
        Assert.Equal(JsonValueKind.Array, notes.ValueKind);
        Assert.Equal(1, notes.GetArrayLength());
        Assert.Equal("not json", notes[0].GetString());
    }

    // Test 7: Newest entries — when include_scratchpad_entries=true, returns newest 3 by created_at
    [Fact]
    public async Task NewestEntries_ReturnsNewest3NotOldest()
    {
        // 5 entries with distinct created_at; entries returned in ASC order (as the server does)
        const string scratchpadId = "sp-abc";
        var handler = new ScriptedHandler(req =>
        {
            var path = req.RequestUri!.AbsolutePath;
            if (path == "/peers") return Json(HttpStatusCode.OK, "[]");
            if (path == "/artifacts") return Json(HttpStatusCode.OK, "[]");
            if (path == "/scratchpads")
                return Json(HttpStatusCode.OK, $$"""
                    [{"scratchpadId":"{{scratchpadId}}","type":"discussion","title":"Chat","status":"open","createdByPeerId":"p","createdAt":"2026-01-01T00:00:00Z"}]
                    """);
            if (path == $"/scratchpads/{scratchpadId}")
                return Json(HttpStatusCode.OK, $$"""
                    {
                      "scratchpad": {"scratchpadId":"{{scratchpadId}}","type":"discussion","title":"Chat","status":"open","createdByPeerId":"p","createdAt":"2026-01-01T00:00:00Z"},
                      "entries": [
                        {"entryId":"e1","scratchpadId":"{{scratchpadId}}","authorPeerId":"p","content":"oldest entry","contentBytes":12,"createdAt":"2026-01-01T01:00:00Z"},
                        {"entryId":"e2","scratchpadId":"{{scratchpadId}}","authorPeerId":"p","content":"second entry","contentBytes":12,"createdAt":"2026-01-01T02:00:00Z"},
                        {"entryId":"e3","scratchpadId":"{{scratchpadId}}","authorPeerId":"p","content":"third entry","contentBytes":11,"createdAt":"2026-01-01T03:00:00Z"},
                        {"entryId":"e4","scratchpadId":"{{scratchpadId}}","authorPeerId":"p","content":"fourth entry","contentBytes":12,"createdAt":"2026-01-01T04:00:00Z"},
                        {"entryId":"e5","scratchpadId":"{{scratchpadId}}","authorPeerId":"p","content":"newest entry","contentBytes":12,"createdAt":"2026-01-01T05:00:00Z"}
                      ]
                    }
                    """);
            return Json(HttpStatusCode.OK, "[]");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        var exported = await ContextTools.Export(
            client, DefaultState(),
            include_peers: false,
            include_scratchpads: true,
            include_artifacts: false,
            include_scratchpad_entries: true);

        var imported = await ContextTools.Import(exported);

        using var doc = JsonDocument.Parse(imported);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("scratchpads", out var scratchpads));
        Assert.Equal(1, scratchpads.GetArrayLength());
        var pad = scratchpads[0];

        Assert.True(pad.TryGetProperty("entries", out var entries), $"No entries in: {imported}");
        Assert.Equal(JsonValueKind.Array, entries.ValueKind);
        Assert.Equal(3, entries.GetArrayLength());

        // Entries should be newest first: e5, e4, e3
        var previews = entries.EnumerateArray().Select(e => e.GetProperty("preview").GetString()).ToArray();
        Assert.Contains("newest entry", previews);
        Assert.Contains("fourth entry", previews);
        Assert.Contains("third entry", previews);
        // Oldest entries (e1, e2) should NOT be present
        Assert.DoesNotContain("oldest entry", previews);
        Assert.DoesNotContain("second entry", previews);

        // All previews are <= 500 chars
        foreach (var preview in previews)
            Assert.True(preview!.Length <= 500);
    }
}
