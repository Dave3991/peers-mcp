using System.Net;
using System.Text;
using System.Text.Json;
using Peers.Mcp;
using Peers.Mcp.Tools;
using Xunit;

namespace Peers.Mcp.Tests;

public class PeerWaitTests
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

    // A single inbox item shaped like InboxItemDto (camelCase matches the BrokerClient JsonOpts)
    private static string OneItemBody(string inboxId = "ib1") => $$"""
        {
          "items": [
            {
              "inboxId": "{{inboxId}}",
              "peerId": "p",
              "event": {
                "eventId": 5,
                "type": "scratchpad.entry_added",
                "payload": "{\"scratchpad_id\":\"sp\",\"entry_id\":\"e1\",\"content\":\"hi\"}",
                "createdAt": "2026-06-12T10:00:00Z"
              },
              "createdAt": "2026-06-12T10:00:00Z"
            }
          ],
          "count": 1,
          "timedOut": false
        }
        """;

    private static string EmptyTimedOutBody() => """
        {"items":[],"count":0,"timedOut":true}
        """;

    // Test 1: item arrives → items array length 1, timed_out==false, ack ids staged
    [Fact]
    public async Task Wait_WithItem_DrainsAndStagesAck()
    {
        var handler = new ScriptedHandler(req =>
        {
            // Wait endpoint
            if (req.Method == HttpMethod.Get && req.RequestUri!.AbsolutePath.Contains("/inbox/wait"))
                return Json(HttpStatusCode.OK, OneItemBody("ib1"));
            // Ack endpoint (should not be called since PendingInboxAckIds is null at start)
            return Json(HttpStatusCode.OK, "{}");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };

        var json = await PollTools.Wait(client, state, 5);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // items array has 1 entry
        Assert.True(root.TryGetProperty("items", out var items));
        Assert.Equal(1, items.GetArrayLength());

        // timed_out is false
        Assert.True(root.TryGetProperty("timed_out", out var timedOut));
        Assert.False(timedOut.GetBoolean());

        // pending ack ids staged
        Assert.NotNull(state.PendingInboxAckIds);
        Assert.Equal(new[] { "ib1" }, state.PendingInboxAckIds);
    }

    // Test 2: timeout with no items → timed_out==true, items empty, no ack ids staged
    [Fact]
    public async Task Wait_Timeout_ReportsTimedOutAndNoAck()
    {
        var handler = new ScriptedHandler(req =>
        {
            if (req.Method == HttpMethod.Get && req.RequestUri!.AbsolutePath.Contains("/inbox/wait"))
                return Json(HttpStatusCode.OK, EmptyTimedOutBody());
            return Json(HttpStatusCode.OK, "{}");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };

        var json = await PollTools.Wait(client, state, 1);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // timed_out is true
        Assert.True(root.TryGetProperty("timed_out", out var timedOut));
        Assert.True(timedOut.GetBoolean());

        // items array is empty
        Assert.True(root.TryGetProperty("items", out var items));
        Assert.Equal(0, items.GetArrayLength());

        // no ack ids staged
        Assert.Null(state.PendingInboxAckIds);
    }

    // Test 4: broker hangs / CTS fires → TaskCanceledException must be caught and returned as error JSON
    [Fact]
    public async Task Wait_BrokerHang_ReturnsGracefulError()
    {
        var handler = new ScriptedHandler(req =>
        {
            if (req.Method == HttpMethod.Get && req.RequestUri!.AbsolutePath.Contains("/inbox/wait"))
                throw new TaskCanceledException("Simulated broker hang");
            return Json(HttpStatusCode.OK, "{}");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };

        // Must not throw — peer_wait catches BrokerApiException and returns error JSON
        var json = await PollTools.Wait(client, state, 1);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.True(root.TryGetProperty("error", out _), $"Expected 'error' field in response, got: {json}");
    }

    // Test 3: 20 large items → result stays under cap even after timed_out injection
    [Fact]
    public async Task Wait_ManyLargeItems_StaysUnderCap()
    {
        const int itemCount = 20;
        const int payloadContentLength = 50_000;

        var itemsJson = string.Join(",\n", Enumerable.Range(1, itemCount).Select(i =>
        {
            var content = new string('x', payloadContentLength);
            var payload = $"{{\\\"scratchpad_id\\\":\\\"sp\\\",\\\"entry_id\\\":\\\"e{i}\\\",\\\"content\\\":\\\"{content}\\\"}}";
            return $$"""
                {
                  "inboxId": "ib{{i}}",
                  "peerId": "p",
                  "event": {
                    "eventId": {{i}},
                    "type": "scratchpad.entry_added",
                    "payload": "{{payload}}",
                    "createdAt": "2026-06-12T10:00:00Z"
                  },
                  "createdAt": "2026-06-12T10:00:00Z"
                }
                """;
        }));

        var body = $$"""{"items":[{{itemsJson}}],"count":{{itemCount}},"timedOut":true}""";

        var handler = new ScriptedHandler(req =>
        {
            if (req.Method == HttpMethod.Get && req.RequestUri!.AbsolutePath.Contains("/inbox/wait"))
                return Json(HttpStatusCode.OK, body);
            return Json(HttpStatusCode.OK, "{}");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };

        var json = await PollTools.Wait(client, state, 1);

        // Must stay under cap
        Assert.True(json.Length <= Limits.MaxToolResultChars,
            $"Result length {json.Length} exceeds MaxToolResultChars {Limits.MaxToolResultChars}");

        // timed_out must be present and true
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.True(root.TryGetProperty("timed_out", out var timedOut));
        Assert.True(timedOut.GetBoolean());
    }
}
