using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Peers.Mcp;
using Peers.Mcp.Tools;
using Xunit;

namespace Peers.Mcp.Tests;

public class MessageToolsTests
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

    [Fact]
    public async Task Send_ReturnsRecipientsAndDeliverySummary()
    {
        const string responseBody = """
            {
                "recipients": {
                    "onlinePeerIds": ["B"],
                    "offlinePeerIds": [],
                    "unresolvedTargets": []
                }
            }
            """;

        var handler = new ScriptedHandler(req =>
        {
            if (req.Method == HttpMethod.Post && req.RequestUri!.AbsolutePath == "/messages")
                return Json(HttpStatusCode.OK, responseBody);
            return Json(HttpStatusCode.InternalServerError, "{}");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "A" };

        var json = await MessageTools.Send(client, state, targets: ["B"], content: "hello");

        // Must parse cleanly
        var doc = JsonNode.Parse(json);
        Assert.NotNull(doc);

        // recipients object present
        var recipients = doc!["recipients"];
        Assert.NotNull(recipients);

        // delivery_summary non-null and mentions "online"
        var summary = doc["delivery_summary"]?.GetValue<string>();
        Assert.NotNull(summary);
        Assert.Contains("online", summary, StringComparison.OrdinalIgnoreCase);

        // Exactly one POST /messages was made
        Assert.Single(handler.Requests, r => r.Method == "POST" && r.Path == "/messages");
    }

    [Fact]
    public async Task Send_NotRegistered_ReturnsError()
    {
        var handler = new ScriptedHandler(_ => Json(HttpStatusCode.OK, "{}"));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = null };

        var json = await MessageTools.Send(client, state, targets: ["B"], content: "hello");

        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("error", out _));
    }
}
