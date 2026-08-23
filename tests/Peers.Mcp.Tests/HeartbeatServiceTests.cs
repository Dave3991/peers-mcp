using System.Net;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Peers.Mcp;
using Peers.Mcp.Lifecycle;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.Mcp.Tests;

public class HeartbeatServiceTests
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

    // Test 1: Re-register on 404
    // Handler returns 404 for PATCH /peers/{id}/heartbeat and 200 for POST /peers.
    // After running, PeerId should become "new-id", LastCursor should be Math.Max(3,7)=7,
    // at least one POST /peers should have been observed, and the body should contain "old-id".
    [Fact]
    public async Task ReRegistersOnHeartbeat404()
    {
        var handler = new ScriptedHandler(req =>
        {
            if (req.Method == HttpMethod.Patch && req.RequestUri!.AbsolutePath.Contains("/heartbeat"))
                return Json(HttpStatusCode.NotFound, "{\"error\":\"not found\"}");
            if (req.Method == HttpMethod.Post && req.RequestUri!.AbsolutePath == "/peers")
                return Json(HttpStatusCode.OK, "{\"peerId\":\"new-id\",\"resumed\":false,\"lastCursor\":7,\"latestEventId\":9}");
            return Json(HttpStatusCode.InternalServerError, "{}");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState
        {
            PeerId = "old-id",
            LastRegisterRequest = new RegisterPeerRequest { Tool = "claude-code", MachineId = "m" }
        };
        state.ResetCursorForRegistration(3);

        var service = new HeartbeatService(client, state, NullLogger<HeartbeatService>.Instance, interval: TimeSpan.FromMilliseconds(40));
        service.Start();

        try
        {
            // Poll up to 3s until PeerId is updated
            var deadline = DateTime.UtcNow.AddSeconds(3);
            while (DateTime.UtcNow < deadline && state.PeerId != "new-id")
                await Task.Delay(30);

            Assert.Equal("new-id", state.PeerId);
            Assert.Equal(7L, state.LastCursor); // Math.Max(3, 7)

            List<(string Method, string Path, string Body)> captured;
            lock (handler.Requests) { captured = handler.Requests.ToList(); }

            var postRequests = captured.Where(r => r.Method == "POST" && r.Path == "/peers").ToList();
            Assert.True(postRequests.Count >= 1, "Expected at least one POST /peers");

            // The POST body should contain the old peer id as resumePeerId
            Assert.Contains("old-id", postRequests[0].Body);
        }
        finally
        {
            await service.DisposeAsync();
        }
    }

    // Test 2: No re-register on non-404 error
    // Handler returns 500 for heartbeat. After ~300ms, PeerId stays "old-id" and no POST /peers.
    [Fact]
    public async Task NoReRegisterOnNon404Error()
    {
        var handler = new ScriptedHandler(req =>
        {
            if (req.Method == HttpMethod.Patch && req.RequestUri!.AbsolutePath.Contains("/heartbeat"))
                return Json(HttpStatusCode.InternalServerError, "{\"error\":\"server error\"}");
            return Json(HttpStatusCode.OK, "{}");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState
        {
            PeerId = "old-id",
            LastRegisterRequest = new RegisterPeerRequest { Tool = "claude-code", MachineId = "m" }
        };

        var service = new HeartbeatService(client, state, NullLogger<HeartbeatService>.Instance, interval: TimeSpan.FromMilliseconds(40));
        service.Start();

        try
        {
            await Task.Delay(300);

            Assert.Equal("old-id", state.PeerId);

            List<(string Method, string Path, string Body)> captured;
            lock (handler.Requests) { captured = handler.Requests.ToList(); }

            var postRequests = captured.Where(r => r.Method == "POST" && r.Path == "/peers").ToList();
            Assert.Empty(postRequests);
        }
        finally
        {
            await service.DisposeAsync();
        }
    }

    // Test 3: Throttle — second re-register attempt is blocked by 1-min throttle.
    // Handler 404s PATCH /peers/old-id/heartbeat, returns 200 for POST /peers with "new-id",
    // then also 404s PATCH /peers/new-id/heartbeat. With 40ms interval and ~400ms run time,
    // the 1-minute throttle should allow only exactly one POST /peers.
    [Fact]
    public async Task ThrottlesReRegisterAttempts()
    {
        var handler = new ScriptedHandler(req =>
        {
            if (req.Method == HttpMethod.Patch && req.RequestUri!.AbsolutePath.Contains("/heartbeat"))
                return Json(HttpStatusCode.NotFound, "{\"error\":\"not found\"}");
            if (req.Method == HttpMethod.Post && req.RequestUri!.AbsolutePath == "/peers")
                return Json(HttpStatusCode.OK, "{\"peerId\":\"new-id\",\"resumed\":false,\"lastCursor\":0}");
            return Json(HttpStatusCode.InternalServerError, "{}");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState
        {
            PeerId = "old-id",
            LastRegisterRequest = new RegisterPeerRequest { Tool = "claude-code", MachineId = "m" }
        };

        var service = new HeartbeatService(client, state, NullLogger<HeartbeatService>.Instance, interval: TimeSpan.FromMilliseconds(40));
        service.Start();

        try
        {
            // Wait long enough for many heartbeat cycles to fire, triggering multiple 404s
            await Task.Delay(400);

            List<(string Method, string Path, string Body)> captured;
            lock (handler.Requests) { captured = handler.Requests.ToList(); }

            var postRequests = captured.Where(r => r.Method == "POST" && r.Path == "/peers").ToList();
            Assert.Single(postRequests);
        }
        finally
        {
            await service.DisposeAsync();
        }
    }
}
