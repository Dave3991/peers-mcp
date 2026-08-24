using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol;
using ModelContextProtocol.Server;
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

    // The wait tool's server and logger factory are SDK wiring, not behavior: every test pins
    // PeerId, so registration short-circuits inside WaitCore and both stay inert.
    private static Task<string> Wait(
        BrokerClient client, SessionState state, int? timeoutSeconds = null, string[]? ackInboxIds = null) =>
        ChannelTools.Wait(client, state, null!, NullLoggerFactory.Instance, timeoutSeconds, ackInboxIds);

    private static Task<string> WaitCore(
        BrokerClient client,
        SessionState state,
        int? timeoutSeconds,
        string[]? ackInboxIds,
        IProgress<ProgressNotificationValue>? progress,
        TimeSpan tickInterval,
        CancellationToken cancellationToken = default) =>
        ChannelTools.WaitCore(client, state, null!, NullLoggerFactory.Instance,
            timeoutSeconds, ackInboxIds, progress, tickInterval, cancellationToken);

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

    // Test 1: item arrives → items array length 1, timed_out==false, and NOTHING is acknowledged.
    // A delivery acking itself is the defect this replaced: the ack committed before the response frame,
    // so a cancelled request lost the item silently.
    [Fact]
    public async Task Wait_WithItem_DrainsAndAcksNothing()
    {
        var handler = new ScriptedHandler(req =>
        {
            // Wait endpoint
            if (req.Method == HttpMethod.Get && req.RequestUri!.AbsolutePath.Contains("/inbox/wait"))
                return Json(HttpStatusCode.OK, OneItemBody("ib1"));
            // Ack endpoint. Reading must never reach it — see the assertion below.
            return Json(HttpStatusCode.OK, "{}");
        });

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };

        var json = await Wait(client, state, 5);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // items array has 1 entry
        Assert.True(root.TryGetProperty("items", out var items));
        Assert.Equal(1, items.GetArrayLength());

        // timed_out is false
        Assert.True(root.TryGetProperty("timed_out", out var timedOut));
        Assert.False(timedOut.GetBoolean());

        // The item was delivered, so the caller now holds the receipt...
        Assert.Equal("ib1", items[0].GetProperty("inbox_id").GetString());
        // ...and until the caller echoes it back, the broker was never told to ack anything.
        Assert.DoesNotContain(handler.Requests, r => r.Path.EndsWith("/inbox/ack"));
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

        var json = await Wait(client, state, 1);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // timed_out is true
        Assert.True(root.TryGetProperty("timed_out", out var timedOut));
        Assert.True(timedOut.GetBoolean());

        // items array is empty
        Assert.True(root.TryGetProperty("items", out var items));
        Assert.Equal(0, items.GetArrayLength());

        // nothing delivered, nothing acked
        Assert.DoesNotContain(handler.Requests, r => r.Path.EndsWith("/inbox/ack"));
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

        // Must not throw — wait catches BrokerApiException and returns error JSON
        var json = await Wait(client, state, 1);

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

        var json = await Wait(client, state, 1);

        // Must stay under cap
        Assert.True(json.Length <= Limits.MaxToolResultChars,
            $"Result length {json.Length} exceeds MaxToolResultChars {Limits.MaxToolResultChars}");

        // timed_out must be present and true
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.True(root.TryGetProperty("timed_out", out var timedOut));
        Assert.True(timedOut.GetBoolean());
    }

    // ---- progress reporting (MCP notifications/progress for the peer_wait long poll) ----

    private sealed class RecordingProgress : IProgress<ProgressNotificationValue>
    {
        private readonly List<ProgressNotificationValue> _values = new();
        private int _lateReports;

        /// <summary>
        /// Set by tests that need the "progress stops when the operation does" boundary PINNED rather than
        /// sampled: once this returns true the caller has already seen the operation finish, so any further
        /// Report is a notification for a completed request. Counted instead of slept on.
        /// </summary>
        public Func<bool>? OperationFinished { get; set; }

        public void Report(ProgressNotificationValue value)
        {
            if (OperationFinished?.Invoke() == true) Interlocked.Increment(ref _lateReports);
            lock (_values) { _values.Add(value); }
        }

        public int Count { get { lock (_values) { return _values.Count; } } }

        public int LateReports => Volatile.Read(ref _lateReports);

        public List<ProgressNotificationValue> Snapshot()
        {
            lock (_values) { return new List<ProgressNotificationValue>(_values); }
        }
    }

    /// <summary>IProgress.Report has no non-throwing guarantee, and a real client transport can go away
    /// mid-wait. Counts calls so "stopped reporting after the throw" is assertable.</summary>
    private sealed class ThrowingProgress : IProgress<ProgressNotificationValue>
    {
        private int _calls;
        public int Calls => Volatile.Read(ref _calls);

        public void Report(ProgressNotificationValue value)
        {
            Interlocked.Increment(ref _calls);
            throw new InvalidOperationException("client transport is gone");
        }
    }

    /// <summary>
    /// Broker that answers only when the test says so. Replaces a fixed delay: with a delay, whether the
    /// ticker is still running when the broker answers depends on the machine, so the completion path these
    /// tests are named for was not reliably exercised.
    /// </summary>
    private sealed class GatedHandler : HttpMessageHandler
    {
        private readonly Task _gate;
        private readonly Func<HttpResponseMessage> _respond;

        public GatedHandler(Task gate, Func<HttpResponseMessage> respond)
        {
            _gate = gate;
            _respond = respond;
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            await _gate.WaitAsync(cancellationToken);
            return _respond();
        }
    }

    /// <summary>Broker call that never answers, and reports when its own token was cancelled.</summary>
    private sealed class TokenWatchingHandler : HttpMessageHandler
    {
        /// <summary>Signalled once the call is really in flight. Cancelling before this point makes
        /// HttpClient throw on the already-cancelled token without ever entering SendAsync, so Cancelled
        /// would never fire — the ticker starting first is NOT evidence that the broker call started.</summary>
        public readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Started.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
                throw new InvalidOperationException("unreachable");
            }
            // Observed on the unwind, NOT via CancellationToken.Register: Cancel() invokes registrations in
            // reverse order, so Task.Delay's own callback runs first, this method resumes, and disposing a
            // Register() registration removes our callback before Cancel() ever reaches it. Under CPU load
            // that race lost 5 runs in 6. The unwind always happens.
            catch (OperationCanceledException)
            {
                Cancelled.TrySetResult();
                throw;
            }
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string because)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition() && DateTime.UtcNow < deadline) await Task.Delay(5);
        Assert.True(condition(), because);
    }

    // Spec: the progress value MUST strictly increase with every notification. The ticker is private
    // to ChannelTools now, so cadence is observed through WaitCore: the broker stays parked on a gate
    // until five ticks have landed, so "ticks happened during the call" is a fact, not a race.
    [Fact]
    public async Task Wait_ProgressStrictlyIncreases_AndFirstTickReportsTheClampedBudget()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new GatedHandler(gate.Task, () => Json(HttpStatusCode.OK, OneItemBody("ib1")));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        var recorder = new RecordingProgress();

        var call = WaitCore(client, state, 120, ackInboxIds: null, recorder, TimeSpan.FromMilliseconds(1));
        await WaitUntilAsync(() => recorder.Count >= 5, "ticker should have emitted at least 5 notifications");
        gate.SetResult();
        await call;

        var values = recorder.Snapshot();
        for (var i = 1; i < values.Count; i++)
        {
            Assert.True(values[i].Progress > values[i - 1].Progress,
                $"progress must strictly increase, got {values[i - 1].Progress} then {values[i].Progress}");
        }

        // First tick reports one 5s interval against the clamped timeout, with a human-readable message.
        Assert.Equal(5f, values[0].Progress);
        Assert.Equal(120f, values[0].Total);
        Assert.Equal("Waiting for peer messages… 5s/120s", values[0].Message);
    }

    // Progress never runs past the clamped timeout it is reported against.
    [Fact]
    public async Task Wait_ProgressStopsAtTotal()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new GatedHandler(gate.Task, () => Json(HttpStatusCode.OK, EmptyTimedOutBody()));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        var recorder = new RecordingProgress();

        var call = WaitCore(client, state, 10, ackInboxIds: null, recorder, TimeSpan.FromMilliseconds(1));
        await WaitUntilAsync(() => recorder.Count >= 2, "both ticks inside the budget should have been reported");
        gate.SetResult();
        await call;

        Assert.Equal(new[] { 5f, 10f }, recorder.Snapshot().Select(v => v.Progress));
    }

    // Spec: progress notifications MUST stop after the operation completes.
    // The broker is gated on the tick count, so "ticks landed during the call" is a fact, not a race; and
    // the ticker is joined inside WaitCore's finally, so LateReports pins the boundary without a sleep.
    [Fact]
    public async Task Wait_StopsProgressWhenBrokerReturns()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new GatedHandler(gate.Task, () => Json(HttpStatusCode.OK, OneItemBody("ib1")));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        var recorder = new RecordingProgress();

        var call = WaitCore(client, state, 120, ackInboxIds: null, recorder, TimeSpan.FromMilliseconds(5));
        recorder.OperationFinished = () => call.IsCompleted;

        await WaitUntilAsync(() => recorder.Count >= 3, "the ticker should tick while the broker is parked");
        gate.SetResult();
        var json = await call;

        Assert.True(recorder.Count >= 3, "expected progress ticks during the broker call");
        Assert.Equal(0, recorder.LateReports);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(1, doc.RootElement.GetProperty("items").GetArrayLength());
    }

    // Same MUST, on the exception path — and the shaped BrokerError must survive the finally that awaits the ticker.
    [Fact]
    public async Task Wait_StopsProgressAndKeepsBrokerErrorWhenBrokerFails()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new GatedHandler(gate.Task, () => Json(HttpStatusCode.InternalServerError, """{"detail":"boom"}"""));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        var recorder = new RecordingProgress();

        var call = WaitCore(client, state, 120, ackInboxIds: null, recorder, TimeSpan.FromMilliseconds(5));
        recorder.OperationFinished = () => call.IsCompleted;

        await WaitUntilAsync(() => recorder.Count >= 3, "the ticker should tick while the broker is parked");
        gate.SetResult();
        var json = await call;

        Assert.True(recorder.Count >= 3, "expected progress ticks before the broker failed");
        Assert.Equal(0, recorder.LateReports);

        // Shaped BrokerError JSON, not a raw tool error leaking out of the finally.
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("error", out _), $"expected shaped broker error, got: {json}");
        Assert.Equal(500, doc.RootElement.GetProperty("status_code").GetInt32());
    }

    // The `completed = true; cts.Cancel();` in WaitCore's finally is what RELEASES the ticker before the
    // same finally awaits it. Without those two lines the await parks until the ticker's next boundary, so
    // every peer_wait returns one full tick interval late — invisible to the LateReports assertions above,
    // which only ever fire on a report that outlives the call.
    [Fact]
    public async Task Wait_ReturnsWithTheBroker_NotAtTheNextTickBoundary()
    {
        var handler = new ScriptedHandler(req => Json(HttpStatusCode.OK, OneItemBody("ib1")));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };

        // A tick interval far longer than any plausible answer, so "returned without waiting for a tick" is
        // a fact rather than a race: the scripted broker answers in milliseconds, the ticker sleeps 30s.
        var call = WaitCore(client, state, 120, ackInboxIds: null, new RecordingProgress(), TimeSpan.FromSeconds(30));

        var json = await call.WaitAsync(TimeSpan.FromSeconds(5));

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(1, doc.RootElement.GetProperty("items").GetArrayLength());
    }

    // A throwing IProgress.Report must not fault the ticker: WaitCore awaits it in a finally, where a fault
    // would displace the real result. Broker-success path.
    [Fact]
    public async Task Wait_ThrowingProgress_StillReturnsTheBrokerResult()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new GatedHandler(gate.Task, () => Json(HttpStatusCode.OK, OneItemBody("ib1")));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        var progress = new ThrowingProgress();

        var call = WaitCore(client, state, 120, ackInboxIds: null, progress, TimeSpan.FromMilliseconds(5));
        await WaitUntilAsync(() => progress.Calls >= 1, "the ticker should have attempted one report");
        gate.SetResult();
        var json = await call;

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(1, doc.RootElement.GetProperty("items").GetArrayLength());
        // Reporting stopped at the first throw instead of retrying every tick.
        Assert.Equal(1, progress.Calls);
    }

    // The half that actually bit: a faulted ticker replaced the in-flight BrokerApiException in the finally,
    // so the caller got a tool error instead of the shaped {"error":…,"status_code":…} JSON.
    [Fact]
    public async Task Wait_ThrowingProgress_StillReturnsTheShapedBrokerError()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new GatedHandler(gate.Task, () => Json(HttpStatusCode.InternalServerError, """{"detail":"boom"}"""));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        var progress = new ThrowingProgress();

        var call = WaitCore(client, state, 120, ackInboxIds: null, progress, TimeSpan.FromMilliseconds(5));
        await WaitUntilAsync(() => progress.Calls >= 1, "the ticker should have attempted one report");
        gate.SetResult();
        var json = await call;

        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("error", out _), $"expected shaped broker error, got: {json}");
        Assert.Equal(500, doc.RootElement.GetProperty("status_code").GetInt32());
        Assert.Equal(1, progress.Calls);
    }

    // ---- client cancellation (MCP notifications/cancelled for the peer_wait request) ----

    // MCP 2026-07-28: stop processing, free resources, do NOT respond. Propagating is what produces the
    // "no response" half — SDK 2.2.0 suppresses the frame for a cancelled request.
    [Fact]
    public async Task Wait_RequestCancelled_AbortsBrokerCallAndPropagates()
    {
        var handler = new TokenWatchingHandler();
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        var recorder = new RecordingProgress();
        using var cts = new CancellationTokenSource();

        var call = WaitCore(client, state, 120, ackInboxIds: null, recorder, TimeSpan.FromMilliseconds(5), cts.Token);
        recorder.OperationFinished = () => call.IsCompleted;
        // BOTH, and the handler one matters most: the ticker starts before the broker call, so tick count
        // alone can be satisfied while SendAsync has not been entered yet — cancel there and HttpClient
        // throws on the already-cancelled token without running the handler at all.
        await handler.Started.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await WaitUntilAsync(() => recorder.Count >= 2, "the ticker should be running before cancellation");

        cts.Cancel();

        // Propagates rather than composing a result for a request the client withdrew...
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);
        // ...the broker HTTP call really was aborted instead of being left to run to completion...
        await handler.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        // ...and the ticker stopped with it (joined in the finally, so no tick can outlive the call).
        Assert.Equal(0, recorder.LateReports);
    }

    // The other branch of the same guard: a broker that never answers is a 504, NOT a cancellation, and
    // must not be laundered into timed_out either.
    [Fact]
    public async Task Wait_BrokerHang_WithLiveRequestToken_IsAGatewayTimeoutNotACancellation()
    {
        var handler = new ScriptedHandler(req => throw new TaskCanceledException("simulated broker hang"));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        using var cts = new CancellationTokenSource();   // deliberately never cancelled

        var json = await WaitCore(client, state, 1, ackInboxIds: null, progress: null, TimeSpan.FromSeconds(5), cts.Token);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal(504, root.GetProperty("status_code").GetInt32());
        Assert.False(root.TryGetProperty("timed_out", out _),
            $"a broker hang is an error, not a long-poll timeout: {json}");
    }

    // The tool's JSON schema is the contract the model sees. Both the IProgress and the CancellationToken
    // parameter are SDK-bound and MUST stay invisible there; asserted against the REAL production wiring so
    // an SDK bump that starts emitting either one fails here. ack_inbox_ids is the opposite case: it is the
    // caller's receipt and MUST be visible, or the acknowledgement contract is unreachable from the model.
    [Fact]
    public void Wait_ToolSchema_ExposesDeclaredParametersOnly()
    {
        Assert.Equal(
            new[] { "ack_inbox_ids", "timeout_seconds" },
            SchemaPropertiesOf("wait").Order().ToArray());
    }

    private static string[] SchemaPropertiesOf(string toolName) =>
        ProductionTool(toolName).ProtocolTool.InputSchema.GetProperty("properties")
            .EnumerateObject().Select(p => p.Name).ToArray();

    /// <summary>The tool exactly as production registers it — descriptions included, so a test can hold
    /// the advertised contract against the behaviour.</summary>
    private static McpServerTool ProductionTool(string toolName)
    {
        var collection = new ServiceCollection();
        // Production registers logging (Program.cs); without it the SDK treats loggerFactory as a
        // declared parameter and leaks it into the schema this test pins.
        collection.AddLogging();
        collection.AddSingleton(new BrokerClient(new HttpClient { BaseAddress = new Uri("http://localhost") }));
        collection.AddSingleton(new SessionState());
        var services = collection.BuildServiceProvider();

        var options = ServerOptionsFactory.Create(services);
        return Assert.Single(options.ToolCollection!, t => t.ProtocolTool.Name == toolName);
    }

    // ---- receipt acknowledgement (the ids the caller echoes back, and nothing else) ----

    // Same receipt contract on the long-poll tool.
    [Fact]
    public async Task Wait_EchoedIds_AreAckedBeforeTheLongPoll()
    {
        var handler = new ScriptedHandler(req =>
            req.RequestUri!.AbsolutePath.EndsWith("/inbox/ack")
                ? Json(HttpStatusCode.OK, """{"ok":true}""")
                : Json(HttpStatusCode.OK, EmptyTimedOutBody()));

        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        await Wait(client, new SessionState { PeerId = "p" }, timeoutSeconds: 1, ackInboxIds: ["ib1"]);

        Assert.Collection(handler.Requests,
            ack => Assert.Equal(("POST", "/peers/p/inbox/ack"), (ack.Method, ack.Path)),
            wait => Assert.Equal(("GET", "/peers/p/inbox/wait"), (wait.Method, wait.Path)));
    }

    // No progress token (SDK binds a no-op reporter) must not perturb the payload by a single byte.
    [Fact]
    public async Task Wait_WithoutProgressToken_ReturnsUnchangedJson()
    {
        const string emptyBaseline =
            """{"cursor":0,"items":[],"truncated":false,"returned_count":0,"remaining_hint":null,"timed_out":true}""";

        var handler = new ScriptedHandler(req => Json(HttpStatusCode.OK, EmptyTimedOutBody()));
        var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        var noProgress = await Wait(client, new SessionState { PeerId = "p" }, 5);
        Assert.Equal(emptyBaseline, noProgress);

        // ...and reporting progress does not change it either.
        var withProgress = await WaitCore(
            client, new SessionState { PeerId = "p" }, 5, ackInboxIds: null, new RecordingProgress(), TimeSpan.FromMilliseconds(5));
        Assert.Equal(noProgress, withProgress);
    }

    // ---- the advertised window, and that the tool actually enforces it ----

    /// <summary>A broker that always times out, plus the wait window the tool asked it for.</summary>
    private sealed class WindowRecorder
    {
        public BrokerClient Client { get; }
        private string? _query;

        public WindowRecorder()
        {
            var handler = new ScriptedHandler(req =>
            {
                if (req.RequestUri!.AbsolutePath.EndsWith("/inbox/wait")) _query = req.RequestUri.Query;
                return Json(HttpStatusCode.OK, EmptyTimedOutBody());
            });
            Client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        }

        public int RequestedSeconds
        {
            get
            {
                var window = Regex.Match(_query ?? "", @"timeout_seconds=(\d+)");
                Assert.True(window.Success, $"no wait window reached the broker: '{_query}'");
                return int.Parse(window.Groups[1].Value);
            }
        }
    }

    // What the caller asks for is not what the broker is told: out-of-band values are clamped, and no
    // argument at all means the default window. Pinned at the broker boundary, the only place the
    // effective window is observable.
    [Theory]
    [InlineData(null, 25)]         // no argument -> the default window
    [InlineData(1, 1)]
    [InlineData(0, 1)]             // below the floor
    [InlineData(-30, 1)]
    [InlineData(25, 25)]
    [InlineData(120, 120)]
    [InlineData(121, 120)]         // above the ceiling
    [InlineData(int.MaxValue, 120)]
    public async Task Wait_TimeoutSeconds_IsClampedIntoTheAdvertisedBand(int? requested, int expected)
    {
        var recorder = new WindowRecorder();

        await Wait(recorder.Client, new SessionState { PeerId = "p" }, requested);

        Assert.Equal(expected, recorder.RequestedSeconds);
    }

    // The defect this guards was never in the wait loop: the tool ADVERTISED a 60s default and a 120s
    // park that no default-configured client could deliver, and the overrun surfaced as a transport
    // error indistinguishable from peer silence. So the advertised text is held against the behaviour,
    // and the default against the request budget clients ship with. The numbers are READ OUT of the
    // description, so drifting either half of the pair — text or clamp — fails here.
    [Fact]
    public async Task Wait_EnforcesExactlyTheWindowItAdvertises()
    {
        var tool = ProductionTool("wait");
        var advertised = Regex.Match(tool.ProtocolTool.Description ?? "", @"default (\d+), max (\d+)");
        Assert.True(advertised.Success, $"the description no longer states its window: {tool.ProtocolTool.Description}");
        var defaultSeconds = int.Parse(advertised.Groups[1].Value);
        var maxSeconds = int.Parse(advertised.Groups[2].Value);

        // The parameter description is what a model reads when it picks a value: same pair, or one of them lies.
        var parameter = tool.ProtocolTool.InputSchema
            .GetProperty("properties").GetProperty("timeout_seconds").GetProperty("description").GetString();
        Assert.Contains($"default {defaultSeconds}, max {maxSeconds}", parameter);

        // A client's request timeout is one-shot: omp resolves 30s by default and never extends it on the
        // 5s progress keepalives, so a default at or above 30 is a transport error waiting to happen.
        Assert.True(defaultSeconds < 30, $"default window {defaultSeconds}s does not fit a 30s client request budget");

        var byDefault = new WindowRecorder();
        await Wait(byDefault.Client, new SessionState { PeerId = "p" });
        Assert.Equal(defaultSeconds, byDefault.RequestedSeconds);

        var overMax = new WindowRecorder();
        await Wait(overMax.Client, new SessionState { PeerId = "p" }, maxSeconds + 1);
        Assert.Equal(maxSeconds, overMax.RequestedSeconds);
    }
}
