using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Peers.Broker.Hosting;
using Peers.Mcp;
using Peers.Mcp.Lifecycle;
using Peers.Mcp.Resources;
using Peers.Mcp.Tools;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.IntegrationTests;

/// <summary>
/// The production wiring end to end: a real broker (in-process), the REAL McpServerOptions the
/// production entry point builds, and a real MCP client over a stream-pair transport. SEP-2575
/// subscriptions are the bulk of it; the tools/list and inbox-acknowledgement tests share the same
/// harness precisely because "real wiring" is the property they all assert.
///
/// The fast tests drive <see cref="SubscriptionRegistry.NotifyEventsAsync"/> with events polled from
/// the real broker — the same call ChannelNotifier's loop body makes, minus its 5s timer, so they
/// stay deterministic. <see cref="ChannelNotifier_DeliversOwnUndirectedEvent_ThroughTheLivePollLoop"/>
/// covers the timer path itself.
/// </summary>
public sealed class McpSubscriptionTests : IAsyncLifetime
{
    private WebApplication _app = null!;
    private string _dbPath = null!;
    private ServiceProvider _services = null!;
    private SessionState _state = null!;
    private BrokerClient _broker = null!;
    private McpServer _server = null!;
    private StreamServerTransport _transport = null!;
    private Pipe _clientToServer = null!;
    private Task _serverTask = null!;
    private McpClient _client = null!;
    private RecordingStream _tap = null!;
    private IAsyncDisposable[]? _handlers;

    private readonly ConcurrentQueue<(string Method, JsonNode? Params)> _notifications = new();

    /// <summary>
    /// Every JSON-RPC frame the server writes, raw. The client API cannot answer "did a response go out
    /// for a request the client already cancelled?" — a cancelled request is off its pending map, so an
    /// arriving response is dropped silently. The wire can answer it.
    /// </summary>
    private readonly ConcurrentQueue<string> _wire = new();
    private string? _peerId;
    private long _testCursor;

    /// <summary>How long "nothing arrives" is given to be wrong.</summary>
    private static readonly TimeSpan Settle = TimeSpan.FromSeconds(1.5);

    public async Task InitializeAsync()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"peers_sub_{Guid.NewGuid():N}.db");

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        BrokerHost.AddBrokerServices(builder, _dbPath);
        _app = builder.Build();
        BrokerHost.MapBrokerRoutes(_app);
        await _app.StartAsync();

        _broker = new BrokerClient(_app.GetTestClient());
        _state = new SessionState();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_broker);
        services.AddSingleton(_state);
        _services = services.BuildServiceProvider();

        // The real production wiring — not a second copy that can pass while production is broken.
        var options = ServerOptionsFactory.Create(_services);

        _clientToServer = new Pipe();
        var clientToServer = _clientToServer;
        var serverToClient = new Pipe();
        _tap = new RecordingStream(serverToClient.Writer.AsStream(), _wire);
        _transport = new StreamServerTransport(
            clientToServer.Reader.AsStream(),
            _tap,
            "peers-test",
            loggerFactory: null);
        _server = McpServer.Create(_transport, options, loggerFactory: null, _services);

        // The production run loop, not a second copy: McpHost is what Program.cs awaits.
        _serverTask = McpHost.RunUntilTransportClosedAsync(_server, _transport);

        _client = await McpClient.CreateAsync(
            new StreamClientTransport(
                clientToServer.Writer.AsStream(),
                serverToClient.Reader.AsStream(),
                loggerFactory: null));

        _handlers =
        [
            _client.RegisterNotificationHandler(NotificationMethods.SubscriptionsAcknowledgedNotification, Capture),
            _client.RegisterNotificationHandler(NotificationMethods.ResourceUpdatedNotification, Capture),
            _client.RegisterNotificationHandler(NotificationMethods.CancelledNotification, Capture),
        ];

        ValueTask Capture(JsonRpcNotification notification, CancellationToken _)
        {
            _notifications.Enqueue((notification.Method, notification.Params));
            return default;
        }
    }

    public async Task DisposeAsync()
    {
        foreach (var handler in _handlers ?? [])
            await Swallow(handler.DisposeAsync().AsTask());

        await Swallow(_client?.DisposeAsync().AsTask());
        await Swallow(_state?.DisposeAsync().AsTask());
        await Swallow(_server?.DisposeAsync().AsTask());
        await Swallow(_serverTask?.WaitAsync(TimeSpan.FromSeconds(5)));
        await Swallow(_services?.DisposeAsync().AsTask());
        _broker?.Dispose();

        try
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
        finally
        {
            foreach (var suffix in new[] { "", "-wal", "-shm" })
            {
                try { File.Delete(_dbPath + suffix); }
                catch { /* best-effort */ }
            }
        }

        static async Task Swallow(Task? task)
        {
            if (task is null) return;
            try { await task; } catch { /* teardown is best-effort */ }
        }
    }

    // -------------------------------------------------------------------------
    // 1. THE NEGATIVE TEST: a subscriber to pad A hears nothing about pad B.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task SubscriberToPadA_ReceivesNothing_WhenPadBChanges()
    {
        await RegisterAsync();
        var padA = await CreatePadAsync("pad A");
        var padB = await CreatePadAsync("pad B");

        using var listen = await ListenAsync("sub-neg", PeerResources.ChannelUri(padA));
        _notifications.Clear();

        await AppendAsync(padB, "change on the pad nobody subscribed to");
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);

        Assert.Empty(_notifications);

        // Same pipe, subscribed URI: proves the silence above was the grant filter, not a dead stream.
        await AppendAsync(padA, "change on the subscribed pad");
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(PeerResources.ChannelUri(padA), update["uri"]!.GetValue<string>());
        Assert.All(_notifications, n =>
            Assert.NotEqual(PeerResources.ChannelUri(padB), n.Params?["uri"]?.GetValue<string>()));
    }

    // -------------------------------------------------------------------------
    // 2. Positive: ack first, then the update — every message tagged with the subscription id.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Subscribe_AcknowledgesFirst_ThenDeliversTaggedUpdates()
    {
        await RegisterAsync();
        var padA = await CreatePadAsync("pad A");
        var uri = PeerResources.ChannelUri(padA);

        using var listen = await ListenAsync("sub-pos", uri);

        // The acknowledgement is the FIRST thing on the subscription.
        Assert.True(_notifications.TryPeek(out var first));
        Assert.Equal(NotificationMethods.SubscriptionsAcknowledgedNotification, first.Method);
        Assert.Equal("sub-pos", SubscriptionIdOf(first.Params!));
        Assert.Equal(new[] { uri }, GrantedUris(first.Params!));

        // Declined types are OMITTED, not present-and-false: the spec has clients compare the ack against
        // what they asked for, and false is a promise about a notification peers never fires.
        var acknowledged = first.Params!["notifications"]!.AsObject().Select(kv => kv.Key).ToArray();
        Assert.DoesNotContain("toolsListChanged", acknowledged);
        Assert.DoesNotContain("promptsListChanged", acknowledged);
        Assert.DoesNotContain("resourcesListChanged", acknowledged);

        // ...and again on the wire, which is where the spec's claim actually lives. Omission is not the
        // registry's doing — it sets those properties to null and relies on McpJsonUtilities.DefaultOptions
        // ignoring nulls (measured: flip that to JsonIgnoreCondition.Never and `"toolsListChanged": null`
        // really is emitted). The assertions above only see it through the SDK client's parsed params.
        var ackFrame = Assert.Single(WireFrames(), f =>
            Str(f["method"]) == NotificationMethods.SubscriptionsAcknowledgedNotification).ToJsonString();
        Assert.DoesNotContain("toolsListChanged", ackFrame);
        Assert.DoesNotContain("promptsListChanged", ackFrame);
        Assert.DoesNotContain("resourcesListChanged", ackFrame);

        await AppendAsync(padA, "an entry");
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(uri, update["uri"]!.GetValue<string>());
        Assert.Equal("sub-pos", SubscriptionIdOf(update));

        // Every message on the stream carries the demux tag, without exception.
        Assert.All(_notifications, n => Assert.Equal("sub-pos", SubscriptionIdOf(n.Params!)));
    }

    [Fact]
    public async Task Subscribe_ToAUriWeDoNotServe_OrToATemplateItself_IsNotGranted()
    {
        await RegisterAsync();
        var pad = await CreatePadAsync("a real pad");

        // The template string is what resources/templates/list hands a client, so subscribing to it
        // verbatim is a plausible mistake. IsMatch accepts it; the fan-out only offers concrete URIs, so
        // granting it would be a subscription that can never fire.
        using var listen = await ListenAsync(
            "sub-unknown",
            "peers://not-a-resource",
            PeerResources.ChannelUriTemplate,
            PeerResources.ChannelUri(pad),
            PeerResources.PeersUri);

        Assert.True(_notifications.TryPeek(out var ack));
        Assert.Equal(
            new[] { PeerResources.ChannelUri(pad), PeerResources.PeersUri }.Order().ToArray(),
            GrantedUris(ack.Params!).Order().ToArray());
    }

    // -------------------------------------------------------------------------
    // 3. THE CONCURRENCY PIN. The whole design rests on this: a parked
    //    subscriptions/listen must not block other requests on the same stdio session.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ToolCall_StillReturns_WhileASubscriptionsListenIsParked()
    {
        await RegisterAsync();
        using var listen = await ListenAsync("sub-conc", PeerResources.PeersUri);

        var call = _client.CallToolAsync("peers").AsTask();
        var finished = await Task.WhenAny(call, Task.Delay(TimeSpan.FromSeconds(10)));

        Assert.True(ReferenceEquals(finished, call),
            "tools/call did not return while a subscriptions/listen request was parked — the SDK serialises requests and this design is wrong.");

        var result = await call;
        Assert.True(result.IsError is not true, TextOf(result));
        Assert.False(listen.Request.IsCompleted, "the listen request should still be parked");
    }

    // -------------------------------------------------------------------------
    // 4. Cancellation returns and deregisters.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task CancellingListen_ReturnsAndDeregisters()
    {
        await RegisterAsync();
        var padA = await CreatePadAsync("pad A");

        var listen = await ListenAsync("sub-cancel", PeerResources.ChannelUri(padA));
        Assert.Equal(1, _state.Subscriptions!.CountForTests);

        // A real stdio client ends a subscription by sending notifications/cancelled referencing
        // the listen request's id — the client-side CTS alone does NOT reach the server (verified
        // empirically against the SDK: local token cancellation never fires the server handler's
        // token). This is what actually unblocks HandleListenAsync's finally-block deregistration.
        await listen.CancelAsync();
        listen.Dispose();

        // Verified empirically: once notifications/cancelled is sent, the SDK client faults the
        // pending request locally with TaskCanceledException (an OperationCanceledException) —
        // it never surfaces the server's graceful EmptyResult response to the caller.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => listen.Request);
        await WaitForAsync(() => _state.Subscriptions!.CountForTests == 0, TimeSpan.FromSeconds(5),
            "the cancelled subscription was never deregistered");

        _notifications.Clear();
        await AppendAsync(padA, "after cancellation");
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);

        Assert.Empty(_notifications);
    }

    // -------------------------------------------------------------------------
    // 4b. ...and a subscription cancelled WHILE the fan-out is mid-flight gets nothing either.
    //     NotifyUriAsync snapshots its subscribers under the lock and then releases it to await the
    //     sends, so on a live session a client that cancels one subscriptions/listen could receive a
    //     resources/updated tagged with the id it just cancelled, AFTER the server had processed the
    //     cancellation — the exact ordering SEP-2575's subscription id exists to keep straight.
    //
    //     Measured, not reasoned about: the wire tap parks the server inside the FIRST subscriber's
    //     send, which is after the snapshot was taken, and the cancellation is completed server-side
    //     while it is parked. Without the active-flag re-check this test fails.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task SubscriptionCancelledMidFanOut_ReceivesNoUpdate_EvenThoughTheSnapshotPredatesIt()
    {
        await RegisterAsync();
        // The 5s notifier would dispatch this same event on its own schedule and make the window
        // non-deterministic. This test drives the dispatch itself.
        await _state.StopServicesAsync();

        var pad = await CreatePadAsync("pad A");
        var uri = PeerResources.ChannelUri(pad);

        using var held = await ListenAsync("sub-held", uri);
        var cancelled = await ListenAsync("sub-cancelled-midflight", uri);
        _notifications.Clear();

        await AppendAsync(pad, "one change, two subscribers");

        // Park the server inside the first subscriber's send: the snapshot of BOTH subscribers is
        // already taken, and the second one's notification has not gone out yet.
        _tap.HoldFirstWriteContaining = NotificationMethods.ResourceUpdatedNotification;
        var dispatch = DispatchBrokerEventsAsync();
        await _tap.Held.WaitAsync(TimeSpan.FromSeconds(15));

        // Server-side deregistration, waited for explicitly: the SDK client faults a cancelled request
        // locally the moment it sends, so the client side proves nothing about what the server did.
        await cancelled.CancelAsync();
        cancelled.Dispose();
        await WaitForAsync(() => _state.Subscriptions!.CountForTests == 1, TimeSpan.FromSeconds(10),
            "the cancellation was never processed server-side, so the race under test never happened");

        _tap.Release();
        await dispatch;

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal("sub-held", SubscriptionIdOf(update));

        await Task.Delay(Settle);
        Assert.DoesNotContain(_notifications, n =>
            n.Method == NotificationMethods.ResourceUpdatedNotification
            && SubscriptionIdOf(n.Params!) == "sub-cancelled-midflight");
    }

    // -------------------------------------------------------------------------
    // The live poll loop: ChannelNotifier must dispatch BEFORE its directed-push filters.
    // This event is authored by the subscriber itself and addressed to nobody, so both
    // IsOwnEvent and ShouldDeliver would drop it — the subscription must still fire.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ChannelNotifier_DeliversOwnUndirectedEvent_ThroughTheLivePollLoop()
    {
        await RegisterAsync();
        var padA = await CreatePadAsync("pad A");
        var uri = PeerResources.ChannelUri(padA);

        using var listen = await ListenAsync("sub-loop", uri);
        _notifications.Clear();

        await AppendAsync(padA, "authored by the subscriber, addressed to nobody");

        // Nothing in this test drives the dispatch; ChannelNotifier polls every 5s.
        var update = await WaitForNotificationAsync(
            NotificationMethods.ResourceUpdatedNotification, TimeSpan.FromSeconds(25));

        Assert.Equal(uri, update["uri"]!.GetValue<string>());
        Assert.Equal("sub-loop", SubscriptionIdOf(update));
    }

    // -------------------------------------------------------------------------
    // A broker stall must cost one tick, not the session. HttpClient.Timeout surfaces as
    // TaskCanceledException — an OperationCanceledException — so the poll loop's unfiltered
    // `catch (OperationCanceledException) { break; }` could not tell an unresponsive broker from our
    // own shutdown, and broke out permanently: push and resources/updated went silent for the life of
    // the session while the durable inbox kept working and nothing looked broken.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ChannelNotifier_SurvivesABrokerStall_AndPollsAgainOnTheNextTick()
    {
        await RegisterAsync();
        // Replaced by the notifier under test, which polls through the stalling handler instead.
        await _state.StopServicesAsync();
        _state.ResetCursorForRegistration(0);

        var stalling = new StallOnceHandler(_app.GetTestServer().CreateHandler(), onPathPrefix: "/poll/");
        using var client = new BrokerClient(new HttpClient(stalling) { BaseAddress = new Uri("http://localhost") });
        await using var notifier = new ChannelNotifier(client, _state);
        notifier.Start();

        // 5s cadence: the stalled poll lands at ~5s, the one that proves the loop is still alive at ~10s.
        await WaitForAsync(() => stalling.Attempts >= 2, TimeSpan.FromSeconds(30),
            "the notifier never polled again after a broker stall — the loop died");

        // Alive is not the same as delivering: the second batch must actually be processed.
        await WaitForAsync(() => _state.LastCursor > 0, TimeSpan.FromSeconds(15),
            "the notifier polled again but processed no batch");

        Assert.Null(stalling.TokenWasCancelled);
    }

    /// <summary>
    /// Fails the first request to <paramref name="onPathPrefix"/> the way <c>HttpClient.Timeout</c> does —
    /// a <see cref="TaskCanceledException"/> raised with the caller's token NOT cancelled — then forwards
    /// everything after it.
    /// </summary>
    private sealed class StallOnceHandler(HttpMessageHandler inner, string onPathPrefix) : DelegatingHandler(inner)
    {
        public int Attempts => Volatile.Read(ref _attempts);
        private int _attempts;

        /// <summary>Non-null if the notifier's own shutdown token was already cancelled at stall time,
        /// which would make this test prove nothing.</summary>
        public string? TokenWasCancelled { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (!request.RequestUri!.AbsolutePath.StartsWith(onPathPrefix, StringComparison.Ordinal))
                return base.SendAsync(request, cancellationToken);

            if (Interlocked.Increment(ref _attempts) != 1)
                return base.SendAsync(request, cancellationToken);

            if (cancellationToken.IsCancellationRequested)
                TokenWasCancelled = "the notifier was already shutting down when the stall was injected";

            return Task.FromException<HttpResponseMessage>(
                new TaskCanceledException("simulated unresponsive broker", new TimeoutException()));
        }
    }

    // -------------------------------------------------------------------------
    // 5. peer_wait's progressToken reaches the client over the wire.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task PeerWait_WithProgressToken_DeliversProgressNotifications()
    {
        await RegisterAsync();

        var ticks = new ProgressCollector();
        // peer_wait ticks every 5s, so the window has to outlast one tick.
        var result = await _client.CallToolAsync(
            "wait",
            new Dictionary<string, object?> { ["timeout_seconds"] = 6 },
            progress: ticks);

        Assert.True(result.IsError is not true, TextOf(result));
        Assert.True(ticks.Values.TryDequeue(out var tick), "no progress notification reached the client");
        Assert.Equal(5f, tick.Progress);
        Assert.Equal(6f, tick.Total!.Value);
        Assert.Contains("Waiting for peer messages", tick.Message);
    }

    // -------------------------------------------------------------------------
    // 5b. The other ending, and the only one a stdio session has besides client
    //     cancellation: the transport closes. SEP-2575 lists it as its own
    //     obligation-free ending ("an abrupt transport drop, which carries no
    //     response"), so peers sends NOTHING — there is no server-initiated
    //     teardown left to send it from. What still has to happen is local, and it
    //     is not free: RunAsync waits for in-flight handlers and transport EOF does
    //     not cancel a parked one, so only McpHost's dispose ends this session. A
    //     plain `await server.RunAsync()` hangs here instead — measured, not feared.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task TransportClose_EndsAParkedListen_WithoutPuttingAnythingOnTheWire()
    {
        await RegisterAsync();
        var registry = _state.Subscriptions!;
        using var listen = await ListenAsync("sub-transport", PeerResources.PeersUri);
        Assert.Equal(1, registry.CountForTests);

        // stdio process exit, faithfully: the write end the server reads from goes away.
        await _clientToServer.Writer.CompleteAsync();

        await _serverTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(0, registry.CountForTests);

        var frames = WireFrames();
        Assert.DoesNotContain(frames, f => Str(f["method"]) == NotificationMethods.CancelledNotification);
        Assert.DoesNotContain(frames, f => Str(f["id"]) == "sub-transport");
    }

    [Fact]
    public async Task ClientCancelledSubscription_ProducesNoServerSentCancelled()
    {
        await RegisterAsync();
        var listen = await ListenAsync("sub-client-cancel", PeerResources.PeersUri);
        _notifications.Clear();

        await listen.CancelAsync();
        listen.Dispose();
        await WaitForAsync(() => _state.Subscriptions!.CountForTests == 0, TimeSpan.FromSeconds(5),
            "the cancelled subscription was never deregistered");
        await Task.Delay(Settle);

        Assert.DoesNotContain(_notifications, n => n.Method == NotificationMethods.CancelledNotification);

        // The cancellation page: a server SHOULD NOT respond to a cancelled request. Measured on the wire
        // because a cancelled request is off the client's pending map, so a late response is dropped
        // silently there. (SDK 2.2.0 suppresses it for us; asserted so an SDK bump cannot change that
        // quietly.)
        var frames = WireFrames();
        Assert.DoesNotContain(frames, f => Str(f["id"]) == "sub-client-cancel");
        Assert.DoesNotContain(frames, f => Str(f["method"]) == NotificationMethods.CancelledNotification);
    }

    // -------------------------------------------------------------------------
    // The same cancellation page, applied to peer_wait — the tool that actually parks. A client that
    // asks for 120s and changes its mind must get: work stopped, and NO response frame.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task CancelledPeerWait_StopsProgress_AndPutsNoResponseOnTheWire()
    {
        await RegisterAsync();

        const string progressToken = "wait-cancel-progress";
        var requestId = new RequestId("wait-cancel");
        using var cts = new CancellationTokenSource();

        // 120s long poll with nothing addressed to this peer: inside this test, only cancellation can end it.
        var call = _client.SendRequestAsync<CallToolRequestParams, CallToolResult>(
            RequestMethods.ToolsCall,
            new CallToolRequestParams
            {
                Name = "wait",
                Arguments = new Dictionary<string, JsonElement> { ["timeout_seconds"] = JsonSerializer.SerializeToElement(120) },
                // RequestParams.ProgressToken is read-only (a projection of _meta), so the token goes in _meta —
                // the same place the SDK reads it from on the server side.
                Meta = new JsonObject { ["progressToken"] = progressToken },
            },
            McpJsonUtilities.DefaultOptions,
            requestId,
            cts.Token).AsTask();

        // peer_wait ticks every 5s, so one tick on the wire proves the handler is parked in the long poll.
        await WaitForAsync(() => ProgressFrames(progressToken).Count >= 1, TimeSpan.FromSeconds(20),
            "peer_wait never reported progress, so it was never parked and this test proves nothing");
        var ticksAtCancel = ProgressFrames(progressToken).Count;

        await _client.SendNotificationAsync(
            NotificationMethods.CancelledNotification,
            new CancelledNotificationParams { RequestId = requestId, Reason = "test" },
            McpJsonUtilities.DefaultOptions);

        // Same local fault as a cancelled subscriptions/listen: the SDK client never surfaces a server result here.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);

        // Longer than one tick interval, and it has to be: there is no gate to insert into a real long poll.
        // If the SDK's CancellationToken binding never reached the handler, the ticker would still be running
        // and this window would catch the next notification.
        await Task.Delay(TimeSpan.FromSeconds(7));
        Assert.Equal(ticksAtCancel, ProgressFrames(progressToken).Count);

        // "Not send a response for the cancelled request." Only the wire can answer it — the client drops a
        // late response for a request already off its pending map.
        Assert.DoesNotContain(WireFrames(), f => Str(f["id"]) == "wait-cancel");
    }

    /// <summary>notifications/progress frames the server actually wrote for one progress token.</summary>
    private List<JsonObject> ProgressFrames(string progressToken) =>
        WireFrames()
            .Where(f => Str(f["method"]) == NotificationMethods.ProgressNotification
                     && Str(f["params"]?["progressToken"]) == progressToken)
            .ToList();

    // -------------------------------------------------------------------------
    // Cold start: a subscription reports changes that happened after it existed,
    // and one poll batch is at most one hint per URI. Both halves of the same
    // defect — before this, a fresh session replayed the retention window.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Register_SeedsTheResourceBaseline_AboveEverythingThatAlreadyExisted()
    {
        await RegisterAsync();
        var pad = await CreatePadAsync("history");
        await AppendAsync(pad, "an entry that predates the session under test");

        var brokerHead = (await _broker.PollAsync(_peerId!, 0, 500)).Cursor;
        Assert.True(brokerHead > 0, "the history this test needs was not written");

        // Re-registering restarts the channel notifier, which is exactly the cold-start path.
        await RegisterAsync();

        Assert.True(_state.ResourceBaseline > brokerHead,
            $"resource dispatch would replay history: baseline {_state.ResourceBaseline} <= head {brokerHead}");
    }

    [Fact]
    public async Task ThreeChangesToOneResourceInOnePollBatch_CoalesceToOneNotification()
    {
        await RegisterAsync();
        var pad = await CreatePadAsync("busy pad");
        var uri = PeerResources.ChannelUri(pad);

        using var listen = await ListenAsync("sub-coalesce", uri);
        _notifications.Clear();

        await AppendAsync(pad, "one");
        await AppendAsync(pad, "two");
        await AppendAsync(pad, "three");

        // One poll batch carrying three channel.message_added events for the same URI.
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(uri, update["uri"]!.GetValue<string>());

        await Task.Delay(Settle);
        Assert.Single(_notifications);
    }

    // -------------------------------------------------------------------------
    // 6. THE REVISION THAT ACTUALLY SHIPS. Every test above negotiates the newest
    //    revision the SDK knows, so they only ever exercise subscriptions/listen —
    //    a method no client in the field speaks. Peers does not serve the older
    //    mechanism; this pins a client to 2025-11-25 and proves it is REFUSED
    //    rather than answered with the SDK's manufactured empty success.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task On2025_11_25_SubscriptionsAreRefused_ButResourceReadsStillWork()
    {
        await using var legacy = await StartLegacySessionAsync();
        Assert.Equal(November2025ProtocolVersion, legacy.Client.NegotiatedProtocolVersion);

        // The half that must KEEP working. resources/list, resources/templates/list and resources/read
        // carry no revision gating, and for every client shipping today they are the entire delivered
        // value of the resource surface. Only subscribing is refused below.
        Assert.Contains(await legacy.Client.ListResourcesAsync(), r => r.Uri == PeerResources.PeersUri);
        Assert.Contains(await legacy.Client.ListResourceTemplatesAsync(),
            t => t.UriTemplate == PeerResources.ChannelUriTemplate);

        var peers = await legacy.Client.ReadResourceAsync(PeerResources.PeersUri);
        Assert.Contains("peer_id", TextOf(peers));

        var padId = JsonNode.Parse(TextOf(await legacy.Client.CallToolAsync(
            "channel_create",
            new Dictionary<string, object?> { ["name"] = $"legacy-read-{Guid.NewGuid():N}" })))!["channel_id"]!.GetValue<string>();
        var channel = await legacy.Client.ReadResourceAsync(PeerResources.ChannelUri(padId));
        Assert.Contains("messages", TextOf(channel));

        // resources/subscribe: ours. Without the handler the SDK answers {"result":{}} and the client
        // waits forever; with it, the client gets a reason it can print.
        var subscribe = await Assert.ThrowsAsync<McpProtocolException>(
            () => legacy.Client.SubscribeToResourceAsync(PeerResources.PeersUri));
        Assert.Equal(McpErrorCode.MethodNotFound, subscribe.ErrorCode);
        Assert.Contains(RequestMethods.SubscriptionsListen, subscribe.Message);
        Assert.Contains(November2025ProtocolVersion, subscribe.Message);

        var unsubscribe = await Assert.ThrowsAsync<McpProtocolException>(
            () => legacy.Client.UnsubscribeFromResourceAsync(PeerResources.PeersUri));
        Assert.Equal(McpErrorCode.MethodNotFound, unsubscribe.ErrorCode);

        // subscriptions/listen: the SDK's, already unreachable on this revision. Asserted so a future SDK
        // bump that quietly starts serving it here does not go unnoticed.
        var listen = await Assert.ThrowsAsync<McpProtocolException>(() =>
            legacy.Client.SendRequestAsync<SubscriptionsListenRequestParams, EmptyResult>(
                RequestMethods.SubscriptionsListen,
                new SubscriptionsListenRequestParams { Notifications = new() { ResourceSubscriptions = [PeerResources.PeersUri] } },
                McpJsonUtilities.DefaultOptions,
                new RequestId("legacy-listen"),
                CancellationToken.None).AsTask());
        Assert.Equal(McpErrorCode.MethodNotFound, listen.ErrorCode);
    }

    /// <summary>The revision every shipping client negotiates. The SDK's McpProtocolVersions is internal, so this is a literal.</summary>
    private const string November2025ProtocolVersion = "2025-11-25";

    /// <summary>Any tool call registers the session; peers is the cheapest one that does.</summary>
    private static Task EnsureSessionRegisteredAsync(McpClient client) => client.CallToolAsync("peers").AsTask();

    /// <summary>
    /// A second MCP session against the same broker, pinned to 2025-11-25 — its own SessionState, built by
    /// the same production ServerOptionsFactory as everything else.
    /// </summary>
    private sealed class LegacySession(
        ServiceProvider services, SessionState state, McpServer server, Task serverTask, McpClient client)
        : IAsyncDisposable
    {
        public McpClient Client => client;

        public async ValueTask DisposeAsync()
        {
            try { await client.DisposeAsync(); } catch { /* teardown is best-effort */ }
            try { await state.DisposeAsync(); } catch { /* teardown is best-effort */ }
            try { await server.DisposeAsync(); } catch { /* teardown is best-effort */ }
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* teardown is best-effort */ }
            try { await services.DisposeAsync(); } catch { /* teardown is best-effort */ }
        }
    }

    private async Task<LegacySession> StartLegacySessionAsync()
    {
        var state = new SessionState();
        var collection = new ServiceCollection();
        collection.AddLogging();
        collection.AddSingleton(_broker);
        collection.AddSingleton(state);
        var services = collection.BuildServiceProvider();

        // The real production wiring again — the point of this test is that it is not a second copy.
        var options = ServerOptionsFactory.Create(services);

        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        var server = McpServer.Create(
            new StreamServerTransport(
                clientToServer.Reader.AsStream(), serverToClient.Writer.AsStream(), "peers-legacy", loggerFactory: null),
            options, loggerFactory: null, services);
        var serverTask = server.RunAsync();

        var client = await McpClient.CreateAsync(
            new StreamClientTransport(clientToServer.Writer.AsStream(), serverToClient.Reader.AsStream(), loggerFactory: null),
            new McpClientOptions { ProtocolVersion = November2025ProtocolVersion });

        await EnsureSessionRegisteredAsync(client);
        return new LegacySession(services, state, server, serverTask, client);
    }

    // -------------------------------------------------------------------------
    // Cold start, through the LIVE poll loop — the only place ChannelNotifier's
    // resource baseline is applied at all. A fresh registration's cursor is 0, so
    // a baseline read from LastCursor (or no baseline filter) replays the whole
    // retention window at the first subscriber. Register_SeedsTheResourceBaseline
    // above only proves the FIELD was written; this proves the field is used.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ColdStart_ReplaysNoHistory_ThroughTheLivePollLoop()
    {
        await RegisterAsync();
        var history = await CreatePadAsync("pad with history");
        await AppendAsync(history, "an entry from before the session under test existed");
        var fresh = await CreatePadAsync("pad changed afterwards");

        // A second registration with no resume id is a NEW peer: last_cursor 0, baseline at the broker
        // head. That is the cold start, and the notifier it starts is the one under test.
        await RegisterAsync();

        using var listen = await ListenAsync(
            "sub-cold", PeerResources.ChannelUri(history), PeerResources.ChannelUri(fresh));
        _notifications.Clear();

        await AppendAsync(fresh, "the only change that happened after this session started");

        // Nothing here drives the dispatch; the notifier polls every 5s. Its first batch starts at cursor 0
        // and therefore CARRIES the historical entry — only the baseline filter keeps it off the wire.
        var update = await WaitForNotificationAsync(
            NotificationMethods.ResourceUpdatedNotification, TimeSpan.FromSeconds(25));
        Assert.Equal(PeerResources.ChannelUri(fresh), update["uri"]!.GetValue<string>());

        await Task.Delay(Settle);
        Assert.DoesNotContain(_notifications, n =>
            n.Params?["uri"]?.GetValue<string>() == PeerResources.ChannelUri(history));
    }

    // -------------------------------------------------------------------------
    // A subscription belongs to the MCP connection, not to a peer registration:
    // re-registering must not disturb a stream nobody cancelled. StopServicesAsync
    // is what re-register calls, and it deliberately does not touch subscriptions.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ReRegistering_LeavesSubscriptionsNobodyCancelledAlone()
    {
        await RegisterAsync();
        var pad = await CreatePadAsync("pad A");
        var uri = PeerResources.ChannelUri(pad);

        using var listen = await ListenAsync("sub-rereg", uri);
        Assert.Equal(1, _state.Subscriptions!.CountForTests);

        await RegisterAsync();
        _notifications.Clear();

        Assert.Equal(1, _state.Subscriptions!.CountForTests);
        Assert.False(listen.Request.IsCompleted, "the re-register ended a listen stream nobody cancelled");

        // Still registered is not the same as still delivering, so prove delivery too.
        await AppendAsync(pad, "after the re-register");
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(uri, update["uri"]!.GetValue<string>());
        Assert.Equal("sub-rereg", SubscriptionIdOf(update));
        Assert.DoesNotContain(_notifications, n => n.Method == NotificationMethods.CancelledNotification);
    }

    // -------------------------------------------------------------------------
    // peers://peers actually fires. Every other resources/updated assertion in this
    // file is on a channel URI, so the peer arm of ResourceUriFor could be
    // deleted with the suite still green — and peers://peers would go permanently
    // silent, which is the whole point of the peer.updated event behind it.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task PeerChange_NotifiesThePeersResource()
    {
        await RegisterAsync();
        using var listen = await ListenAsync("sub-peers", PeerResources.PeersUri);

        // Drain what predates the subscription, so what follows is only the change under test.
        await DispatchBrokerEventsAsync();
        _notifications.Clear();

        // A heartbeat carrying a status field is exactly what changes the peers://peers payload; the
        // broker emits peer.updated for it (an all-null lease renewal deliberately emits nothing).
        await _client.CallToolAsync(
            "presence", new Dictionary<string, object?> { ["summary"] = "now reviewing" });
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(PeerResources.PeersUri, update["uri"]!.GetValue<string>());
        Assert.Equal("sub-peers", SubscriptionIdOf(update));
    }

    // -------------------------------------------------------------------------
    // QUIET WHISPERS ARE PARTICIPANT-SCOPED. A quiet message addressed to somebody else must not
    // even generate a hint on this session's stream: the registry filters by this session's peer id
    // before any URI reaches the fan-out. The loud control proves the stream is alive either way.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task QuietWhisperForSomebodyElse_NotifiesNothing_ButALoudMessageStillDoes()
    {
        await RegisterAsync();
        var channel = await CreatePadAsync("whispers");
        var uri = PeerResources.ChannelUri(channel);

        using var listen = await ListenAsync("sub-whisper", uri);
        _notifications.Clear();

        // Addressed to a third peer: this session is neither author nor mention.
        var stranger = await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = "stranger-machine", RepoRoot = "/other-repo", Branch = "main",
        });
        await WhisperFromOtherPeerAsync(channel, "not for you", stranger.PeerId);
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);

        Assert.DoesNotContain(_notifications, n => n.Method == NotificationMethods.ResourceUpdatedNotification);

        // Same stream, loud message: the silence above was the whisper filter, not a dead subscription.
        await AppendAsync(channel, "for everyone");
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(uri, update["uri"]!.GetValue<string>());
        Assert.Equal("sub-whisper", SubscriptionIdOf(update));
    }

    // -------------------------------------------------------------------------
    // ...and a whisper this session IS mentioned in does notify it.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task QuietWhisperMentioningUs_NotifiesOurStream()
    {
        await RegisterAsync();
        var channel = await CreatePadAsync("whispers-for-us");
        var uri = PeerResources.ChannelUri(channel);

        using var listen = await ListenAsync("sub-whisper-mine", uri);
        _notifications.Clear();

        await WhisperFromOtherPeerAsync(channel, "just for you", _peerId!);
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(uri, update["uri"]!.GetValue<string>());
        Assert.Equal("sub-whisper-mine", SubscriptionIdOf(update));
    }

    // -------------------------------------------------------------------------
    // Two listen streams open at once — what the _meta subscription id EXISTS for.
    // Every other test here opens exactly one, so tagging every notification with
    // the first subscription's id would pass all of them while a real multiplexing
    // client routed pad B's update into pad A's stream.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task TwoOpenSubscriptions_EachUpdateIsTaggedForItsOwnStream()
    {
        await RegisterAsync();
        var padA = await CreatePadAsync("pad A");
        var padB = await CreatePadAsync("pad B");
        var uriA = PeerResources.ChannelUri(padA);
        var uriB = PeerResources.ChannelUri(padB);

        // Overlapping grants: both streams asked for pad A, only one asked for pad B.
        using var one = await ListenAsync("sub-one", uriA);
        using var two = await ListenAsync("sub-two", uriA, uriB);
        _notifications.Clear();

        await AppendAsync(padA, "both streams asked for this");
        await AppendAsync(padB, "only sub-two asked for this");
        await DispatchBrokerEventsAsync();

        await WaitForAsync(
            () => Updates().Count(u => u.Uri == uriA) >= 2 && Updates().Any(u => u.Uri == uriB),
            TimeSpan.FromSeconds(10),
            "both subscriptions should have been notified about the shared URI");

        // Distinct, because the live 5s poll loop may re-deliver the same URI while this test runs.
        var updates = Updates();
        Assert.Equal(
            new[] { "sub-one", "sub-two" },
            updates.Where(u => u.Uri == uriA).Select(u => u.Id).Distinct().Order().ToArray());
        Assert.Equal(
            new[] { "sub-two" },
            updates.Where(u => u.Uri == uriB).Select(u => u.Id).Distinct().ToArray());

        (string Uri, string Id)[] Updates() => _notifications
            .Where(n => n.Method == NotificationMethods.ResourceUpdatedNotification)
            .Select(n => (n.Params!["uri"]!.GetValue<string>(), SubscriptionIdOf(n.Params!)))
            .ToArray();
    }

    // -------------------------------------------------------------------------
    // THE REGISTRATION GUARD. peer_message shipped for its entire life implementing a tool that
    // ServerOptionsFactory never registered, so it never appeared in tools/list — and its unit tests
    // passed the whole time by calling the static method directly. A hard-coded expected list would
    // NOT have caught that (the tool was missing from both sides), so the expectation is derived from
    // the assembly: every [McpServerTool] that EXISTS must be REACHABLE.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ToolsList_ExposesEveryToolDeclaredInTheAssembly()
    {
        var declared = DeclaredToolNames();
        var listed = (await _client.ListToolsAsync()).Select(t => t.Name).Order().ToArray();

        Assert.Equal(declared, listed);

        // And the surface itself, spelled out: a tool silently disappearing from BOTH sides is still a
        // contract change, and this is the line that has to be edited to make it deliberate.
        Assert.Equal(
            new[]
            {
                "artifact_get", "artifact_list", "artifact_publish",
                "channel_create", "channels",
                "context_export", "context_import",
                "peers", "presence", "read", "send", "wait",
            },
            listed);
    }

    /// <summary>Every [McpServerTool] in the Peers.Mcp assembly, registered or not.</summary>
    private static string[] DeclaredToolNames() =>
        typeof(ChannelTools).Assembly.GetTypes()
            .SelectMany(t => t.GetMethods(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => (Method: m, Attr: m.GetCustomAttribute<McpServerToolAttribute>()))
            .Where(x => x.Attr is not null)
            // Every tool in this repo names itself explicitly; a nameless one would mismatch here and say so.
            .Select(x => x.Attr!.Name ?? x.Method.Name)
            .Order()
            .ToArray();

    // -------------------------------------------------------------------------
    // Acknowledgement is by receipt. Reading acknowledges nothing, so an item stays deliverable until
    // the caller echoes the inbox_id it was given. Under the previous defer-until-next-call scheme the
    // SECOND read below came back empty — the first read had staged its own ack.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Inbox_RedeliversUntilTheCallerEchoesTheReceipt()
    {
        await RegisterAsync();
        await SendReviewRequestToSessionPeerAsync("please review");

        var first = await InboxAsync();
        var inboxId = Assert.Single(InboxIds(first));

        // Not echoed => not acknowledged => delivered again. Duplication, which is the correct side of
        // at-least-once, instead of the silent loss the old scheme produced.
        var second = await InboxAsync();
        Assert.Equal(new[] { inboxId }, InboxIds(second));

        // Echoed => acknowledged, before the read, so it cannot come back in this same result...
        Assert.Empty(InboxIds(await InboxAsync(inboxId)));
        // ...and it stays acknowledged.
        Assert.Empty(InboxIds(await InboxAsync()));
    }

    // -------------------------------------------------------------------------
    // THE DEFECT THIS REPLACED, MEASURED. The server drains the item and writes a result the client
    // never receives — injected at the transport, so it is a fact and not a race against
    // notifications/cancelled, which in-process is unwinnable in the interesting direction.
    //
    // Under defer-until-next-call this test FAILS: the dropped result had already staged its ids, and
    // the next drain acked them before reading, so the mention was gone with nobody having
    // ever seen it. Now the caller holds no receipt, echoes nothing, and the item is still there.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task DroppedResponse_LeavesTheItemDeliverable()
    {
        await RegisterAsync();
        await SendReviewRequestToSessionPeerAsync("review request whose result never lands");

        var requestId = new RequestId("wait-dropped");
        _tap.DropResponsesForId = "wait-dropped";
        using var abandoned = new CancellationTokenSource();

        // The item is already in the inbox, so the long poll returns it on its first look: the server
        // really drains it, shapes a result, and writes the response frame — which the tap swallows.
        var call = _client.SendRequestAsync<CallToolRequestParams, CallToolResult>(
            RequestMethods.ToolsCall,
            new CallToolRequestParams
            {
                Name = "wait",
                Arguments = new Dictionary<string, JsonElement> { ["timeout_seconds"] = JsonSerializer.SerializeToElement(120) },
            },
            McpJsonUtilities.DefaultOptions,
            requestId,
            abandoned.Token).AsTask();

        await WaitForAsync(() => _tap.DroppedFrames >= 1, TimeSpan.FromSeconds(20),
            "no response frame for peer_wait was ever produced, so nothing was dropped and this test proves nothing");

        // The result really was produced — and it really did carry the item, so the old scheme would
        // have staged its id here.
        var dropped = WireFrames().Single(f => Str(f["id"]) == "wait-dropped");
        Assert.Contains("inbox_id", dropped.ToJsonString());

        // The client never completed: a dropped response is exactly a caller holding no receipt.
        Assert.False(call.IsCompleted, "the response was not actually dropped");

        // Therefore nothing can be echoed, and the item must still be deliverable.
        _tap.DropResponsesForId = null;
        Assert.Single(InboxIds(await InboxAsync()));
    }

    /// <summary>A directed inbox item for this session's peer: a mention from a second peer.</summary>
    private async Task SendReviewRequestToSessionPeerAsync(string content)
    {
        var reviewer = await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "claude-code", MachineId = "test-machine", RepoRoot = "/other-repo", Branch = "main",
        });

        var channel = await _broker.CreateChannelAsync(new CreateChannelRequest
        {
            Name = $"review-{Guid.NewGuid():N}",
            RepoRoot = "/other-repo",
            CreatedByPeerId = reviewer.PeerId,
        });

        var sent = await _broker.SendMessageAsync(channel.ChannelId, new SendMessageRequest
        {
            AuthorPeerId = reviewer.PeerId, Content = content, To = [_peerId!],
        });

        Assert.Equal(new[] { _peerId }, sent!.Mentions);
    }

    private async Task<JsonNode> InboxAsync(params string[] ackInboxIds)
    {
        var args = new Dictionary<string, object?>();
        if (ackInboxIds.Length > 0)
            args["ack_inbox_ids"] = ackInboxIds;

        args["timeout_seconds"] = 1;
        var result = await _client.CallToolAsync("wait", args);
        Assert.True(result.IsError is not true, TextOf(result));
        return JsonNode.Parse(TextOf(result))!;
    }

    private static string[] InboxIds(JsonNode inbox) =>
        inbox["items"]!.AsArray().Select(i => i!["inbox_id"]!.GetValue<string>()).ToArray();

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    /// <summary>Write-through tap on the server's outbound stream. Records what actually went on the wire.</summary>
    private sealed class RecordingStream(Stream inner, ConcurrentQueue<string> sink) : Stream
    {
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override Task FlushAsync(CancellationToken ct) => inner.FlushAsync(ct);
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <summary>
        /// When set, response frames carrying this JSON-RPC id are recorded and then NOT forwarded: the
        /// server really produced a result and the client really never received it. That is the exact
        /// failure the receipt-ack contract exists for, and injecting it here makes it a fact instead of a
        /// race against notifications/cancelled — which can only ever be won by luck in-process.
        /// </summary>
        public string? DropResponsesForId { get; set; }

        public int DroppedFrames => Volatile.Read(ref _dropped);
        private int _dropped;

        /// <summary>
        /// When set, the FIRST outbound write containing this text is parked before it reaches the stream
        /// and <see cref="Held"/> completes; it resumes on <see cref="Release"/>. That is the only way to
        /// stand inside the server's fan-out — between the subscriber snapshot and a later subscriber's
        /// send — and change the registry while it is there.
        /// </summary>
        public string? HoldFirstWriteContaining { get; set; }

        public Task Held => _held.Task;
        private readonly TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => _release.TrySetResult();

        /// <summary>Partial frame carried across a chunk boundary while dropping is armed.</summary>
        private string _carry = "";

        public override void Write(byte[] buffer, int offset, int count)
        {
            var pass = Filter(buffer.AsSpan(offset, count));
            if (pass.Length > 0)
                inner.Write(pass, 0, pass.Length);
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            var bytes = buffer.ToArray();

            var hold = HoldFirstWriteContaining;
            if (hold is not null && Encoding.UTF8.GetString(bytes).Contains(hold, StringComparison.Ordinal))
            {
                HoldFirstWriteContaining = null;
                _held.TrySetResult();
                await _release.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);
            }

            var pass = Filter(bytes);
            if (pass.Length > 0)
                await inner.WriteAsync(pass, ct);
        }

        private byte[] Filter(ReadOnlySpan<byte> bytes)
        {
            var text = Encoding.UTF8.GetString(bytes);
            var trimmed = text.Trim();
            if (trimmed.Length > 0)
                sink.Enqueue(trimmed);

            var dropId = DropResponsesForId;
            // Unarmed: byte-exact pass-through, so every other test in this file sees the stream it always saw.
            if (dropId is null)
                return bytes.ToArray();

            // Frames are newline-delimited, so only whole lines can be judged. Hold a partial one back.
            _carry += text;
            var lastNewline = _carry.LastIndexOf('\n');
            if (lastNewline < 0)
                return [];

            var complete = _carry[..(lastNewline + 1)];
            _carry = _carry[(lastNewline + 1)..];

            var kept = new StringBuilder();
            foreach (var line in complete.Split('\n'))
            {
                if (line.Length == 0)
                    continue;
                if (IsFrameFor(line, dropId))
                {
                    Interlocked.Increment(ref _dropped);
                    continue;
                }
                kept.Append(line).Append('\n');
            }

            return Encoding.UTF8.GetBytes(kept.ToString());
        }

        private static bool IsFrameFor(string line, string id)
        {
            try { return JsonNode.Parse(line) is JsonObject frame && Str(frame["id"]) == id; }
            catch { return false; }
        }
    }

    private sealed class ProgressCollector : IProgress<ProgressNotificationValue>
    {
        public ConcurrentQueue<ProgressNotificationValue> Values { get; } = new();

        public void Report(ProgressNotificationValue value) => Values.Enqueue(value);
    }

    private sealed class Listen(Task request, CancellationTokenSource cts, McpClient client, RequestId requestId)
        : IDisposable
    {
        public Task Request { get; } = request;

        /// <summary>
        /// Cancels the way a real stdio client does: sends notifications/cancelled referencing this
        /// listen request's id. The SDK does NOT do this on local CTS cancellation alone (verified
        /// empirically — see CancellingListen_ReturnsAndDeregisters), so the server handler's token
        /// only fires once this notification arrives.
        /// </summary>
        public Task CancelAsync() => client.SendNotificationAsync(
            NotificationMethods.CancelledNotification,
            new CancelledNotificationParams { RequestId = requestId, Reason = "test" },
            McpJsonUtilities.DefaultOptions);

        public void Dispose() => cts.Cancel();
    }

    /// <summary>Opens a subscriptions/listen with an explicit request id, and returns once acknowledged.</summary>
    private async Task<Listen> ListenAsync(string subscriptionId, params string[] uris)
    {
        var cts = new CancellationTokenSource();
        var requestId = new RequestId(subscriptionId);
        var request = _client.SendRequestAsync<SubscriptionsListenRequestParams, EmptyResult>(
            RequestMethods.SubscriptionsListen,
            new SubscriptionsListenRequestParams { Notifications = new() { ResourceSubscriptions = uris } },
            McpJsonUtilities.DefaultOptions,
            requestId,
            cts.Token).AsTask();

        try
        {
            await WaitForNotificationAsync(NotificationMethods.SubscriptionsAcknowledgedNotification);
        }
        catch
        {
            cts.Cancel();
            throw;
        }

        return new Listen(request, cts, _client, requestId);
    }

    // Registration is no longer an agent-facing tool: sessions register themselves. Tests drive the
    // same production seam with an explicit identity so ids stay deterministic; a second call is a
    // NEW peer (cursor 0, baseline at head), which is what the cold-start tests need.
    private async Task RegisterAsync()
    {
        var resp = await Peers.Mcp.Lifecycle.SessionRegistration.RegisterWithAsync(
            _broker, _state,
            Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance,
            new RegisterPeerRequest
            {
                Tool = "claude-code", MachineId = "test-machine", RepoRoot = "/repo", Branch = "main",
                Summary = "integration session",
            });
        _peerId = resp.PeerId;
    }

    private async Task<string> CreatePadAsync(string name)
    {
        var resp = await _broker.CreateChannelAsync(new CreateChannelRequest
        {
            Name = $"{name}-{Guid.NewGuid():N}",
            RepoRoot = "/repo",
            CreatedByPeerId = _peerId!,
        });
        return resp.ChannelId;
    }

    private Task AppendAsync(string channelId, string content) =>
        _broker.SendMessageAsync(channelId, new SendMessageRequest
        {
            AuthorPeerId = _peerId!,
            Content = content,
        });

    /// <summary>A quiet whisper authored by a second peer, addressed to <paramref name="mentions"/> only.</summary>
    private async Task WhisperFromOtherPeerAsync(string channelId, string content, params string[] mentions)
    {
        var author = await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = "whisper-machine", RepoRoot = "/other-repo", Branch = "main",
        });
        var sent = await _broker.SendMessageAsync(channelId, new SendMessageRequest
        {
            AuthorPeerId = author.PeerId,
            Content = content,
            To = mentions,
            Quiet = true,
        });
        Assert.NotNull(sent);
    }

    /// <summary>Exactly what ChannelNotifier's loop body does, without waiting on its 5s timer.</summary>
    private async Task DispatchBrokerEventsAsync()
    {
        var resp = await _broker.PollAsync(_peerId!, _testCursor, 100);
        _testCursor = resp.Cursor;
        await _state.Subscriptions!.NotifyEventsAsync(resp.Events);
    }

    private async Task<JsonObject> WaitForNotificationAsync(string method, TimeSpan? timeout = null)
    {
        JsonNode? found = null;
        await WaitForAsync(
            () => _notifications.Any(n => n.Method == method && (found = n.Params) is not null),
            timeout ?? TimeSpan.FromSeconds(10),
            $"no {method} notification arrived");

        return found!.AsObject();
    }

    private static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout, string because)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(25);
        }

        Assert.Fail(because);
    }

    /// <summary>A JSON string value, or null for anything else. JSON-RPC ids may be numbers.</summary>
    private static string? Str(JsonNode? node) =>
        node is JsonValue value && value.TryGetValue<string>(out var text) ? text : null;

    /// <summary>Every JSON-RPC frame the server wrote, parsed, in order.</summary>
    private List<JsonObject> WireFrames()
    {
        var frames = new List<JsonObject>();
        foreach (var chunk in _wire)
        {
            foreach (var line in chunk.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                try { if (JsonNode.Parse(line) is JsonObject frame) frames.Add(frame); }
                catch { /* a partial write is not a frame */ }
            }
        }

        return frames;
    }

    private static string[] GrantedUris(JsonNode ackParams) =>
        ackParams["notifications"]!["resourceSubscriptions"]!.AsArray()
            .Select(n => n!.GetValue<string>()).ToArray();

    private static string SubscriptionIdOf(JsonNode notificationParams) =>
        notificationParams["_meta"]![MetaKeys.SubscriptionId]!.GetValue<string>();

    private static string TextOf(CallToolResult result) =>
        Assert.IsType<TextContentBlock>(Assert.Single(result.Content)).Text;

    private static string TextOf(ReadResourceResult result) =>
        Assert.IsType<TextResourceContents>(Assert.Single(result.Contents)).Text;
}
