using System.Collections.Concurrent;
using System.Diagnostics;
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
/// production entry point builds, and a real MCP client over a stream-pair transport. resources/subscribe
/// behaviour is the bulk of it; the tools/list and inbox-acknowledgement tests share the same harness
/// precisely because "real wiring" is the property they all assert.
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
        services.AddSingleton(new Peers.Mcp.Reviews.ReviewStorageOptions(Path.Combine(Path.GetTempPath(), "peers-review-sub-" + Guid.NewGuid().ToString("N"))));
        _services = services.BuildServiceProvider();

        // The real production wiring — not a second copy that can pass while production is broken.
        var options = ServerOptionsFactory.Create(_services);

        var clientToServer = new Pipe();
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
    // THE PUSH CONTRACT, case (b): a URI nobody subscribed to yields NOTHING, and
    // the same batch proves the positive half so silence is never ambiguous.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task SubscriberToPadA_ReceivesNothing_WhenPadBChanges()
    {
        await RegisterAsync();
        var padA = await CreatePadAsync("pad A");
        var padB = await CreatePadAsync("pad B");

        await SubscribeAsync(PeerResources.ChannelUri(padA));
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

    [Fact]
    public async Task UnsubscribedUri_YieldsNoHints_AtAll()
    {
        await RegisterAsync();
        _notifications.Clear();

        // A mention lands durably while NOTHING is subscribed: no resources/updated may be generated,
        // because the push path must never invent delivery for URIs the client did not ask about.
        await SendReviewRequestToSessionPeerAsync("mention with zero subscribers");
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);

        Assert.DoesNotContain(_notifications, n => n.Method == NotificationMethods.ResourceUpdatedNotification);
    }

    // -------------------------------------------------------------------------
    // Case (d): a TEMPLATE URI is what resources/templates/list hands a client, so
    // subscribing to it verbatim is a plausible mistake. IsMatch accepts it; the
    // dispatch only ever offers concrete URIs, so granting it would be a
    // subscription that can never fire. The subscribe call SUCCEEDS on the wire
    // (there is no refusal protocol to speak of) — "not granted" means the server
    // never put the URI in its subscriber set, which only the server can see.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task TemplateUri_SubscribeAttempt_IsNotGranted()
    {
        await RegisterAsync();
        var registry = _state.Subscriptions!;

        await _client.SubscribeToResourceAsync(PeerResources.ChannelUriTemplate);
        await _client.SubscribeToResourceAsync("peers://not-a-resource");
        Assert.Equal(0, registry.CountForTests);

        // Control: a concrete URI the server serves IS granted.
        await _client.SubscribeToResourceAsync(PeerResources.PeersUri);
        Assert.True(registry.IsSubscribedForTests(PeerResources.PeersUri));
        Assert.Equal(1, registry.CountForTests);
    }

    // -------------------------------------------------------------------------
    // Case (a): the inbox push path. Two mentions land in ONE poll batch — N
    // changes to one resource are ONE hint, not N.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task MentionBatch_ForSubscribedInbox_YieldsExactlyOneHint()
    {
        await RegisterAsync();
        await SubscribeAsync(PeerResources.InboxUri);
        _notifications.Clear();

        var author = await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = "mention-machine", RepoRoot = "/other-repo", Branch = "main",
        });
        var channel = await _broker.CreateChannelAsync(new CreateChannelRequest
        {
            Name = $"mentions-{Guid.NewGuid():N}",
            RepoRoot = "/other-repo",
            CreatedByPeerId = author.PeerId,
        });
        for (var i = 0; i < 2; i++)
        {
            var sent = await _broker.SendMessageAsync(channel.ChannelId, new SendMessageRequest
            {
                AuthorPeerId = author.PeerId, Content = $"mention {i}", To = [_peerId!],
            });
            Assert.NotNull(sent);
        }

        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(PeerResources.InboxUri, update["uri"]!.GetValue<string>());

        await Task.Delay(Settle);
        Assert.Single(_notifications);
    }

    // -------------------------------------------------------------------------
    // Case (c): a quiet whisper addressed to a THIRD peer must not produce even a
    // hint here. The loud mention below proves the stream was alive either way.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task QuietWhisperForAThirdPeer_YieldsNoInboxHintHere()
    {
        await RegisterAsync();
        await SubscribeAsync(PeerResources.InboxUri);
        _notifications.Clear();

        var stranger = await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = "stranger-machine", RepoRoot = "/other-repo", Branch = "main",
        });
        var channel = await CreatePadAsync("whispers-for-someone-else");
        await WhisperFromOtherPeerAsync(channel, "not for you", stranger.PeerId);
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);

        Assert.DoesNotContain(_notifications, n => n.Method == NotificationMethods.ResourceUpdatedNotification);

        // Loud control, same subscription.
        await SendReviewRequestToSessionPeerAsync("loud control mention");
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(PeerResources.InboxUri, update["uri"]!.GetValue<string>());
    }

    // -------------------------------------------------------------------------
    // Case (e), the presence-through-quietness guard: recipient_peer_ids rides
    // QUIET sends too, so a whisper addressed TO this session produces exactly one
    // peers://inbox hint even though it is invisible to every other session.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task QuietWhisperAddressedToUs_YieldsExactlyOneInboxHint()
    {
        await RegisterAsync();
        await SubscribeAsync(PeerResources.InboxUri);
        _notifications.Clear();

        var channel = await CreatePadAsync("whispers-addressed-to-us");
        await WhisperFromOtherPeerAsync(channel, "quiet, but for you", _peerId!);
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(PeerResources.InboxUri, update["uri"]!.GetValue<string>());

        await Task.Delay(Settle);
        Assert.Single(_notifications);
    }

    // -------------------------------------------------------------------------
    // Reading acknowledges nothing: the resource read leaves the item deliverable,
    // and wait keeps redelivering it until the caller echoes ack_inbox_ids.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ReadingTheInboxResource_NeverAcknowledges()
    {
        await RegisterAsync();
        await SendReviewRequestToSessionPeerAsync("read me, but do not ack");

        var first = await _client.ReadResourceAsync(PeerResources.InboxUri);
        Assert.Contains("inbox_id", TextOf(first));

        // Still there on the second read — reading staged no acknowledgement.
        var second = await _client.ReadResourceAsync(PeerResources.InboxUri);
        Assert.Single(InboxIds(JsonNode.Parse(TextOf(second))!));

        // And wait still delivers it, until it is explicitly acked.
        Assert.Single(InboxIds(await InboxAsync()));
    }

    [Fact]
    public async Task Unsubscribe_StopsDelivery()
    {
        await RegisterAsync();
        var pad = await CreatePadAsync("pad going quiet");
        var uri = PeerResources.ChannelUri(pad);

        await SubscribeAsync(uri);
        await AppendAsync(pad, "while subscribed");
        await DispatchBrokerEventsAsync();
        await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);

        await _client.UnsubscribeFromResourceAsync(uri);
        _notifications.Clear();

        await AppendAsync(pad, "after unsubscribing");
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);

        Assert.Empty(_notifications);
    }

    [Fact]
    public async Task ThreeChangesToOneResourceInOnePollBatch_CoalesceToOneNotification()
    {
        await RegisterAsync();
        var pad = await CreatePadAsync("busy pad");
        var uri = PeerResources.ChannelUri(pad);

        await SubscribeAsync(uri);
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

        await SubscribeAsync(uri);
        _notifications.Clear();

        await AppendAsync(padA, "authored by the subscriber, addressed to nobody");

        // Nothing in this test drives the dispatch; ChannelNotifier polls every 5s.
        var update = await WaitForNotificationAsync(
            NotificationMethods.ResourceUpdatedNotification, TimeSpan.FromSeconds(25));

        Assert.Equal(uri, update["uri"]!.GetValue<string>());
    }


    // -------------------------------------------------------------------------
    // The notifier is what POPULATES the roster the wait ticker reads. Nothing else
    // writes SessionState.PeerRoster, so if this refresh regresses, peer progress
    // silently never appears on a keepalive and every ticker unit test still passes
    // (they seed the roster directly). This is the link between the two halves.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ChannelNotifier_RefreshesThePeerRoster_WhenAPeerReportsProgress()
    {
        await RegisterAsync();
        // Replaced by the notifier under test.
        await _state.StopServicesAsync();
        _state.ResetCursorForRegistration(0);

        using var client = new BrokerClient(new HttpClient(_app.GetTestServer().CreateHandler())
        {
            BaseAddress = new Uri("http://localhost"),
        });
        await using var notifier = new ChannelNotifier(client, _state);
        notifier.Start();

        // Same repo as the test session: the refresh is scoped to state.RepoRoot.
        var worker = await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = "worker-machine", RepoRoot = "/repo", Branch = "main",
        });
        await _broker.HeartbeatAsync(worker.PeerId, new HeartbeatRequest
        {
            Summary = "running the suite", ProgressCurrent = 340, ProgressTotal = 1200,
        });

        // 5s poll cadence, same as ChannelNotifier_SurvivesABrokerStall.
        await WaitForAsync(
            () => _state.PeerRoster.Any(p => p.PeerId == worker.PeerId && p.ProgressCurrent == 340),
            TimeSpan.FromSeconds(30),
            "the notifier never refreshed the peer roster after a progress report");

        var seen = _state.PeerRoster.First(p => p.PeerId == worker.PeerId);
        Assert.Equal(1200, seen.ProgressTotal);
        Assert.NotNull(seen.ProgressUpdatedAt);
    }


    // -------------------------------------------------------------------------
    // The roster refresh must never cost a hint. It runs in the same loop body as
    // resources/updated dispatch, AFTER the cursor has already advanced — so if it
    // throws before dispatch, that batch's hints are gone permanently and the
    // cursor has moved past them. An HttpClient timeout arrives as
    // TaskCanceledException with our token untouched, which a
    // `is not OperationCanceledException` filter would let escape, so this drives
    // a hard failure through the same path.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ChannelNotifier_StillDispatchesHints_WhenTheRosterRefreshFails()
    {
        await RegisterAsync();
        var channel = await CreatePadAsync("refresh-failure");
        var channelUri = PeerResources.ChannelUri(channel);
        await SubscribeAsync(channelUri);
        await _state.StopServicesAsync();
        _state.ResetCursorForRegistration(0);
        _notifications.Clear();

        var failing = new FailPathHandler(_app.GetTestServer().CreateHandler(), "/peers");
        using var client = new BrokerClient(new HttpClient(failing) { BaseAddress = new Uri("http://localhost") });
        await using var notifier = new ChannelNotifier(client, _state);
        notifier.Start();

        // Registering into a repo that already has a channel emits BOTH peer.registered — which
        // triggers the roster refresh, and so the failure under test — AND a join announcement on
        // that channel, which must still produce its hint despite the refresh throwing.
        await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = "worker-machine", RepoRoot = "/repo", Branch = "main",
        });

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification, TimeSpan.FromSeconds(30));
        Assert.Equal(channelUri, update["uri"]!.GetValue<string>());

        // The refresh really did fail — otherwise this test would pass even with the ordering wrong.
        Assert.True(failing.Attempts > 0, "the roster refresh never ran, so this proves nothing");
        Assert.Empty(_state.PeerRoster);
    }

    /// <summary>Fails every GET to exactly <paramref name="path"/> the way a broker hiccup does, and
    /// forwards everything else. Raises TaskCanceledException with the caller's token NOT cancelled,
    /// which is how HttpClient.Timeout presents.</summary>
    private sealed class FailPathHandler(HttpMessageHandler inner, string path) : DelegatingHandler(inner)
    {
        public int Attempts => Volatile.Read(ref _attempts);
        private int _attempts;

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Get && request.RequestUri?.AbsolutePath == path)
            {
                Interlocked.Increment(ref _attempts);
                return Task.FromException<HttpResponseMessage>(new TaskCanceledException("simulated request timeout"));
            }

            return base.SendAsync(request, cancellationToken);
        }
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
    // peer_wait's progressToken reaches the client over the wire.
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
    // The regression that started all this: `wait` with NO arguments must come back with a result
    // inside the request budget a default-configured client gives it. An MCP client's deadline is
    // one-shot — omp resolves 30s and never extends it on the 5s progress keepalives — so an
    // advertised window above that budget turned every idle wait into a client-side transport error
    // the caller could not tell apart from peer silence.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task PeerWait_DefaultWindow_ReturnsTimedOutInsideAThirtySecondClientBudget()
    {
        await RegisterAsync();

        // The client deadline, modelled the way omp models it: fixed at the call, never extended.
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ticks = new ProgressCollector();
        var startedAt = Stopwatch.StartNew();

        // Nothing is addressed to this peer, so only the default window can end the call.
        var result = await _client.CallToolAsync("wait", progress: ticks, cancellationToken: budget.Token);
        startedAt.Stop();

        Assert.True(result.IsError is not true, TextOf(result));
        Assert.True(JsonNode.Parse(TextOf(result))!["timed_out"]!.GetValue<bool>(),
            $"the default window must report timed_out, not an error: {TextOf(result)}");
        Assert.True(startedAt.Elapsed < TimeSpan.FromSeconds(30),
            $"the default window took {startedAt.Elapsed.TotalSeconds:F1}s, past a 30s client budget");
        // Keepalives still go out inside the shortened window, so a client that DOES reset on progress
        // can still park for the full 120s maximum.
        Assert.False(ticks.Values.IsEmpty, "no progress keepalive reached the client");
    }

    // -------------------------------------------------------------------------
    // A client that asks peer_wait for 120s and changes its mind gets: work stopped,
    // and NO response frame on the wire.
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

        // The SDK client never surfaces a server result for a request it cancelled.
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

        await SubscribeAsync(PeerResources.ChannelUri(history), PeerResources.ChannelUri(fresh));
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
    // re-registering must not disturb a subscription nobody cancelled. StopServicesAsync
    // is what re-register calls, and it deliberately does not touch subscriptions.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ReRegistering_LeavesSubscriptionsNobodyCancelledAlone()
    {
        await RegisterAsync();
        var pad = await CreatePadAsync("pad A");
        var uri = PeerResources.ChannelUri(pad);

        await SubscribeAsync(uri);

        await RegisterAsync();
        _notifications.Clear();

        Assert.True(_state.Subscriptions!.IsSubscribedForTests(uri),
            "the re-register dropped a subscription nobody cancelled");

        // Still registered is not the same as still delivering, so prove delivery too.
        await AppendAsync(pad, "after the re-register");
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(uri, update["uri"]!.GetValue<string>());
    }

    // -------------------------------------------------------------------------
    // peers://peers actually fires. Several other resources/updated assertions in this
    // file are on channel or inbox URIs, so the peer arm of ResourceUrisFor could be
    // deleted with the suite still green — and peers://peers would go permanently
    // silent, which is the whole point of the peer.updated event behind it.
    // -------------------------------------------------------------------------
    /// <summary>Registers a peer and has it ADDRESS this session in <paramref name="channelId"/>.
    /// The mention is what makes them correspondents — the real protocol always carries `to`, and mere
    /// co-authorship would not qualify (a fresh peer auto-announces into every channel in its repo).</summary>
    private async Task<string> CorrespondentAsync(string channelId, string machine)
    {
        var partner = await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = machine, RepoRoot = "/repo", Branch = "main",
        });
        await _broker.SendMessageAsync(channelId, new SendMessageRequest
        {
            AuthorPeerId = partner.PeerId, Content = "reviewer here", To = [_peerId!], Quiet = false,
        });
        return partner.PeerId;
    }

    // -------------------------------------------------------------------------
    // A roster hint is only OUR business when it is somebody we correspond with.
    // Unscoped, every presence call anywhere on the machine woke every subscribed
    // session — and since progress reporting rides peer.updated, that became
    // constant churn from unrelated pairs working in the same repo.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task PeerChange_NotifiesOnlyPeersWeCorrespondWith()
    {
        await RegisterAsync();
        var channel = await CreatePadAsync("roster-scope");
        await SubscribeAsync(PeerResources.PeersUri);
        await AppendAsync(channel, "driver here");

        var partner = await CorrespondentAsync(channel, "partner-machine");
        // Same repo, never addressed us: exactly the unrelated pair whose churn used to wake us.
        var stranger = (await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = "stranger-machine", RepoRoot = "/repo", Branch = "main",
        })).PeerId;

        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);
        _notifications.Clear();

        await _broker.HeartbeatAsync(stranger, new HeartbeatRequest { Summary = "unrelated work" });
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);

        Assert.DoesNotContain(_notifications, n => n.Method == NotificationMethods.ResourceUpdatedNotification);

        // Same stream, a peer that did address us: the silence above was the scope filter, not a
        // dead subscription.
        await _broker.HeartbeatAsync(partner, new HeartbeatRequest { Summary = "now reviewing" });
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(PeerResources.PeersUri, update["uri"]!.GetValue<string>());
    }

    // -------------------------------------------------------------------------
    // A stranger STARTING a session in our repo must not wake us either.
    // peer.registered carries recipient_peer_ids, which is the durable INBOX
    // audience (every peer in the repo) — reusing it as a roster hint would be
    // repo scoping wearing a disguise. The inbox still delivers it.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task PeerRegistration_DoesNotPushARosterHint()
    {
        await RegisterAsync();
        await SubscribeAsync(PeerResources.PeersUri);
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);
        _notifications.Clear();

        await _broker.RegisterAsync(new RegisterPeerRequest
        {
            Tool = "codex", MachineId = "newcomer-machine", RepoRoot = "/repo", Branch = "main",
        });
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);

        Assert.DoesNotContain(_notifications, n => n.Method == NotificationMethods.ResourceUpdatedNotification);
    }

    // -------------------------------------------------------------------------
    // Progress is PULL-only: read from the roster, or off a wait keepalive. It must
    // never wake a subscriber, even one we correspond with — a driver reporting every
    // few seconds would otherwise notify its reviewer on every tick. The event still
    // exists (the notifier's roster cache and the CLI both read it); it carries no
    // audience.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Progress_AloneNotifiesNobody()
    {
        await RegisterAsync();
        var channel = await CreatePadAsync("progress-quiet");
        await SubscribeAsync(PeerResources.PeersUri);
        await AppendAsync(channel, "driver here");

        var partner = await CorrespondentAsync(channel, "partner-machine");

        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);
        _notifications.Clear();

        await _broker.HeartbeatAsync(partner, new HeartbeatRequest { ProgressCurrent = 4, ProgressTotal = 9 });
        await DispatchBrokerEventsAsync();
        await Task.Delay(Settle);

        Assert.DoesNotContain(_notifications, n => n.Method == NotificationMethods.ResourceUpdatedNotification);

        // Same peer, a status change: proves progress was what stayed quiet, not the partner.
        await _broker.HeartbeatAsync(partner, new HeartbeatRequest { Summary = "done thinking" });
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(PeerResources.PeersUri, update["uri"]!.GetValue<string>());
    }

    // -------------------------------------------------------------------------
    // QUIET WHISPERS ARE PARTICIPANT-SCOPED on the CHANNEL arm too. A quiet message
    // addressed to somebody else must not generate the channel's hint on this
    // session's stream. The loud control proves the stream is alive either way.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task QuietWhisperForSomebodyElse_NotifiesNothing_ButALoudMessageStillDoes()
    {
        await RegisterAsync();
        var channel = await CreatePadAsync("whispers");
        var uri = PeerResources.ChannelUri(channel);

        await SubscribeAsync(uri);
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
    }

    // -------------------------------------------------------------------------
    // ...and a whisper this session IS mentioned in does notify the channel arm.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task QuietWhisperMentioningUs_NotifiesOurChannelSubscription()
    {
        await RegisterAsync();
        var channel = await CreatePadAsync("whispers-for-us");
        var uri = PeerResources.ChannelUri(channel);

        await SubscribeAsync(uri);
        _notifications.Clear();

        await WhisperFromOtherPeerAsync(channel, "just for you", _peerId!);
        await DispatchBrokerEventsAsync();

        var update = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(uri, update["uri"]!.GetValue<string>());
    }

    // -------------------------------------------------------------------------
    // THE REVISION THAT ACTUALLY SHIPS. The server pins ProtocolVersion to
    // 2025-11-25 — the revision every shipping client negotiates — so this session
    // is not a special case, it is EVERY session. On it, resources/subscribe must
    // WORK (that is the mechanism peers serves now) and the plain resource surface
    // must keep working.
    // -------------------------------------------------------------------------
    [Fact]
    public async Task OnThePinnedRevision_SubscribesSucceed_AndResourceReadsStillWork()
    {
        await using var legacy = await StartLegacySessionAsync();
        Assert.Equal(November2025ProtocolVersion, legacy.Client.NegotiatedProtocolVersion);

        // The always-on half: list, templates and read carry no revision gating.
        Assert.Contains(await legacy.Client.ListResourcesAsync(), r => r.Uri == PeerResources.PeersUri);
        Assert.Contains(await legacy.Client.ListResourceTemplatesAsync(),
            t => t.UriTemplate == PeerResources.ChannelUriTemplate);
        Assert.Contains(await legacy.Client.ListResourcesAsync(), r => r.Uri == PeerResources.InboxUri);

        var peers = await legacy.Client.ReadResourceAsync(PeerResources.PeersUri);
        Assert.Contains("peer_id", TextOf(peers));

        var padId = JsonNode.Parse(TextOf(await legacy.Client.CallToolAsync(
            "channel_create",
            new Dictionary<string, object?> { ["name"] = $"legacy-read-{Guid.NewGuid():N}" })))!["channel_id"]!.GetValue<string>();
        var channel = await legacy.Client.ReadResourceAsync(PeerResources.ChannelUri(padId));
        Assert.Contains("messages", TextOf(channel));

        // resources/subscribe: served, on the revision everyone negotiates.
        await legacy.Client.SubscribeToResourceAsync(PeerResources.PeersUri);
        await legacy.Client.SubscribeToResourceAsync(PeerResources.InboxUri);
        await legacy.Client.UnsubscribeFromResourceAsync(PeerResources.PeersUri);

        // subscriptions/listen: the 2026-only method is GONE — refused, not served, so an SDK bump
        // cannot quietly reintroduce it.
        await Assert.ThrowsAnyAsync<McpException>(() =>
            legacy.Client.SendRequestAsync<SubscriptionsListenRequestParams, EmptyResult>(
                RequestMethods.SubscriptionsListen,
                new SubscriptionsListenRequestParams { Notifications = new() { ResourceSubscriptions = [PeerResources.PeersUri] } },
                McpJsonUtilities.DefaultOptions,
                new RequestId("legacy-listen"),
                CancellationToken.None).AsTask());
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
        collection.AddSingleton(new Peers.Mcp.Reviews.ReviewStorageOptions(Path.Combine(Path.GetTempPath(), "peers-review-sub-" + Guid.NewGuid().ToString("N"))));
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
    }

    [Fact]
    public async Task ToolFailures_AreMarkedAsMcpErrors()
    {
        var missingLookup = await _client.CallToolAsync("artifact_get");
        Assert.True(missingLookup.IsError is true, TextOf(missingLookup));
        Assert.Contains("Missing parameters", TextOf(missingLookup));

        var missingChannel = await _client.CallToolAsync(
            "read",
            new Dictionary<string, object?> { ["channel"] = $"missing-{Guid.NewGuid():N}" });
        Assert.True(missingChannel.IsError is true, TextOf(missingChannel));
        Assert.Contains("not found", TextOf(missingChannel));
    }

    // End-to-end wiring for the claim surface: tool → HTTP → service → SQLite → back. The overlap and
    // liveness rules are pinned in ClaimServiceTests; what can only break here is the plumbing —
    // the peer's repo scope reaching the service, and the roster of held paths coming back.
    //
    // RegisterAsync first, like every other test that needs a live session: the shared session state
    // carries whichever peer a previously-run test left behind, and a claim by a stale peer id is a
    // legitimate NotFound the harness would report as a JSON parse error four lines later.
    [Fact]
    public async Task ClaimTools_GrantAPath_ThenListAndReleaseIt()
    {
        await RegisterAsync();
        var path = "src/Peers.Mcp/Tools/ClaimTools.cs";
        var claimArgs = new Dictionary<string, object?> { ["paths"] = new[] { path } };

        var granted = await CallOkAsync("claim", claimArgs);
        Assert.Empty(granted["conflicts"]!.AsArray());
        Assert.Equal(path, Str(granted["granted"]![0]!["path"]));

        var listed = await CallOkAsync("claims");
        Assert.Contains(listed["claims"]!.AsArray(), claim => Str(claim!["path"]) == path);

        // Re-claiming my own path renews rather than conflicting, and release empties the map.
        Assert.Empty((await CallOkAsync("claim", claimArgs))["conflicts"]!.AsArray());

        var released = await CallOkAsync("release");
        Assert.Equal(1, released["released"]!.GetValue<int>());
        Assert.Empty((await CallOkAsync("claims"))["claims"]!.AsArray());
    }

    // The retry contract on the wire: same key, same message, and the second call says so.
    [Fact]
    public async Task SendWithAnIdempotencyKey_IsPostedOnce()
    {
        await RegisterAsync();
        var channel = $"idem-{Guid.NewGuid():N}";
        await CallOkAsync("channel_create", new Dictionary<string, object?> { ["name"] = channel });

        var args = new Dictionary<string, object?>
        {
            ["channel"] = channel,
            ["content"] = "PLAN round 1",
            ["idempotency_key"] = "round-1",
        };
        var first = await CallOkAsync("send", args);
        var retry = await CallOkAsync("send", args);

        Assert.False(first["deduplicated"]!.GetValue<bool>());
        Assert.True(retry["deduplicated"]!.GetValue<bool>());
        Assert.Equal(Str(first["message_id"]), Str(retry["message_id"]));

        var read = await CallOkAsync("read", new Dictionary<string, object?> { ["channel"] = channel });
        Assert.Single(read["messages"]!.AsArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RemovalHints_NotifyOnlyWhisperParticipants(bool addressedToUs)
    {
        await RegisterAsync();
        var author = await _broker.RegisterAsync(new RegisterPeerRequest
            { Tool = "codex", MachineId = "removal-author", RepoRoot = "/other-repo" });
        var other = await _broker.RegisterAsync(new RegisterPeerRequest
            { Tool = "codex", MachineId = "removal-other", RepoRoot = "/other-repo" });
        var channel = await CreatePadAsync("removal-hints");
        await SubscribeAsync(PeerResources.ChannelUri(channel), PeerResources.InboxUri);
        var message = (await _broker.SendMessageAsync(channel, new SendMessageRequest
        {
            AuthorPeerId = author.PeerId, Content = "whisper to remove", Quiet = true,
            To = [addressedToUs ? _peerId! : other.PeerId],
        }))!;
        await DispatchBrokerEventsAsync();
        if (addressedToUs)
            await WaitForAsync(() => _notifications.Count == 2, TimeSpan.FromSeconds(10), "initial whisper hints missing");
        else
            await Task.Delay(Settle);
        _notifications.Clear();

        await _broker.RemoveMessagesAsync(channel, new RemoveMessagesRequest { MessageIds = [message.MessageId] });
        await DispatchBrokerEventsAsync();
        if (addressedToUs)
        {
            await WaitForAsync(() => _notifications.Count == 2, TimeSpan.FromSeconds(10), "removal hints missing");
            Assert.Contains(_notifications, n => n.Params?["uri"]?.GetValue<string>() == PeerResources.ChannelUri(channel));
            Assert.Contains(_notifications, n => n.Params?["uri"]?.GetValue<string>() == PeerResources.InboxUri);
            Assert.DoesNotContain((await _broker.GetInboxAsync(_peerId!))!.Items,
                item => item.Event.Type == Peers.Shared.Models.EventTypes.ChannelMessageAdded);
        }
        else
        {
            await Task.Delay(Settle);
            Assert.Empty(_notifications);
        }
    }

    [Fact]
    public async Task DeletionTools_UseRepoScope_NotifyClear_AndPreserveReadCursor()
    {
        await RegisterAsync();
        // Explicit registration bypasses git identity derivation in this harness.
        _state.RepoRoot = "/repo";
        var name = $"delete-{Guid.NewGuid():N}";
        var global = await _broker.CreateChannelAsync(new CreateChannelRequest
            { Name = name, CreatedByPeerId = _peerId! });
        await AppendAsync(global.ChannelId, "global survives");
        var channel = await CallOkAsync("channel_create", new() { ["name"] = name });
        var id = Str(channel["channel_id"])!;
        var first = await CallOkAsync("send", new() { ["channel"] = name, ["content"] = "first" });
        var last = await CallOkAsync("send", new() { ["channel"] = name, ["content"] = "last" });
        var removed = await CallOkAsync("messages_remove", new()
            { ["channel"] = name, ["message_ids"] = new[] { Str(last["message_id"]) } });
        Assert.Equal(1, removed["deleted_messages"]!.GetValue<int>());
        var read = await CallOkAsync("read", new() { ["channel"] = name });
        Assert.Equal(Str(first["message_id"]), Str(Assert.Single(read["messages"]!.AsArray())!["message_id"]));

        await DispatchBrokerEventsAsync();
        await SubscribeAsync(PeerResources.ChannelUri(id));
        _notifications.Clear();
        var cleared = await CallOkAsync("channel_clear", new() { ["channel"] = name });
        Assert.Equal(1, cleared["deleted_messages"]!.GetValue<int>());
        Assert.Equal(name, Str(cleared["name"]));
        Assert.Equal("/repo", Str(cleared["repo_root"]));
        await DispatchBrokerEventsAsync();
        var hint = await WaitForNotificationAsync(NotificationMethods.ResourceUpdatedNotification);
        Assert.Equal(PeerResources.ChannelUri(id), hint["uri"]!.GetValue<string>());
        Assert.Equal("global survives", Assert.Single((await _broker.ReadChannelMessagesAsync(global.ChannelId, _peerId))!.Messages).Content);
        Assert.Empty((await CallOkAsync("read", new() { ["channel"] = name }))["messages"]!.AsArray());

        var next = await CallOkAsync("send", new() { ["channel"] = name, ["content"] = "after clear" });
        Assert.True(next["seq"]!.GetValue<long>() > last["seq"]!.GetValue<long>());
        var page = await CallOkAsync("read", new() { ["channel"] = name, ["since_seq"] = last["seq"]!.GetValue<long>() });
        Assert.Equal(Str(next["message_id"]), Str(Assert.Single(page["messages"]!.AsArray())!["message_id"]));
        var bad = await _client.CallToolAsync("messages_remove", new Dictionary<string, object?>
            { ["channel"] = name, ["message_ids"] = Array.Empty<string>() });
        Assert.True(bad.IsError is true);

        var foreign = await _broker.CreateChannelAsync(new CreateChannelRequest
            { Name = $"{name}-foreign", RepoRoot = "/other-repo", CreatedByPeerId = _peerId! });
        await AppendAsync(foreign.ChannelId, "unrelated history");
        var wrongScope = await _client.CallToolAsync("channel_clear", new Dictionary<string, object?>
            { ["channel"] = $"{name}-foreign" });
        Assert.True(wrongScope.IsError is true);
        Assert.Equal("unrelated history", Assert.Single((await _broker.ReadChannelMessagesAsync(foreign.ChannelId, _peerId))!.Messages).Content);
    }

    // Observed 2026-10-22: a reviewer session in one repo got "Channel ... not found" for the driver's
    // channel scoped to an API worktree. Names stay scoped; an explicit channel_id reaches any repo.
    [Fact]
    public async Task ChannelId_ReachesAnotherReposChannel_WhileNamesStayScoped()
    {
        await RegisterAsync();
        _state.RepoRoot = "/repo";
        var name = $"by-id-{Guid.NewGuid():N}";
        var foreign = await _broker.CreateChannelAsync(new CreateChannelRequest
            { Name = name, RepoRoot = "/api-worktree", CreatedByPeerId = _peerId! });

        var byName = await _client.CallToolAsync("send", new Dictionary<string, object?>
            { ["channel"] = name, ["content"] = "lost" });
        Assert.True(byName.IsError is true);
        Assert.Contains($"Channel '{name}' not found.", TextOf(byName));

        var sent = await CallOkAsync("send", new() { ["channel_id"] = foreign.ChannelId, ["content"] = "reached" });
        Assert.Equal(foreign.ChannelId, Str(sent["channel_id"]));
        var read = await CallOkAsync("read", new() { ["channel_id"] = foreign.ChannelId });
        Assert.Equal("reached", Str(Assert.Single(read["messages"]!.AsArray())!["content"]));

        // Name lookup is unchanged: the same name now resolves to this repo's own channel.
        var own = await CallOkAsync("channel_create", new() { ["name"] = name });
        var local = await CallOkAsync("send", new() { ["channel"] = name, ["content"] = "local" });
        Assert.Equal(Str(own["channel_id"]), Str(local["channel_id"]));
        Assert.Equal("local", Str(Assert.Single((await CallOkAsync("read", new() { ["channel"] = name }))["messages"]!.AsArray())!["content"]));
        Assert.Single((await _broker.ReadChannelMessagesAsync(foreign.ChannelId, _peerId))!.Messages);

        foreach (var (tool, args) in new (string, Dictionary<string, object?>)[]
        {
            ("send", new() { ["content"] = "neither" }),
            ("send", new() { ["channel"] = name, ["channel_id"] = foreign.ChannelId, ["content"] = "both" }),
            ("read", new()),
            ("read", new() { ["channel"] = name, ["channel_id"] = foreign.ChannelId }),
        })
        {
            var rejected = await _client.CallToolAsync(tool, args);
            Assert.True(rejected.IsError is true, $"{tool} accepted {string.Join(",", args.Keys)}");
            Assert.Contains("Pass exactly one of `channel` (name) or `channel_id`.", TextOf(rejected));
        }
        Assert.Single((await _broker.ReadChannelMessagesAsync(foreign.ChannelId, _peerId))!.Messages);
    }

    // An id reaches any repo, so deleting by id must also name the channel: confirm_channel is checked
    // against the name stored for that id. Here no channel of that name exists in this session's repo,
    // so a confirmation resolved locally could never match.
    [Fact]
    public async Task DeletionById_RequiresTheStoredName_AndDeletesNothingWithoutIt()
    {
        await RegisterAsync();
        _state.RepoRoot = "/repo";
        var name = $"confirm-{Guid.NewGuid():N}";
        var foreign = await _broker.CreateChannelAsync(new CreateChannelRequest
            { Name = name, RepoRoot = "/api-worktree", CreatedByPeerId = _peerId! });
        async Task<string> PostAsync(string content) => (await _broker.SendMessageAsync(foreign.ChannelId,
            new SendMessageRequest { AuthorPeerId = _peerId!, Content = content }))!.MessageId;
        var first = await PostAsync("first");
        await PostAsync("second");
        var actual = $"channel_id '{foreign.ChannelId}' is '{name}' in repo_root '/api-worktree'";

        foreach (var (tool, args, expected) in new (string, Dictionary<string, object?>, string)[]
        {
            ("channel_clear", new() { ["channel_id"] = foreign.ChannelId }, "requires confirm_channel"),
            ("channel_clear", new() { ["channel_id"] = foreign.ChannelId, ["confirm_channel"] = name.ToUpperInvariant() },
                $"confirm_channel '{name.ToUpperInvariant()}' does not match"),
            ("messages_remove", new() { ["channel_id"] = foreign.ChannelId, ["message_ids"] = new[] { first } },
                "requires confirm_channel"),
            ("messages_remove", new() { ["channel_id"] = foreign.ChannelId, ["message_ids"] = new[] { first }, ["confirm_channel"] = "wrong" },
                "confirm_channel 'wrong' does not match"),
        })
        {
            var rejected = await _client.CallToolAsync(tool, args);
            Assert.True(rejected.IsError is true, $"{tool} accepted {string.Join(",", args.Keys)}");
            Assert.Contains(expected, TextOf(rejected));
            Assert.Contains($"{actual}. Nothing was deleted.", TextOf(rejected));
        }
        Assert.Equal(new[] { "first", "second" },
            (await _broker.ReadChannelMessagesAsync(foreign.ChannelId, _peerId))!.Messages.Select(m => m.Content));

        var removed = await CallOkAsync("messages_remove", new()
            { ["channel_id"] = foreign.ChannelId, ["confirm_channel"] = name, ["message_ids"] = new[] { first } });
        Assert.Equal(foreign.ChannelId, Str(removed["channel_id"]));
        Assert.Equal(name, Str(removed["name"]));
        Assert.Equal("/api-worktree", Str(removed["repo_root"]));
        Assert.Equal(1, removed["deleted_messages"]!.GetValue<int>());
        Assert.Equal("second", Assert.Single((await _broker.ReadChannelMessagesAsync(foreign.ChannelId, _peerId))!.Messages).Content);

        var cleared = await CallOkAsync("channel_clear", new()
            { ["channel_id"] = foreign.ChannelId, ["confirm_channel"] = name });
        Assert.Equal(foreign.ChannelId, Str(cleared["channel_id"]));
        Assert.Equal(name, Str(cleared["name"]));
        Assert.Equal("/api-worktree", Str(cleared["repo_root"]));
        Assert.Equal(1, cleared["deleted_messages"]!.GetValue<int>());
        Assert.Empty((await _broker.ReadChannelMessagesAsync(foreign.ChannelId, _peerId))!.Messages);
    }

    // By name, confirm_channel is optional, but a given one must equal `channel`: a contradiction means
    // the caller is unsure which channel it means, so nothing is deleted. A blank one counts as absent.
    [Fact]
    public async Task DeletionByName_RejectsAMismatchedConfirmation_AndDeletesOnAMatch()
    {
        await RegisterAsync();
        _state.RepoRoot = "/repo";
        var name = $"confirm-name-{Guid.NewGuid():N}";
        var channel = await _broker.CreateChannelAsync(new CreateChannelRequest
            { Name = name, RepoRoot = "/repo", CreatedByPeerId = _peerId! });
        async Task<string> PostAsync(string content) => (await _broker.SendMessageAsync(channel.ChannelId,
            new SendMessageRequest { AuthorPeerId = _peerId!, Content = content }))!.MessageId;
        var first = await PostAsync("first");
        var second = await PostAsync("second");
        await PostAsync("third");

        foreach (var (tool, args, wrong) in new (string, Dictionary<string, object?>, string)[]
        {
            ("channel_clear", new() { ["channel"] = name, ["confirm_channel"] = name.ToUpperInvariant() }, name.ToUpperInvariant()),
            ("messages_remove", new() { ["channel"] = name, ["confirm_channel"] = "wrong", ["message_ids"] = new[] { first } }, "wrong"),
        })
        {
            var rejected = await _client.CallToolAsync(tool, args);
            Assert.True(rejected.IsError is true, $"{tool} accepted confirm_channel '{wrong}'");
            Assert.Contains($"confirm_channel '{wrong}' does not match channel '{name}'. Nothing was deleted.", TextOf(rejected));
        }
        Assert.Equal(new[] { "first", "second", "third" },
            (await _broker.ReadChannelMessagesAsync(channel.ChannelId, _peerId))!.Messages.Select(m => m.Content));

        var blank = await CallOkAsync("messages_remove", new()
            { ["channel"] = name, ["confirm_channel"] = " ", ["message_ids"] = new[] { first } });
        Assert.Equal(1, blank["deleted_messages"]!.GetValue<int>());
        var removed = await CallOkAsync("messages_remove", new()
            { ["channel"] = name, ["confirm_channel"] = name, ["message_ids"] = new[] { second } });
        Assert.Equal(1, removed["deleted_messages"]!.GetValue<int>());
        Assert.Equal("third", Assert.Single((await _broker.ReadChannelMessagesAsync(channel.ChannelId, _peerId))!.Messages).Content);

        var cleared = await CallOkAsync("channel_clear", new() { ["channel"] = name, ["confirm_channel"] = name });
        Assert.Equal(channel.ChannelId, Str(cleared["channel_id"]));
        Assert.Equal(1, cleared["deleted_messages"]!.GetValue<int>());
        Assert.Empty((await _broker.ReadChannelMessagesAsync(channel.ChannelId, _peerId))!.Messages);
    }

    /// <summary>Calls a tool, fails with the server's own message when it errored, and parses the result.</summary>
    private async Task<JsonNode> CallOkAsync(string tool, Dictionary<string, object?>? args = null)
    {
        var result = await _client.CallToolAsync(tool, args);
        Assert.True(result.IsError is not true, $"{tool} failed: {TextOf(result)}");
        return JsonNode.Parse(TextOf(result))!;
    }

    // A stdio server serves the assembly it loaded at launch for the whole client session, so "which
    // build am I talking to" has to be answerable from the wire. A literal version string cannot answer
    // it: five stale servers advertising deleted tools all reported the same "2.0.0".
    [Fact]
    public void InitializeResult_ReportsTheBuildIdentity()
    {
        var stamped = typeof(ChannelTools).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

        Assert.Equal(stamped, _client.ServerInfo.Version);
        Assert.Contains('+', stamped!);
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

    private async Task SubscribeAsync(params string[] uris)
    {
        foreach (var uri in uris)
            await _client.SubscribeToResourceAsync(uri);
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

    private static string TextOf(CallToolResult result) =>
        Assert.IsType<TextContentBlock>(Assert.Single(result.Content)).Text;

    private static string TextOf(ReadResourceResult result) =>
        Assert.IsType<TextResourceContents>(Assert.Single(result.Contents)).Text;
}
