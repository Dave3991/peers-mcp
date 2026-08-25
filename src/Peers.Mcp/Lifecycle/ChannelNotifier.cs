using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Peers.Mcp.Lifecycle;

/// <summary>
/// The session-side engine of resource push: polls the broker every 5 seconds and feeds each batch of
/// events to the subscription registry, which coalesces them into resources/updated hints per subscribed
/// URI. Directed delivery does NOT run through here — the broker writes inbox rows durably at emit
/// time, and wait drains them — so this loop is purely a best-effort freshness hint and its failure
/// modes are deliberately boring.
/// </summary>
public sealed class ChannelNotifier : IDisposable, IAsyncDisposable
{
    private readonly BrokerClient _client;
    private readonly SessionState _state;
    private readonly ILogger<ChannelNotifier> _logger;
    private readonly CancellationTokenSource _cts = new();
    private long _channelCursor;
    private long _resourceBaseline;
    private Task? _task;
    private int _disposeStarted;

    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(5);

    public ChannelNotifier(BrokerClient client, SessionState state, ILogger<ChannelNotifier>? logger = null)
    {
        _client = client;
        _state = state;
        _logger = logger ?? NullLogger<ChannelNotifier>.Instance;
    }

    public void Start()
    {
        if (Volatile.Read(ref _disposeStarted) != 0)
            throw new ObjectDisposedException(nameof(ChannelNotifier));
        if (_task is not null)
            throw new InvalidOperationException("Channel notifier already started.");

        _channelCursor = _state.LastCursor;
        // Resource dispatch starts at the broker head as of registration (the id of this session's own
        // peer.registered event), NOT at _channelCursor. A fresh peer's cursor is 0, so sharing it would
        // replay the whole retention window of every channel's history as resources/updated the moment a
        // subscription opened. Separate baseline, so the cursor semantics stay untouched.
        _resourceBaseline = _state.ResourceBaseline;
        _task = RunAsync(_cts.Token);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(PollInterval, ct);

                if (_state.PeerId is null)
                    continue;

                var resp = await _client.PollAsync(_state.PeerId, _channelCursor, 50, ct);
                if (resp.Events.Count == 0)
                    continue;

                var fromCursor = _channelCursor;
                _channelCursor = resp.Cursor;
                _state.AdvanceCursor(_channelCursor);

                try
                {
                    var subscriptions = _state.Subscriptions;
                    if (subscriptions is not null)
                        await subscriptions.NotifyEventsAsync(
                            resp.Events.Where(evt => evt.EventId > _resourceBaseline), ct);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "Resource-updated dispatch failed for batch ({FromCursor} -> {ToCursor})",
                        fromCursor, _channelCursor);
                }

                // Peer events are exactly the ones that make the cached roster wrong — including the
                // peer.updated a progress report emits — so the refresh rides the poll we already did
                // instead of the wait ticker doing its own.
                //
                // STRICTLY AFTER dispatch, and it swallows everything short of our own shutdown. The
                // cursor has already advanced by this point, so anything that throws between here and
                // NotifyEventsAsync would drop that batch's hints forever. And the filter is
                // !ct.IsCancellationRequested, not `is not OperationCanceledException`: an HttpClient
                // timeout arrives as TaskCanceledException with OUR token untouched (same trap the
                // outer handler documents), which the narrower filter would let escape. A failed
                // refresh just keeps the previous snapshot — slightly stale peer progress on a
                // keepalive is never worth a dropped hint.
                if (resp.Events.Any(evt => evt.Type.StartsWith("peer.", StringComparison.Ordinal)))
                {
                    try
                    {
                        // Unscoped, because the `peers` tool is unscoped: scoping this to RepoRoot
                        // would hide a cross-repo partner's progress from the keepalive while the
                        // roster the agent can read still shows it — two surfaces disagreeing about
                        // who is working. A driver/reviewer pair is not required to share a repo.
                        _state.PeerRoster = await _client.ListPeersAsync(cancellationToken: ct);
                    }
                    catch (Exception ex) when (!ct.IsCancellationRequested)
                    {
                        _logger.LogDebug(ex, "Peer roster refresh failed; keeping the previous snapshot");
                    }
                }
            }
            // ONLY our own shutdown ends the loop. An unfiltered catch here cannot tell that from an
            // HttpClient timeout, which surfaces as TaskCanceledException — an OperationCanceledException —
            // so one unresponsive-broker stall must not kill resource dispatch for the life of the session.
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            // Everything else, including that timeout, is transient: logged and retried on the next tick.
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Channel notifier poll error; retrying in {PollInterval}", PollInterval);
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;
        _cts.Cancel();
        _task = null;
        _cts.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;
        _cts.Cancel();
        if (_task is not null)
        {
            try { await _task.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
        _task = null;
        _cts.Dispose();
    }
}
