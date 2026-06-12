using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Lifecycle;

public sealed class HeartbeatService : IDisposable, IAsyncDisposable
{
    private readonly BrokerClient _client;
    private readonly SessionState _state;
    private readonly ILogger<HeartbeatService> _logger;
    private readonly TimeSpan _interval;
    private readonly CancellationTokenSource _cts = new();
    private Task? _task;
    private int _disposeStarted;
    private DateTime _lastReRegisterUtc = DateTime.MinValue;

    public HeartbeatService(BrokerClient client, SessionState state, ILogger<HeartbeatService>? logger = null, TimeSpan? interval = null)
    {
        _client = client;
        _state = state;
        _logger = logger ?? NullLogger<HeartbeatService>.Instance;
        _interval = interval ?? TimeSpan.FromSeconds(30);
    }

    public void Start()
    {
        if (Volatile.Read(ref _disposeStarted) != 0)
            throw new ObjectDisposedException(nameof(HeartbeatService));
        if (_task is not null)
            throw new InvalidOperationException("Heartbeat service already started.");

        _task = RunAsync(_cts.Token);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_interval, ct);
                var peerId = _state.PeerId;
                if (peerId is null) continue;
                await _client.HeartbeatAsync(peerId, new HeartbeatRequest());
            }
            catch (OperationCanceledException) { break; }
            catch (BrokerApiException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                await TryReRegisterAsync(ct);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Heartbeat failed for peer {PeerId}", _state.PeerId);
            }
        }
    }

    private async Task TryReRegisterAsync(CancellationToken ct)
    {
        // Throttle re-register ATTEMPTS to at most once per minute (set timestamp at start of attempt).
        if (DateTime.UtcNow - _lastReRegisterUtc < TimeSpan.FromMinutes(1))
        {
            _logger.LogWarning("Broker returned 404 for peer {PeerId}; re-register throttled (last attempt < 1 min ago)", _state.PeerId);
            return;
        }
        _lastReRegisterUtc = DateTime.UtcNow;

        var stored = _state.LastRegisterRequest;
        if (stored is null)
        {
            _logger.LogWarning("Broker lost peer {PeerId} but no stored registration request to replay", _state.PeerId);
            return;
        }

        try
        {
            // NOTE: the broker only 404s the heartbeat when the peer ROW IS MISSING (RenewLeaseAsync
            // reactivates expired-but-present rows). So explicit resume by old id finds nothing and the
            // broker mints a FRESH id — self-heal => NEW identity (resumed=false). ResumePeerId is set for
            // the impossible-here case where the same row reappears; it's harmless otherwise.
            var clone = new RegisterPeerRequest
            {
                Tool = stored.Tool,
                MachineId = stored.MachineId,
                RepoRoot = stored.RepoRoot,
                WorktreeRoot = stored.WorktreeRoot,
                Branch = stored.Branch,
                CommitSha = stored.CommitSha,
                Role = stored.Role,
                Summary = stored.Summary,
                FocusPaths = stored.FocusPaths,
                Capabilities = stored.Capabilities,
                ParentPeerId = stored.ParentPeerId,
                ResumePeerId = _state.PeerId,
            };
            var resp = await _client.RegisterAsync(clone);
            _state.PeerId = resp.PeerId;
            _state.LastCursor = Math.Max(_state.LastCursor, resp.LastCursor); // don't rewind the poll cursor
            _logger.LogInformation("Broker lost peer; self-healed by re-registering as {PeerId} (resumed={Resumed}) — new identity", resp.PeerId, resp.Resumed);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Self-heal re-register failed; will retry after throttle window");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        _cts.Cancel();

        var task = _task;
        if (task is not null)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        _cts.Dispose();
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
