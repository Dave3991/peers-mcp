using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Lifecycle;

public sealed class HeartbeatService : IDisposable, IAsyncDisposable
{
    private readonly BrokerClient _client;
    private readonly string _peerId;
    private readonly ILogger<HeartbeatService> _logger;
    private readonly CancellationTokenSource _cts = new();
    private Task? _task;
    private int _disposeStarted;

    public HeartbeatService(BrokerClient client, string peerId, ILogger<HeartbeatService>? logger = null)
    {
        _client = client;
        _peerId = peerId;
        _logger = logger ?? NullLogger<HeartbeatService>.Instance;
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
                await Task.Delay(TimeSpan.FromSeconds(30), ct);
                await _client.HeartbeatAsync(_peerId, new HeartbeatRequest());
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Heartbeat failed for peer {PeerId}", _peerId);
            }
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
