using Peers.Mcp.Lifecycle;

namespace Peers.Mcp;

public sealed class SessionState : IDisposable, IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly Queue<PendingChannelMessage> _pendingChannelMessages = new();
    private readonly int _maxBufferedMessages;
    private int _droppedMessageCount;

    public SessionState()
    {
        var configuredSize = Environment.GetEnvironmentVariable("PEERS_MAX_BUFFERED_MESSAGES");
        _maxBufferedMessages = int.TryParse(configuredSize, out var size) && size > 0 ? size : 100;
        ChatId = $"peers_{Environment.ProcessId}_{Guid.NewGuid():N}";
    }

    public string ChatId { get; }
    public string? PeerId { get; set; }
    public string? Tool { get; set; }
    public long LastCursor { get; set; }
    public HeartbeatService? Heartbeat { get; set; }
    public ChannelNotifier? ChannelNotifier { get; set; }

    public void EnqueueChannelMessage(PendingChannelMessage message)
    {
        lock (_gate)
        {
            if (_pendingChannelMessages.Count >= _maxBufferedMessages)
            {
                _pendingChannelMessages.Dequeue();
                _droppedMessageCount++;
            }

            _pendingChannelMessages.Enqueue(message);
        }
    }

    public DrainChannelMessagesResult DrainChannelMessages(int? maxItems = null)
    {
        lock (_gate)
        {
            var takeCount = maxItems is > 0
                ? Math.Min(maxItems.Value, _pendingChannelMessages.Count)
                : _pendingChannelMessages.Count;

            var messages = new List<PendingChannelMessage>(takeCount);
            for (var i = 0; i < takeCount; i++)
                messages.Add(_pendingChannelMessages.Dequeue());

            var dropped = _droppedMessageCount;
            _droppedMessageCount = 0;

            return new DrainChannelMessagesResult
            {
                Messages = messages,
                DroppedMessageCount = dropped,
                RemainingCount = _pendingChannelMessages.Count,
            };
        }
    }

    public async ValueTask StopServicesAsync()
    {
        var channelNotifier = ChannelNotifier;
        ChannelNotifier = null;

        var heartbeat = Heartbeat;
        Heartbeat = null;

        if (channelNotifier is not null)
            await channelNotifier.DisposeAsync();
        if (heartbeat is not null)
            await heartbeat.DisposeAsync();
    }

    public ValueTask DisposeAsync() => StopServicesAsync();

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
