using System.Text.Json;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using Peers.Shared.Models;

namespace Peers.Mcp.Lifecycle;

public sealed class ChannelNotifier : IDisposable, IAsyncDisposable
{
    private readonly BrokerClient _client;
    private readonly SessionState _state;
    private readonly McpSession _session;
    private readonly CancellationTokenSource _cts = new();
    private long _channelCursor;
    private Task? _task;
    private int _disposeStarted;

    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(5);

    public ChannelNotifier(BrokerClient client, SessionState state, McpSession session)
    {
        _client = client;
        _state = state;
        _session = session;
    }

    public void Start()
    {
        if (Volatile.Read(ref _disposeStarted) != 0)
            throw new ObjectDisposedException(nameof(ChannelNotifier));
        if (_task is not null)
            throw new InvalidOperationException("Channel notifier already started.");

        _channelCursor = _state.LastCursor;
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

                var resp = await _client.PollAsync(_state.PeerId, _channelCursor, 50);
                if (resp.Events.Count == 0)
                    continue;

                _channelCursor = resp.Cursor;
                _state.LastCursor = Math.Max(_state.LastCursor, _channelCursor);

                foreach (var evt in resp.Events)
                {
                    if (!ShouldDeliver(evt) || IsOwnEvent(evt))
                        continue;

                    var (content, level) = FormatEvent(evt);
                    if (content is null)
                        continue;

                    await _session.SendNotificationAsync(
                        NotificationMethods.LoggingMessageNotification,
                        new LoggingMessageNotificationParams
                        {
                            Level = level,
                            Logger = "peers.channel",
                            Data = JsonSerializer.SerializeToElement(content),
                        },
                        cancellationToken: ct);
                }
            }
            catch (OperationCanceledException) { break; }
            catch { /* swallow and retry next cycle */ }
        }
    }

    private bool ShouldDeliver(PeerEvent evt)
    {
        if (_state.PeerId is null)
            return false;

        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);
            return doc.RootElement.TryGetProperty("recipient_peer_ids", out var recipients)
                && recipients.ContainsString(_state.PeerId);
        }
        catch
        {
            return false;
        }
    }

    private bool IsOwnEvent(PeerEvent evt)
    {
        if (_state.PeerId is null)
            return false;

        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);
            foreach (var propertyName in new[] { "peer_id", "author_peer_id", "created_by_peer_id", "published_by_peer_id" })
            {
                if (doc.RootElement.TryGetProperty(propertyName, out var actor) &&
                    actor.GetString() == _state.PeerId)
                    return true;
            }
        }
        catch { }

        return false;
    }

    private static (string? content, LoggingLevel level) FormatEvent(PeerEvent evt)
    {
        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);
            var root = doc.RootElement;

            switch (evt.Type)
            {
                case EventTypes.ScratchpadEntryAdded:
                    var padId = root.GetProp("scratchpad_id") ?? "unknown";
                    var title = root.GetProp("title") ?? "Untitled";
                    var author = root.GetProp("author_peer_id") ?? "unknown";
                    var entryContent = root.GetProp("content");
                    var preview = string.IsNullOrWhiteSpace(entryContent)
                        ? "(empty)"
                        : entryContent.Length > 200
                        ? entryContent[..200] + "..."
                        : entryContent;
                    return ($"Peers review reply: '{title}' (pad: {padId}, from: {author})\n\n{preview}", LoggingLevel.Warning);

                case EventTypes.ScratchpadCreated:
                    title = root.GetProp("title") ?? "Untitled";
                    var type = root.GetProp("type") ?? "unknown";
                    var sid = root.GetProp("scratchpad_id") ?? "unknown";
                    var creator = root.GetProp("created_by_peer_id") ?? "unknown";
                    return ($"Peers review request: '{title}' (type: {type}, id: {sid}, from: {creator})", LoggingLevel.Warning);

                case EventTypes.ScratchpadClosed:
                    var closedId = root.GetProp("scratchpad_id") ?? "unknown";
                    var closedTitle = root.GetProp("title") ?? "Untitled";
                    return ($"Peers scratchpad closed: '{closedTitle}' (id: {closedId})", LoggingLevel.Info);

                default:
                    return (null, LoggingLevel.Info);
            }
        }
        catch
        {
            return (null, LoggingLevel.Info);
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

internal static class JsonElementExtensions
{
    public static string? GetProp(this JsonElement el, string name) =>
        el.TryGetProperty(name, out var val) ? val.GetString() : null;

    public static bool ContainsString(this JsonElement el, string value)
    {
        if (el.ValueKind != JsonValueKind.Array)
            return false;

        foreach (var item in el.EnumerateArray())
        {
            if (item.GetString() == value)
                return true;
        }

        return false;
    }
}
