using System.Text.Json;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Peers.Shared.Models;

namespace Peers.Mcp.Lifecycle;

public sealed class ChannelNotifier : IDisposable, IAsyncDisposable
{
    private readonly BrokerClient _client;
    private readonly SessionState _state;
    private readonly McpServer _server;
    private readonly CancellationTokenSource _cts = new();
    private long _channelCursor;
    private Task? _task;
    private int _disposeStarted;

    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(5);
    private const string LoggerName = "peers.channel";

    public ChannelNotifier(BrokerClient client, SessionState state, McpServer server)
    {
        _client = client;
        _state = state;
        _server = server;
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
        await Console.Error.WriteLineAsync(
            $"[Peers] ChannelNotifier started for peer {_state.PeerId}, cursor={_channelCursor}, delivery={DescribeDeliveryMode()}");

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

                await Console.Error.WriteLineAsync($"[Peers] Poll returned {resp.Events.Count} events (cursor {_channelCursor} → {resp.Cursor})");
                _channelCursor = resp.Cursor;
                _state.LastCursor = Math.Max(_state.LastCursor, _channelCursor);

                foreach (var evt in resp.Events)
                {
                    var shouldDeliver = ShouldDeliver(evt);
                    var isOwn = IsOwnEvent(evt);
                    if (!shouldDeliver || isOwn)
                    {
                        await Console.Error.WriteLineAsync($"[Peers] Skipping event {evt.EventId} ({evt.Type}): shouldDeliver={shouldDeliver}, isOwn={isOwn}");
                        continue;
                    }

                    var message = FormatEvent(evt);
                    if (message is null)
                    {
                        await Console.Error.WriteLineAsync($"[Peers] FormatEvent returned null for event {evt.EventId} ({evt.Type})");
                        continue;
                    }

                    if (IsQuietEvent(evt))
                    {
                        await Console.Error.WriteLineAsync($"[Peers] Quiet event {evt.EventId} ({evt.Type}): quiet event; push suppressed (durable inbox covers it)");
                        continue;
                    }

                    await Console.Error.WriteLineAsync($"[Peers] Delivering event {evt.EventId} ({evt.Type}): {message.Content[..Math.Min(80, message.Content.Length)]}...");
                    await TryPushAsync(message, ct);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                await Console.Error.WriteLineAsync($"[Peers] ChannelNotifier poll error: {ex.Message}");
            }
        }
    }

    public async Task SendTestNotificationAsync(string content, CancellationToken ct = default)
    {
        var msgId = $"peers_test_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        await Console.Error.WriteLineAsync($"[Peers] Sending test notification: {msgId}");

        var message = new PendingChannelMessage
        {
            MessageId = msgId,
            EventId = 0,
            EventType = "peer_test_push",
            CreatedAt = DateTime.UtcNow,
            Content = content,
            Level = LoggingLevel.Notice,
            ScratchpadId = null,
            Title = "Peers test push",
            FromPeerId = "peers",
        };

        await TryPushAsync(message, ct, throwOnFailure: true);

        await Console.Error.WriteLineAsync($"[Peers] Test notification sent successfully: {msgId}");
    }

    private async Task TryPushAsync(PendingChannelMessage message, CancellationToken ct, bool throwOnFailure = false)
    {
        var methods = GetPushMethods();
        Exception? lastFailure = null;

        foreach (var method in methods)
        {
            try
            {
                switch (method)
                {
                    case PushMethod.LoggingMessage:
                        await TryPushViaLoggingMessageAsync(message, ct);
                        return;
                    case PushMethod.ClaudeChannel:
                        await TryPushViaClaudeChannelAsync(message, ct);
                        return;
                }
            }
            catch (Exception ex)
            {
                lastFailure = ex;
                await Console.Error.WriteLineAsync(
                    $"[Peers] {method} push FAILED: {message.MessageId}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        if (throwOnFailure && lastFailure is not null)
            throw lastFailure;
    }

    private async Task TryPushViaLoggingMessageAsync(PendingChannelMessage message, CancellationToken ct)
    {
        await Console.Error.WriteLineAsync($"[Peers] Pushing logging notification: {message.MessageId}");

        await _server.SendNotificationAsync(
            NotificationMethods.LoggingMessageNotification,
            new LoggingMessageNotificationParams
            {
                Level = message.Level,
                Logger = LoggerName,
                Data = JsonSerializer.SerializeToElement(message.Content),
            },
            cancellationToken: ct);

        await Console.Error.WriteLineAsync($"[Peers] Logging push succeeded: {message.MessageId}");
    }

    private async Task TryPushViaClaudeChannelAsync(PendingChannelMessage message, CancellationToken ct)
    {
        await Console.Error.WriteLineAsync($"[Peers] Pushing channel notification: {message.MessageId}");

        await _server.SendNotificationAsync(
            "notifications/claude/channel",
            new
            {
                content = message.Content,
                meta = new
                {
                    chat_id = message.ScratchpadId ?? _state.ChatId,
                    message_id = message.MessageId,
                    user = message.FromPeerId ?? "Peers",
                    user_id = message.FromPeerId ?? "peers",
                    ts = message.CreatedAt.ToUniversalTime().ToString("O"),
                    source_type = "peers",
                },
            },
            cancellationToken: ct);

        await Console.Error.WriteLineAsync($"[Peers] Channel push succeeded: {message.MessageId}");
    }

    private PushMethod[] GetPushMethods()
    {
        if (UsesClaudeChannel())
            return [PushMethod.ClaudeChannel];

        return [PushMethod.LoggingMessage];
    }

    private bool UsesClaudeChannel()
    {
        var tool = _state.Tool;
        if (!string.IsNullOrWhiteSpace(tool) &&
            tool.Contains("claude", StringComparison.OrdinalIgnoreCase))
            return true;

        var clientName = _server.ClientInfo?.Name;
        return !string.IsNullOrWhiteSpace(clientName)
            && clientName.Contains("claude", StringComparison.OrdinalIgnoreCase);
    }

    private string DescribeDeliveryMode()
    {
        var methods = GetPushMethods();
        return string.Join(", ", methods.Select(method => method switch
        {
            PushMethod.LoggingMessage => "notifications/message",
            PushMethod.ClaudeChannel => "notifications/claude/channel",
            _ => method.ToString(),
        }));
    }

    private enum PushMethod
    {
        LoggingMessage,
        ClaudeChannel,
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

    private static bool IsQuietEvent(PeerEvent evt)
    {
        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);
            return doc.RootElement.TryGetProperty("quiet", out var quiet)
                && quiet.ValueKind == JsonValueKind.True;
        }
        catch { return false; }
    }

    private bool IsOwnEvent(PeerEvent evt)
    {
        if (_state.PeerId is null)
            return false;

        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);

            // For entry-added events, ownership is determined solely by
            // who authored the entry, not who created the scratchpad.
            var propertyNames = evt.Type == EventTypes.ScratchpadEntryAdded
                ? new[] { "author_peer_id" }
                : new[] { "peer_id", "author_peer_id", "created_by_peer_id", "published_by_peer_id" };

            foreach (var propertyName in propertyNames)
            {
                if (doc.RootElement.TryGetProperty(propertyName, out var actor) &&
                    actor.GetString() == _state.PeerId)
                    return true;
            }
        }
        catch { }

        return false;
    }

    private static PendingChannelMessage? FormatEvent(PeerEvent evt)
    {
        try
        {
            using var doc = JsonDocument.Parse(evt.Payload);
            var root = doc.RootElement;

            return evt.Type switch
            {
                EventTypes.ScratchpadEntryAdded => BuildMessage(
                    evt,
                    scratchpadId: root.GetProp("scratchpad_id"),
                    title: root.GetProp("title") ?? "Untitled",
                    fromPeerId: root.GetProp("author_peer_id"),
                    level: LoggingLevel.Warning,
                    content: BuildReplyText(
                        title: root.GetProp("title") ?? "Untitled",
                        scratchpadId: root.GetProp("scratchpad_id") ?? "unknown",
                        fromPeerId: root.GetProp("author_peer_id") ?? "unknown",
                        entryContent: root.GetProp("content"))),

                EventTypes.ScratchpadCreated => BuildMessage(
                    evt,
                    scratchpadId: root.GetProp("scratchpad_id"),
                    title: root.GetProp("title") ?? "Untitled",
                    fromPeerId: root.GetProp("created_by_peer_id"),
                    level: LoggingLevel.Warning,
                    content: $"Peers review request: '{root.GetProp("title") ?? "Untitled"}' " +
                             $"(type: {root.GetProp("type") ?? "unknown"}, " +
                             $"id: {root.GetProp("scratchpad_id") ?? "unknown"}, " +
                             $"from: {root.GetProp("created_by_peer_id") ?? "unknown"})"),

                EventTypes.ScratchpadResolved => BuildMessage(
                    evt,
                    scratchpadId: root.GetProp("scratchpad_id"),
                    title: root.GetProp("title") ?? "Untitled",
                    fromPeerId: null,
                    level: LoggingLevel.Info,
                    content: $"Peers scratchpad resolved: '{root.GetProp("title") ?? "Untitled"}' " +
                             $"(id: {root.GetProp("scratchpad_id") ?? "unknown"}" +
                             (root.GetProp("resolution") is { } res ? $", resolution: {res}" : "") + ")"),

                EventTypes.ScratchpadClosed => BuildMessage(
                    evt,
                    scratchpadId: root.GetProp("scratchpad_id"),
                    title: root.GetProp("title") ?? "Untitled",
                    fromPeerId: null,
                    level: LoggingLevel.Info,
                    content: $"Peers scratchpad closed: '{root.GetProp("title") ?? "Untitled"}' " +
                             $"(id: {root.GetProp("scratchpad_id") ?? "unknown"})"),

                EventTypes.ArtifactPublished => BuildMessage(
                    evt,
                    scratchpadId: null,
                    title: root.GetProp("name") ?? "unknown",
                    fromPeerId: root.GetProp("published_by_peer_id"),
                    level: LoggingLevel.Notice,
                    content: $"Peers artifact published: '{root.GetProp("name") ?? "unknown"}' " +
                             $"(ns: {root.GetProp("ns") ?? "unknown"}, " +
                             $"version: {root.GetProp("version") ?? "?"}, " +
                             $"from: {root.GetProp("published_by_peer_id") ?? "unknown"})"),

                EventTypes.PeerRegistered => BuildMessage(
                    evt,
                    scratchpadId: null,
                    title: "Peer joined",
                    fromPeerId: root.GetProp("peer_id"),
                    level: LoggingLevel.Info,
                    content: $"Peers new peer joined: {root.GetProp("peer_id") ?? "unknown"} " +
                             $"(tool: {root.GetProp("tool") ?? "unknown"}, " +
                             $"repo: {root.GetProp("repo_root") ?? "unknown"})"),

                EventTypes.PeerExpired => BuildMessage(
                    evt,
                    scratchpadId: null,
                    title: "Peers expired",
                    fromPeerId: null,
                    level: LoggingLevel.Info,
                    content: $"Peers: {root.GetProp("expired_count") ?? "?"} peer(s) expired"),

                _ => null,
            };
        }
        catch
        {
            return null;
        }
    }

    private static PendingChannelMessage BuildMessage(
        PeerEvent evt,
        string? scratchpadId,
        string title,
        string? fromPeerId,
        LoggingLevel level,
        string content) =>
        new()
        {
            MessageId = $"peers_evt_{evt.EventId}",
            EventId = evt.EventId,
            EventType = evt.Type,
            CreatedAt = evt.CreatedAt,
            ScratchpadId = scratchpadId,
            Title = title,
            FromPeerId = fromPeerId,
            Level = level,
            Content = content,
        };

    private static string BuildReplyText(string title, string scratchpadId, string fromPeerId, string? entryContent)
    {
        var preview = string.IsNullOrWhiteSpace(entryContent)
            ? "(empty)"
            : entryContent.Length > 200
            ? entryContent[..200] + "..."
            : entryContent;

        return $"Peers review reply: '{title}' (pad: {scratchpadId}, from: {fromPeerId})\n\n{preview}";
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
