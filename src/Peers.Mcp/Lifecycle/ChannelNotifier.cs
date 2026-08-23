// ponytail: MCP9005 disabled file-wide. The MCP Logging feature (LoggingLevel,
// notifications/message) is deprecated as of spec revision 2026-07-28 (SEP-2577), but it is
// still the only push path for clients on 2025-11-25 and earlier — which today is all of them.
// Scoped to this file rather than <NoWarn> in the csproj so the NEXT MCP9005 elsewhere is still
// reported. Remove the pragma, and this push path, once no supported client predates 2026-07-28.
//
// This disable runs to end of file, so DO NOT add new (non-legacy) code here: it would be silently
// exempt from MCP9005. Subscription registry and resources/updated dispatch live in their own
// files precisely so they stay under live deprecation warnings; this class only calls into them.
#pragma warning disable MCP9005

using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Peers.Shared.Models;

namespace Peers.Mcp.Lifecycle;

public sealed class ChannelNotifier : IDisposable, IAsyncDisposable
{
    private readonly BrokerClient _client;
    private readonly SessionState _state;
    private readonly McpServer _server;
    private readonly ILogger<ChannelNotifier> _logger;
    private readonly CancellationTokenSource _cts = new();
    private long _channelCursor;
    private long _resourceBaseline;
    private Task? _task;
    private int _disposeStarted;

    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(5);
    private const string LoggerName = "peers.channel";

    /// <summary>
    /// SEP-2577 retires the MCP logging feature: from this revision on, MetaKeys.LogLevel scopes log
    /// notifications to a request, so an UNSOLICITED notifications/message has no conforming form.
    /// The SDK's own McpProtocolVersions and McpSession.IsJuly2026OrLaterProtocol are both internal
    /// (compile-checked against 2.2.0), so the comparison lives here. Revision strings are fixed-width
    /// ISO dates, which order correctly under ordinal comparison.
    /// </summary>
    private const string July2026ProtocolVersion = "2026-07-28";

    public ChannelNotifier(BrokerClient client, SessionState state, McpServer server, ILogger<ChannelNotifier>? logger = null)
    {
        _client = client;
        _state = state;
        _server = server;
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
        // replay the whole retention window of every pad's history as resources/updated the moment a
        // subscription opened. Separate baseline, so the push path's resume-from-cursor is untouched.
        _resourceBaseline = _state.ResourceBaseline;
        _task = RunAsync(_cts.Token);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        _logger.LogDebug("ChannelNotifier started for peer {PeerId}, cursor={Cursor}, claudeChannel={ClaudeChannel}, legacyLogPushRetired={Retired}",
            _state.PeerId, _channelCursor, UsesClaudeChannel(), SuppressesLegacyLogPush());

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
                _logger.LogDebug("Poll returned {Count} events ({FromCursor} -> {ToCursor})",
                    resp.Events.Count, fromCursor, resp.Cursor);
                _channelCursor = resp.Cursor;
                _state.AdvanceCursor(_channelCursor);

                // Resource subscriptions dispatch BEFORE ShouldDeliver/IsOwnEvent on purpose. Those two
                // filters are right for directed pushes and wrong for subscriptions: a subscriber to a pad
                // wants every update to it, including ones it is not a named recipient of and ones it made
                // itself — the resource genuinely changed, and the client can dedupe.
                //
                // Per-batch, not per-event: resources/updated is a hint to re-read, so N changes to one URI
                // in one batch are one hint. The registry coalesces; this only feeds it the batch.
                //
                // Catch mirrors how TryPushAsync swallows: the cursor already advanced above, so one throw
                // here would abandon the whole batch — past the cursor, unrecoverable — including the push
                // path below.
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
                        fromCursor, resp.Cursor);
                }

                foreach (var evt in resp.Events)
                {
                    var shouldDeliver = ShouldDeliver(evt);
                    var isOwn = IsOwnEvent(evt);
                    if (!shouldDeliver || isOwn)
                    {
                        _logger.LogTrace("Skipping event {EventId} ({EventType}) shouldDeliver={ShouldDeliver} isOwn={IsOwn}",
                            evt.EventId, evt.Type, shouldDeliver, isOwn);
                        continue;
                    }

                    var message = FormatEvent(evt);
                    if (message is null)
                    {
                        _logger.LogDebug("FormatEvent returned null for event {EventId} ({EventType})",
                            evt.EventId, evt.Type);
                        continue;
                    }

                    if (IsQuietEvent(evt))
                    {
                        _logger.LogTrace("Quiet event {EventId} ({EventType}); push suppressed (durable inbox covers it)",
                            evt.EventId, evt.Type);
                        continue;
                    }

                    var preview = message.Content[..Math.Min(80, message.Content.Length)];
                    _logger.LogDebug("Delivering event {EventId} ({EventType}): {Preview}",
                        evt.EventId, evt.Type, preview);
                    await TryPushAsync(message, ct);
                }
            }
            // ONLY our own shutdown ends the loop. An unfiltered catch here cannot tell that from an
            // HttpClient timeout, which surfaces as TaskCanceledException — an OperationCanceledException —
            // so one unresponsive-broker stall used to kill push and resources/updated for the whole life
            // of the session, silently, while peer_inbox kept working and nothing looked broken.
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            // Everything else, including that timeout, is transient: logged and retried on the next tick.
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "ChannelNotifier poll loop error; retrying in {PollInterval}", PollInterval);
            }
        }
    }

    /// <summary>
    /// Diagnostic push for peer_test_push. Returns what actually happened — including "sent nothing, and
    /// why" — because a diagnostic that reports success without delivering is worse than no diagnostic.
    /// </summary>
    public async Task<PushResult> SendTestNotificationAsync(string content, CancellationToken ct = default)
    {
        var msgId = $"peers_test_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        _logger.LogDebug("Sending test notification {MessageId}", msgId);

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

        var result = await TryPushAsync(message, ct, throwOnFailure: true);

        _logger.LogDebug("Test notification {MessageId}: {Outcome} — {Detail}", msgId, result.Outcome, result.Detail);
        return result;
    }

    /// <summary>
    /// Sends one message by the one method that applies. There is never a combination to fall back
    /// through: notifications/claude/channel is custom and SEP-2577 has no opinion on it, so a Claude
    /// client takes it and nothing else; every other client takes the legacy log push, unless the
    /// negotiated revision retired that — which leaves nothing conforming to send at all.
    /// </summary>
    private async Task<PushResult> TryPushAsync(PendingChannelMessage message, CancellationToken ct, bool throwOnFailure = false)
    {
        var claudeChannel = UsesClaudeChannel();

        // No method at all is not a failure to retry — it is a standing property of the negotiated
        // revision. Reported, never thrown: the poll loop has nothing to do about it, and peer_test_push
        // needs the reason rather than an exception message.
        if (!claudeChannel && SuppressesLegacyLogPush())
        {
            _logger.LogDebug("No conforming push method for message {MessageId}; pull-mode peer_inbox covers delivery",
                message.MessageId);
            return new PushResult(
                PushOutcome.SuppressedByProtocolRevision,
                $"Nothing sent: protocol revision {_server.NegotiatedProtocolVersion} retired unsolicited " +
                "notifications/message (SEP-2577). Resource subscriptions or pull-mode peer_inbox cover delivery.");
        }

        try
        {
            if (claudeChannel)
            {
                await TryPushViaClaudeChannelAsync(message, ct);
                return new PushResult(PushOutcome.ClaudeChannel, "Sent as notifications/claude/channel.");
            }

            await TryPushViaLoggingMessageAsync(message, ct);
            return new PushResult(PushOutcome.LoggingMessage, "Sent as notifications/message.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Push failed for message {MessageId}", message.MessageId);
            // The poll loop swallows — one dead push must not abandon the batch, whose cursor already
            // advanced. peer_test_push asks to rethrow, because a diagnostic owes the caller the reason.
            if (throwOnFailure)
                throw;
            return new PushResult(PushOutcome.Failed, ex.Message);
        }
    }

    private async Task TryPushViaLoggingMessageAsync(PendingChannelMessage message, CancellationToken ct)
    {
        _logger.LogDebug("Pushing logging notification {MessageId}", message.MessageId);

        await _server.SendNotificationAsync(
            NotificationMethods.LoggingMessageNotification,
            new LoggingMessageNotificationParams
            {
                Level = message.Level,
                Logger = LoggerName,
                Data = JsonSerializer.SerializeToElement(message.Content),
            },
            cancellationToken: ct);

        _logger.LogDebug("Logging push succeeded {MessageId}", message.MessageId);
    }

    private async Task TryPushViaClaudeChannelAsync(PendingChannelMessage message, CancellationToken ct)
    {
        _logger.LogDebug("Pushing channel notification {MessageId}", message.MessageId);

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

        _logger.LogDebug("Channel push succeeded {MessageId}", message.MessageId);
    }

    /// <summary>True once the negotiated revision is 2026-07-28 or later. Null (pre-initialize) means no.</summary>
    private bool SuppressesLegacyLogPush() =>
        string.CompareOrdinal(_server.NegotiatedProtocolVersion, July2026ProtocolVersion) >= 0;

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
                : new[] { "peer_id", "author_peer_id", "created_by_peer_id", "published_by_peer_id", "from_peer_id" };

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
