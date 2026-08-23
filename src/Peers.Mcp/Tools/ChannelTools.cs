using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

/// <summary>
/// The whole agent-facing surface: channels, messages, mentions, presence. Registration is automatic
/// (see <see cref="SessionRegistration"/>) — every tool ensures it first, so there is no setup ritual.
///
/// Delivery model: loud messages notify channel subscribers and appear in every read; quiet messages
/// (quiet=true + at least one mention in `to`) are whispers — inbox-delivered to the mentioned peers,
/// invisible to everyone else's reads and notifications. Directed delivery is always at-least-once
/// with echoed receipts: reading acknowledges nothing, ack_inbox_ids does.
/// </summary>
[McpServerToolType]
public sealed class ChannelTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    private static string BrokerError(BrokerApiException ex) =>
        JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode }, JsonOpts);

    [McpServerTool(Name = "peers"), Description(
        "Live roster of peer sessions known to the local broker — who is working where, on what branch, doing what.\n" +
        "Returns: array of peers with peer_id, tool, repo_root, branch, role, summary, self flag.")]
    public static async Task<string> Peers(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var peers = await client.ListPeersAsync();
            return JsonSerializer.Serialize(peers, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "presence"), Description(
        "Update how this session appears to other peers: role (implementing/reviewing/debugging/spec), summary of current work, capability tags, focus paths.\n" +
        "Identity (tool, repo, branch) is derived automatically; presence is for the human-meaningful fields. Renewed automatically by heartbeat.")]
    public static async Task<string> Presence(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Role: implementing, reviewing, debugging, spec")] string? role = null,
        [Description("Brief summary of current work")] string? summary = null,
        [Description("File paths this session focuses on")] string[]? focus_paths = null,
        [Description("Capability tags this session advertises, e.g. [\"build\",\"test\",\"review\"]")] string[]? capabilities = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            await client.HeartbeatAsync(state.PeerId!, new HeartbeatRequest
            {
                Role = role,
                Summary = summary,
                FocusPaths = focus_paths is null ? null : JsonSerializer.Serialize(focus_paths),
                Capabilities = capabilities is null ? null : JsonSerializer.Serialize(capabilities),
            });
            return JsonSerializer.Serialize(new { ok = true, peer_id = state.PeerId }, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "channels"), Description(
        "List channels — named message boards, optionally scoped to a repository. A channel name belongs to agents; the repo metadata says whose code it is about.\n" +
        "Returns: array of { channel_id, name, repo_root, message_count, created_at }.")]
    public static async Task<string> Channels(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var resp = await client.ListChannelsAsync();
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "channel_create"), Description(
        "Create a channel. Name is yours to choose; repo_root tags which repository it belongs to (omit for a global channel).\n" +
        "Names are unique per scope — creating an existing (repo, name) pair returns an error naming the existing channel.")]
    public static async Task<string> ChannelCreate(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Channel name, e.g. 'deploy' or 'mafia-review'")] string name,
        [Description("Repository root this channel belongs to (defaults to this session's repo)")] string? repo_root = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var resp = await client.CreateChannelAsync(new CreateChannelRequest
            {
                Name = name,
                RepoRoot = repo_root ?? state.RepoRoot,
                CreatedByPeerId = state.PeerId!,
            });
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "send"), Description(
        "Post one message to a channel (resolved by name). Mentions go in `to` as peer IDs or prefixes.\n" +
        "Loud (default): visible to everyone reading the channel; subscribers get a push hint.\n" +
        "quiet=true: a whisper — delivered to the mentioned peers only; nobody else ever sees it in reads or notifications. Quiet requires at least one mention.\n" +
        "Returns: { message_id, seq, mentions, delivery:{online,offline,unresolved} } — delivery tells you whether each mention reached a live session.")]
    public static async Task<string> Send(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Channel name")] string channel,
        [Description("Markdown content to post")] string content,
        [Description("Peer IDs (or ID prefixes) to mention")] string[]? to = null,
        [Description("Deliver only to mentions; hide from all other readers")] bool quiet = false,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var ch = await ResolveChannelAsync(client, channel, state.RepoRoot, cancellationToken)
                     ?? await ResolveChannelAsync(client, channel, null, cancellationToken);
            if (ch is null)
                return JsonSerializer.Serialize(new { error = $"Channel '{channel}' not found." }, JsonOpts);

            var resp = await client.SendMessageAsync(ch.ChannelId, new SendMessageRequest
            {
                AuthorPeerId = state.PeerId!,
                Content = content,
                To = to,
                Quiet = quiet,
            });
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "read"), Description(
        "Read a channel's messages (by name), oldest first. Whispers addressed to others are filtered out of your view automatically; you see your own quiet messages and those mentioning you.\n" +
        "since_seq pages forward from a previous read's last seq; truncated=true means call again with since_seq set.")]
    public static async Task<string> Read(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Channel name")] string channel,
        [Description("Only messages after this seq (from a previous read)")] long? since_seq = null,
        [Description("Max messages per call (default 200)")] int? limit = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var ch = await ResolveChannelAsync(client, channel, state.RepoRoot, cancellationToken)
                     ?? await ResolveChannelAsync(client, channel, null, cancellationToken);
            if (ch is null)
                return JsonSerializer.Serialize(new { error = $"Channel '{channel}' not found." }, JsonOpts);

            var resp = await client.ReadChannelMessagesAsync(ch.ChannelId, viewer: state.PeerId, since_seq ?? 0, limit ?? 200);
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "wait"), Description(
        "Long-poll: block up to timeout_seconds (default 60, max 120) until a directed message arrives — mentions, replies, review requests. Returns as soon as something lands; timed_out:true at the deadline.\n" +
        "At-least-once by receipt: pass inbox_id values from your PREVIOUS result as ack_inbox_ids; ids you never echo are redelivered, never lost.")]
    public static Task<string> Wait(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Max seconds to block waiting for directed events (default 60, max 120)")] int? timeout_seconds = null,
        [Description("inbox_id values from a PREVIOUS wait result, to acknowledge them")] string[]? ack_inbox_ids = null,
        // SDK-bound (progress token / request cancellation), excluded from the tool schema; see WaitCore.
        IProgress<ProgressNotificationValue>? progress = null,
        CancellationToken cancellationToken = default) =>
        WaitCore(client, state, server, loggerFactory, timeout_seconds, ack_inbox_ids, progress, TickInterval, cancellationToken);

    /// <summary>Seconds credited per progress tick. Also the real tick cadence in production (spec: servers SHOULD rate-limit).</summary>
    private const int TickSeconds = 5;
    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(TickSeconds);

    internal static async Task<string> WaitCore(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        int? timeout_seconds,
        string[]? ack_inbox_ids,
        IProgress<ProgressNotificationValue>? progress,
        TimeSpan tickInterval,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);

            // Before the long poll, so an id acknowledged now cannot come back in this same result.
            if (ack_inbox_ids is { Length: > 0 })
                await client.AckInboxAsync(state.PeerId!, ack_inbox_ids, cancellationToken: cancellationToken);

            var t = Math.Clamp(timeout_seconds ?? 60, 1, 120);
            var completed = false;
            // Linked, so a client cancellation stops the ticker as well as the broker call.
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var ticker = TickAsync(progress, t, tickInterval, () => completed, cts.Token);
            InboxResponse? resp;
            try
            {
                resp = await client.WaitInboxAsync(state.PeerId!, t, cancellationToken: cancellationToken);
            }
            finally
            {
                completed = true;
                cts.Cancel();
                await ticker;
            }
            return Shape(state, resp, resp?.TimedOut ?? true);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
        // No catch for OperationCanceledException: MCP 2026-07-28 says a server SHOULD NOT respond to a
        // cancelled request, and SDK 2.2.0 delivers that by suppressing the response when the handler
        // propagates.
    }

    private static async Task TickAsync(
        IProgress<ProgressNotificationValue>? progress,
        int totalSeconds,
        TimeSpan tickInterval,
        Func<bool> completed,
        CancellationToken ct)
    {
        var elapsedSeconds = 0;
        try
        {
            while (true)
            {
                await Task.Delay(tickInterval, ct);
                elapsedSeconds += TickSeconds;
                if (elapsedSeconds > totalSeconds) return;
                if (completed()) return;
                try
                {
                    progress?.Report(new ProgressNotificationValue
                    {
                        Progress = elapsedSeconds,
                        Total = totalSeconds,
                        Message = $"Waiting for peer messages… {elapsedSeconds}s/{totalSeconds}s",
                    });
                }
                catch { return; }
            }
        }
        catch (OperationCanceledException) { }
    }

    private static string Shape(SessionState state, InboxResponse? resp, bool? timedOut)
    {
        var reserve = timedOut is null ? 0 : 24;
        var shape = InboxResultShaper.ShapeInbox(resp?.Items ?? [], state.LastCursor, Limits.MaxToolResultChars - reserve, JsonOpts);
        if (timedOut is null) return shape.Json;
        var node = System.Text.Json.Nodes.JsonNode.Parse(shape.Json)!.AsObject();
        node["timed_out"] = timedOut.Value;
        return node.ToJsonString();
    }

    private static async Task<ChannelSummary?> ResolveChannelAsync(
        BrokerClient client, string name, string? repoRoot, CancellationToken ct)
    {
        var list = await client.ListChannelsAsync(repoRoot, limit: 500);
        foreach (var c in list.Channels)
            if (string.Equals(c.Name, name, StringComparison.Ordinal))
                return c;
        return null;
    }
}
