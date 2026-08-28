using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;
using Peers.Shared;
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

    /// <summary>Broker failures are tool errors, not payloads: the SDK turns this into isError=true.</summary>
    private static McpException BrokerError(BrokerApiException ex) => new(ex.Message, ex);

    [McpServerTool(Name = "peers"), Description(
        "Live roster of peer sessions known to the local broker — who is working where, on what branch, doing what.\n" +
        "Returns: array of peers with peer_id, tool, repo_root, branch, role, summary, and progress_current/progress_total/progress_updated_at for peers reporting long-running work.")]
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
        catch (BrokerApiException ex) { throw BrokerError(ex); }
    }

    [McpServerTool(Name = "presence"), Description(
        "Update how this session appears to other peers: role (implementing/reviewing/debugging/spec), summary of current work, capability tags, focus paths.\n" +
        "Identity (tool, repo, branch) is derived automatically; presence is for the human-meaningful fields. Renewed automatically by heartbeat.\n" +
        "Long-running work: call again with progress_current (and progress_total when you know it) as you go. Last value wins, one task at a time, and `summary` says what is being counted. Other peers see it in the roster, on peers://peers, and on their wait keepalives.")]
    public static async Task<string> Presence(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Role: implementing, reviewing, debugging, spec")] string? role = null,
        [Description("Brief summary of current work")] string? summary = null,
        [Description("File paths this session focuses on")] string[]? focus_paths = null,
        [Description("Capability tags this session advertises, e.g. [\"build\",\"test\",\"review\"]")] string[]? capabilities = null,
        [Description("Units of the current long-running task done so far, e.g. 7 of 12 files")] int? progress_current = null,
        [Description("Total units in the current task; omit for an open-ended count. Requires progress_current")] int? progress_total = null,
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
                ProgressCurrent = progress_current,
                ProgressTotal = progress_total,
            });
            return JsonSerializer.Serialize(new { ok = true, peer_id = state.PeerId }, JsonOpts);
        }
        catch (BrokerApiException ex) { throw BrokerError(ex); }
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
        catch (BrokerApiException ex) { throw BrokerError(ex); }
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
        catch (BrokerApiException ex) { throw BrokerError(ex); }
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
        [Description("Retry-safe key: a second send with this key in this channel returns the first message instead of posting twice")] string? idempotency_key = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var ch = await ResolveChannelAsync(client, channel, state.RepoRoot, cancellationToken)
                     ?? await ResolveChannelAsync(client, channel, null, cancellationToken);
            if (ch is null)
                throw new McpException($"Channel '{channel}' not found.");

            var resp = await client.SendMessageAsync(ch.ChannelId, new SendMessageRequest
            {
                AuthorPeerId = state.PeerId!,
                Content = content,
                To = to,
                Quiet = quiet,
                IdempotencyKey = idempotency_key,
            });
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { throw BrokerError(ex); }
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
                throw new McpException($"Channel '{channel}' not found.");

            var resp = await client.ReadChannelMessagesAsync(ch.ChannelId, viewer: state.PeerId, since_seq ?? 0, limit ?? 200);
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { throw BrokerError(ex); }
    }

    [McpServerTool(Name = "wait"), Description(
        "Long-poll: block up to timeout_seconds (default 25, max 120) until a directed message arrives — mentions, replies, review requests. Returns as soon as something lands; timed_out:true at the deadline.\n" +
        "The default fits inside the 30s MCP request timeout most clients ship. Progress keepalives go out every 5s, but a client that does not reset its deadline on them aborts a longer wait as a transport error — indistinguishable from broker silence — so raise the client's per-server MCP timeout before passing more than 25.\n" +
        "At-least-once by receipt: pass inbox_id values from your PREVIOUS result as ack_inbox_ids; ids you never echo are redelivered, never lost.")]
    public static Task<string> Wait(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Max seconds to block waiting for directed events (default 25, max 120; above 25 needs the client's MCP request timeout raised to match)")] int? timeout_seconds = null,
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

            // 25s, not the broker route's 60: an MCP client's request timeout is one-shot (omp: 30s
            // default, no progress-based reset), so a longer default hands the caller a transport error
            // instead of timed_out. Callers who raised their client budget can still ask for up to 120.
            var t = Math.Clamp(timeout_seconds ?? 25, 1, 120);
            var completed = false;
            // Linked, so a client cancellation stops the ticker as well as the broker call.
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var ticker = TickAsync(progress, state, t, tickInterval, () => completed, cts.Token);
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
        catch (BrokerApiException ex) { throw BrokerError(ex); }
        // No catch for OperationCanceledException: MCP 2026-07-28 says a server SHOULD NOT respond to a
        // cancelled request, and SDK 2.2.0 delivers that by suppressing the response when the handler
        // propagates.
    }

    private static async Task TickAsync(
        IProgress<ProgressNotificationValue>? progress,
        SessionState state,
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
                        Message = $"Waiting for peer messages… {elapsedSeconds}s/{totalSeconds}s{PeerProgress(state)}",
                    });
                }
                catch { return; }
            }
        }
        catch (OperationCanceledException) { }
    }

    /// <summary>
    /// What the other peers are doing, as a message suffix, or "" when nobody is reporting.
    ///
    /// Reads the notifier's cached roster — no I/O, no await. Progress/Total stay the elapsed-seconds
    /// countdown; only this text changes, so the keepalive's numeric contract is untouched.
    ///
    /// Stale progress is SHOWN with its age, not hidden. The question a parked peer actually has is
    /// "stalled or still thinking?", and "7/12 (14m ago)" answers it where "" answers nothing — a
    /// driver inside one long tool call reports nothing for many minutes, and silence there reads
    /// exactly like nobody working. It also keeps this agreeing with the CLI, which already annotates
    /// age. No upper bound is needed: the roster only ever holds peers whose lease is still live.
    ///
    /// ponytail: two peers, 40 chars of summary each. A keepalive is a status line, not the roster —
    /// the peers tool has the full text. Widen only if a real wait ever needs more.
    /// </summary>
    private static string PeerProgress(SessionState state)
    {
        var now = DateTime.UtcNow;
        var line = string.Join("; ", state.PeerRoster
            .Where(p => p.PeerId != state.PeerId && p.ProgressCurrent is not null)
            .OrderByDescending(p => p.ProgressUpdatedAt)
            .Take(2)
            .Select(p => $"{(p.PeerId.Length <= 8 ? p.PeerId : p.PeerId[..8])} {ProgressFormat.CountsWithAge(p.ProgressCurrent, p.ProgressTotal, p.ProgressUpdatedAt, now)}"
                         + (p.Summary is { Length: > 0 } s ? $" {(s.Length <= 40 ? s : s[..40] + "…")}" : "")));

        return line.Length == 0 ? "" : $" · {line}";
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
