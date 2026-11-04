using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
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
        "Live roster of peer sessions: peer_id, tool, repo_root, branch, role, summary, and progress.")]
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
        "Set how this session appears to peers: role, summary, capabilities, focus paths, and progress.")]
    public static async Task<string> Presence(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Role: implementing, reviewing, debugging, or spec. Identity (tool, repo, branch) is derived automatically: presence sets only the human-meaningful fields, and the heartbeat renews them")] string? role = null,
        [Description("Brief summary of current work. During a long-running task, it says what progress_current counts")] string? summary = null,
        [Description("File paths this session focuses on")] string[]? focus_paths = null,
        [Description("Capability tags this session advertises, e.g. [\"build\",\"test\",\"review\"]")] string[]? capabilities = null,
        [Description("Units of the current long-running task done so far, e.g. 7 of 12 files. Call presence again as you go: the last value wins, one task at a time. Other peers see it in the roster, on peers://peers, and on their wait keepalives")] int? progress_current = null,
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

    private static readonly string[] ChannelsActions = ["list", "create"];

    [McpServerTool(Name = "channels"), Description(
        "List channels or create one. A channel is a named message board for one repo or for all repos.")]
    public static async Task<string> Channels(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description(
            "list: every channel. A channel name belongs to agents; repo_root says whose code it is about. " +
            "Returns { channels:[{channel_id, name, repo_root, created_by_peer_id, created_at, message_count}] }.\n" +
            "create (needs name): create a channel scoped to this session's repo, to repo_root, or to every repo with global. " +
            "Names are unique per scope: an existing (repo, name) pair returns an error that names the existing channel. Returns { channel_id, name, repo_root }.\n" +
            "Exact, lowercase."),
         AllowedValues("list", "create")] string action,
        [Description("create (required): channel name, e.g. 'deploy' or 'mafia-review'")] string? name = null,
        [Description("create only: repository root this channel belongs to (defaults to this session's repo). Omit it with global")] string? repo_root = null,
        [Description(
            "create only: true creates a global channel that sessions in every repo reach by name; use it for notices to sessions in several repos. " +
            "A name otherwise reaches only channels in the caller's repo and the global scope; sessions in other repos address a repo channel by its channel_id")] bool global = false,
        CancellationToken cancellationToken = default)
    {
        action = ToolArgs.RequireAction(action, ChannelsActions);
        if (action == "create")
        {
            ToolArgs.RequirePayload(action, "name", name is not null, required: true);
            if (global && repo_root is not null)
                throw new McpException($"repo_root is '{repo_root}' and global is true; a channel has one scope. Omit repo_root for a global channel, or global for a repo channel.");
        }
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            return action == "list"
                ? JsonSerializer.Serialize(await client.ListChannelsAsync(), JsonOpts)
                : JsonSerializer.Serialize(await client.CreateChannelAsync(new CreateChannelRequest
                {
                    Name = name!,
                    RepoRoot = global ? null : repo_root ?? state.RepoRoot,
                    CreatedByPeerId = state.PeerId!,
                }), JsonOpts);
        }
        catch (BrokerApiException ex) { throw BrokerError(ex); }
    }

    [McpServerTool(Name = "send"), Description(
        "Post a message to a channel. Mention peers with `to`; quiet=true whispers to the mentions only.")]
    public static async Task<string> Send(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Markdown content to post. Returns { message_id, seq, mentions, delivery:{online,offline,unresolved} }: delivery tells you whether each mention reached a live session")] string content,
        [Description(ChannelNameDescription)] string? channel = null,
        [Description(ChannelIdDescription)] string? channel_id = null,
        [Description("Peer IDs (or ID prefixes) to mention")] string[]? to = null,
        [Description("false (default): loud, visible to everyone reading the channel; subscribers get a push hint. true: a whisper, delivered to the mentioned peers only; nobody else ever sees it in reads or notifications. Quiet requires at least one mention")] bool quiet = false,
        [Description("Retry-safe key: a second send with this key in this channel returns the first message instead of posting twice")] string? idempotency_key = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var id = await ChannelIdAsync(client, state, channel, channel_id, cancellationToken);
            var resp = await client.SendMessageAsync(id, new SendMessageRequest
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
        "Read a channel's messages, oldest first. You see a whisper only if you wrote it or it mentions you.")]
    public static async Task<string> Read(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description(ChannelNameDescription)] string? channel = null,
        [Description(ChannelIdDescription)] string? channel_id = null,
        [Description("Only messages after this seq (from a previous read). truncated=true in a result means: call again with since_seq set to its last seq")] long? since_seq = null,
        [Description("Max messages per call (default 200)")] int? limit = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var id = await ChannelIdAsync(client, state, channel, channel_id, cancellationToken);
            var resp = await client.ReadChannelMessagesAsync(id, viewer: state.PeerId, since_seq ?? 0, limit ?? 200);
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { throw BrokerError(ex); }
    }

    [McpServerTool(Name = "channel_clear"), Description(
        "DESTRUCTIVE: permanently delete every message in a channel, for all sessions. The channel stays.")]
    public static async Task<string> ChannelClear(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description(DeletableChannelNameDescription)] string? channel = null,
        [Description(ChannelIdDescription)] string? channel_id = null,
        [Description(ConfirmChannelDescription + DeletionResultDescription)] string? confirm_channel = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var id = await DeletableChannelIdAsync(client, state, channel, channel_id, confirm_channel, cancellationToken);
            var resp = await client.ClearChannelAsync(id, cancellationToken);
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { throw BrokerError(ex); }
    }

    [McpServerTool(Name = "messages_remove"), Description(
        "DESTRUCTIVE: permanently delete selected messages from a channel, for all sessions.")]
    public static async Task<string> MessagesRemove(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description("Message IDs to remove (1..200), from read or send. Missing IDs are ignored, so removing them again returns zero")] string[] message_ids,
        [Description(DeletableChannelNameDescription)] string? channel = null,
        [Description(ChannelIdDescription)] string? channel_id = null,
        [Description(ConfirmChannelDescription + DeletionResultDescription)] string? confirm_channel = null,
        CancellationToken cancellationToken = default)
    {
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var id = await DeletableChannelIdAsync(client, state, channel, channel_id, confirm_channel, cancellationToken);
            var resp = await client.RemoveMessagesAsync(id,
                new RemoveMessagesRequest { MessageIds = message_ids }, cancellationToken);
            return JsonSerializer.Serialize(resp, JsonOpts);
        }
        catch (BrokerApiException ex) { throw BrokerError(ex); }
    }

    [McpServerTool(Name = "wait"), Description(
        "Wait for a directed message (mention, reply, review request); returns as soon as one lands.")]
    public static Task<string> Wait(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        [Description(
            "Max seconds to block (default 120, max 120); the result has timed_out:true at the deadline. Omit it: the default returns the moment a message lands, so a shorter window only adds empty polls. " +
            "The client's MCP request timeout must exceed the window (omp: timeout 130000 on the peers entry); a client limited to less passes a smaller timeout_seconds")] int? timeout_seconds = null,
        [Description("inbox_id strings from your PREVIOUS wait result, to acknowledge them. Delivery is at-least-once by receipt: ids you never echo are redelivered, never lost")] string[]? ack_inbox_ids = null,
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
            if (ack_inbox_ids is not null)
                for (var i = 0; i < ack_inbox_ids.Length; i++)
                    if (string.IsNullOrWhiteSpace(ack_inbox_ids[i]))
                        throw new McpException($"ack_inbox_ids[{i}] is {(ack_inbox_ids[i] is null ? "null" : "blank")}; " +
                            "pass the inbox_id strings of a previous wait result, or omit ack_inbox_ids. Nothing was acknowledged.");

            await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);

            // Before the long poll, so an id acknowledged now cannot come back in this same result.
            if (ack_inbox_ids is { Length: > 0 })
                await client.AckInboxAsync(state.PeerId!, ack_inbox_ids, cancellationToken: cancellationToken);

            // The broker's maximum. The poll returns the moment a directed message lands, so a long
            // window costs nothing, while a short one only multiplies empty polls. An MCP client's
            // request timeout is one-shot (omp does not reset it on progress), so the client budget
            // must exceed this window: omp's peers entry sets 130000 ms.
            var t = Math.Clamp(timeout_seconds ?? 120, 1, 120);
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

    private const string ChannelNameDescription =
        "Channel name, resolved in this session's repo, then the global scope. Pass this or channel_id, not both";
    private const string DeletableChannelNameDescription =
        "Channel name, resolved in this session's repo only: it never falls back to a global channel of the same name. Delete a global channel by channel_id with confirm_channel. Pass this or channel_id, not both";
    private const string ChannelIdDescription =
        "Channel ID (from channels, a message, or an inbox item); reaches a channel scoped to any repo. Pass this or channel, not both";
    private const string ConfirmChannelDescription =
        "The channel's exact, case-sensitive name; blank counts as absent. Required with channel_id: compared with the name the broker stores for that id, the name only, not the repo (names are unique per repo, so check repo_root in the result). " +
        "Optional with channel: if given, it must equal channel. A missing (by id) or mismatched confirmation deletes nothing";
    private const string DeletionResultDescription =
        ". The deletion applies to every session and includes the pending inbox deliveries of the deleted messages; sequence numbers are not reset. Returns { channel_id, name, repo_root, deleted_messages }";

    /// <summary>
    /// The broker routes are keyed by id, so an id goes straight through — an unknown one is the
    /// broker's NotFound. A name stays scoped: this session's repo first, then global, never an
    /// unrelated repo; addressing another repo's channel takes its explicit id. A name that misses both
    /// is diagnosed rather than reported missing: a channel id passed as the name reaches that channel
    /// (the id is unambiguous), and a same-named channel in another repo is reported with its id.
    /// </summary>
    private static async Task<string> ChannelIdAsync(
        BrokerClient client, SessionState state, string? channel, string? channelId, CancellationToken ct)
    {
        if (!ByName(channel, channelId)) return channelId!;

        if (state.RepoRoot is not null && await ResolveChannelAsync(client, channel!, state.RepoRoot, ct) is { } own)
            return own.ChannelId;
        // ListAsync(null) returns every channel, so the global lookup and the diagnosis share one read.
        var named = (await client.ListChannelsAsync(null, limit: 500)).Channels
            .Where(c => string.Equals(c.Name, channel, StringComparison.Ordinal)).ToList();
        if (named.FirstOrDefault(c => c.RepoRoot is null) is { } global)
            return global.ChannelId;
        try { return (await client.GetChannelAsync(channel!, ct)).ChannelId; }
        catch (BrokerApiException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound) { }

        var scope = state.RepoRoot is null
            ? "the global scope (this session has no repo_root)"
            : $"this session's repo_root '{state.RepoRoot}' or the global scope";
        throw new McpException(named.Count == 0
            ? $"No channel named or with channel_id '{channel}' exists in {scope} or any other repo. List channels with `channels` action list, or create one with action create."
            : $"Channel '{channel}' is not in {scope}; a name reaches only those. It exists in another repo: " +
              string.Join("; ", named.Select(c => $"channel_id '{c.ChannelId}' (repo_root '{c.RepoRoot}')")) +
              ". Pass channel_id instead of channel. For notices to sessions in every repo, create the channel with `channels` action create, global: true.");
    }

    /// <summary>True for a name, false for an id; anything but exactly one of them is an error.</summary>
    private static bool ByName(string? channel, string? channelId)
    {
        var byName = !string.IsNullOrWhiteSpace(channel);
        if (byName == !string.IsNullOrWhiteSpace(channelId))
            throw new McpException("Pass exactly one of `channel` (name) or `channel_id`.");
        return byName;
    }

    /// <summary>
    /// By name, deletion resolves in this session's repo ONLY. The send/read fallback to a global
    /// channel is unsafe here: a session in a linked worktree or outside Git does not see the main
    /// checkout's repo channels, so `channel_clear("review")` would silently empty a global "review".
    /// The name is its own confirmation, but a confirmation that contradicts it means the caller is
    /// unsure which channel it means.
    ///
    /// Deletion by id reaches a channel in any repo, global included, so a stray or stale id must not
    /// silently empty another repo's history. The confirmation is compared with the name stored for THAT
    /// id — never resolved in this session's repo, where the same name can be a different channel. Only
    /// the name is compared: names are unique per repo, so a same-named channel elsewhere also passes,
    /// and the result's repo_root shows which channel was hit.
    /// </summary>
    private static async Task<string> DeletableChannelIdAsync(BrokerClient client, SessionState state,
        string? channel, string? channelId, string? confirmChannel, CancellationToken ct)
    {
        if (ByName(channel, channelId))
        {
            if (!string.IsNullOrWhiteSpace(confirmChannel) && !string.Equals(channel, confirmChannel, StringComparison.Ordinal))
                throw new McpException($"confirm_channel '{confirmChannel}' does not match channel '{channel}'. Nothing was deleted.");
            var own = state.RepoRoot is null ? null : await ResolveChannelAsync(client, channel!, state.RepoRoot, ct);
            return own?.ChannelId ?? throw new McpException($"Channel '{channel}' not found in " +
                (state.RepoRoot is null ? "this session's repo: the session has no repo_root" : $"this session's repo_root '{state.RepoRoot}'") +
                ". Deletion by name never falls back to a global channel; delete one by channel_id with confirm_channel. Nothing was deleted.");
        }

        var id = channelId!;
        var stored = await client.GetChannelAsync(id, ct);
        if (string.Equals(stored.Name, confirmChannel, StringComparison.Ordinal)) return id;
        var actual = $"channel_id '{id}' is '{stored.Name}' in repo_root " +
                     (stored.RepoRoot is null ? "null (global)" : $"'{stored.RepoRoot}'");
        throw new McpException(string.IsNullOrWhiteSpace(confirmChannel)
            ? $"Deleting by channel_id requires confirm_channel, the channel's exact name: {actual}. Nothing was deleted."
            : $"confirm_channel '{confirmChannel}' does not match: {actual}. Nothing was deleted.");
    }

    private static async Task<ChannelSummary?> ResolveChannelAsync(
        BrokerClient client, string name, string? repoRoot, CancellationToken ct)
    {
        var list = await client.ListChannelsAsync(repoRoot, limit: 500);
        foreach (var c in list.Channels)
            if (string.Equals(c.Name, name, StringComparison.Ordinal) && c.RepoRoot == repoRoot)
                return c;
        return null;
    }
}
