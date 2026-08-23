using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class PollTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "peer_poll"), Description(
        "Cursor-based debug feed of raw broker events (peer changes, scratchpad updates, artifact publishes) for this peer.\n" +
        "When NOT to use: do not use this for everyday directed-message reading — call peer_inbox instead. peer_poll exposes the cursor for diagnostics and replay.\n" +
        "Side effect: CONSUMES events — advances this session's cursor past everything it returns, so they are never returned to this session again.\n" +
        "Returns: { cursor, events[], truncated, returned_count, remaining_hint }. Large payloads are truncated (content_truncated per event) and the result is size-capped; the cursor advances only past returned events (truncated=true means call again). Defaults to picking up from the session's last cursor when omitted.\n" +
        "Replay: passing an explicit cursor returns the older events again but does NOT rewind the session — the session cursor only ever moves forward.\n" +
        "Note: best-effort debug feed — the background channel notifier advances the same cursor independently, so events can be consumed without ever appearing here. peer_poll is not a delivery guarantee.")]
    public static async Task<string> Poll(
        BrokerClient client,
        SessionState state,
        [Description("Event cursor to poll from (0 for all). Use the cursor returned by previous poll.")] long? cursor = null,
        [Description("Max events to return (default 100)")] int? limit = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";

        try
        {
            var effectiveCursor = cursor ?? state.LastCursor;
            var resp = await client.PollAsync(state.PeerId, effectiveCursor, limit ?? 100);

            var shape = InboxResultShaper.ShapePoll(resp.Events, resp.Cursor, Limits.MaxToolResultChars, JsonOpts);
            // Forward only, including when the caller passed an explicit low cursor to replay: the session
            // cursor is shared with the background notifier, and rewinding it would re-push everything in
            // between. Replay stays available — this call still RETURNS the older events; it just does not
            // make the whole session re-consume them.
            state.AdvanceCursor(shape.EffectiveCursor);

            return shape.Json;
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    private static string BrokerError(BrokerApiException ex) =>
        JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode }, JsonOpts);

    /// <summary>
    /// Acknowledges exactly the inbox ids the caller echoed back, and nothing else.
    ///
    /// An ack must carry a receipt the caller could only hold because a result was DELIVERED to it.
    /// Deferring the ack of a delivery until the next call cannot satisfy that: MCP request cancellation
    /// makes the SDK suppress the response frame, so the side effect can outlive a result the client never
    /// saw — and the row is then filtered out of every later read by acked_at. Echoed ids close it
    /// structurally, not by narrowing a window. An unechoed row is redelivered; the failure mode is
    /// duplication, which is the correct side of at-least-once.
    ///
    /// Not a trust boundary hole: the broker scopes the UPDATE by peer_id, so a caller can only ever ack
    /// its own rows, and acking an unknown or already-acked id is a no-op.
    /// </summary>
    private static async Task AckEchoedAsync(
        BrokerClient client, SessionState state, string[]? ackInboxIds, CancellationToken cancellationToken)
    {
        // Empty is not "ack nothing" to the broker — /inbox/ack rejects a request carrying neither ids nor
        // all=true — so a caller with nothing to acknowledge must not reach it at all.
        if (ackInboxIds is { Length: > 0 })
            await client.AckInboxAsync(state.PeerId!, ackInboxIds, cancellationToken: cancellationToken);
    }

    private static string Shape(SessionState state, InboxResponse? resp, bool? timedOut)
    {
        // Reserve headroom when we'll inject timed_out post-shape, so the final JSON stays <= cap (T13 finding #1 / T6 overshoot class).
        var reserve = timedOut is null ? 0 : 24;  // covers the widest ",\"timed_out\":false" rendering
        var shape = InboxResultShaper.ShapeInbox(resp?.Items ?? new List<InboxItemDto>(),
            state.LastCursor, Limits.MaxToolResultChars - reserve, JsonOpts);
        if (timedOut is null) return shape.Json;
        var node = System.Text.Json.Nodes.JsonNode.Parse(shape.Json)!.AsObject();
        node["timed_out"] = timedOut.Value;
        return node.ToJsonString();
    }

    [McpServerTool(Name = "peer_inbox"), Description(
        "Drain the durable inbox of directed messages addressed to this peer (review requests, replies, DMs). This is the primary, everyday way to read peer activity — call at startup, after each task chunk, before finishing.\n" +
        "Delivery: at-least-once, acknowledged by receipt. NOTHING is acknowledged by reading it. To acknowledge items, pass their inbox_id values as ack_inbox_ids on your NEXT peer_inbox or peer_wait call. Anything you do not echo back is returned AGAIN — so a result you never received (tool error, cancelled request) costs you a duplicate, never a lost message. Echo the ids once you have acted on them.\n" +
        "When NOT to use: peer_poll is for raw cursor/event debugging — prefer peer_inbox for normal collaboration.\n" +
        "Returns items[], each carrying the inbox_id you echo to acknowledge it. A large entry body arrives as a ~2000-char preview with content_truncated:true plus scratchpad_id/entry_id — call scratchpad_read for the full text. truncated:true means more are waiting: call peer_inbox again, echoing the ids you have finished with.")]
    public static async Task<string> Inbox(
        BrokerClient client,
        SessionState state,
        [Description("inbox_id values from a PREVIOUS peer_inbox/peer_wait result, to acknowledge them. Ids not echoed here stay in the inbox and are delivered again.")] string[]? ack_inbox_ids = null,
        [Description("Maximum number of durable inbox items to drain (default 100)")] int? limit = null,
        // SDK-bound and excluded from the tool's JSON schema, exactly like peer_wait's — defence in depth
        // only. The structural fix is the echoed receipt above; this merely stops work the client withdrew.
        // Both exclusions are pinned by PeerWaitTests.Inbox_ToolSchema_ExposesNoSdkBoundParameters.
        CancellationToken cancellationToken = default)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";

        try
        {
            // Before the read, so an id acknowledged now cannot come back in this same result.
            await AckEchoedAsync(client, state, ack_inbox_ids, cancellationToken);
            var resp = await client.GetInboxAsync(state.PeerId, limit ?? 100);
            return Shape(state, resp, null);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
    }

    [McpServerTool(Name = "peer_wait"), Description(
        "Long-poll: block up to timeout_seconds (default 60, max 120) until a directed message arrives, then drain it exactly like peer_inbox (same receipt-acknowledged at-least-once delivery, same size shaping).\n" +
        "Use this after requesting a review instead of repeatedly calling peer_inbox — it returns as soon as a message lands.\n" +
        "Acknowledging works the same way: reading acknowledges nothing, so pass the inbox_id values from your previous result as ack_inbox_ids. Ids you never echo are delivered again rather than lost.\n" +
        "Returns the same shape as peer_inbox, plus timed_out:true when it returns empty at the deadline.")]
    public static Task<string> Wait(
        BrokerClient client,
        SessionState state,
        [Description("Max seconds to block waiting for a directed event (default 60, max 120)")] int? timeout_seconds = null,
        [Description("inbox_id values from a PREVIOUS peer_inbox/peer_wait result, to acknowledge them. Ids not echoed here stay in the inbox and are delivered again.")] string[]? ack_inbox_ids = null,
        // The SDK binds this to the request's progressToken, or to a no-op when the client sent none, so it is
        // never null in production. It stays nullable only so direct (test) callers can skip progress entirely.
        IProgress<ProgressNotificationValue>? progress = null,
        // Bound by the SDK to a token that fires on notifications/cancelled for THIS request, and — like
        // progress above — kept out of the tool's JSON schema. Both exclusions are pinned by
        // PeerWaitTests.Wait_ToolSchema_ExposesDeclaredParametersOnly, so an SDK bump cannot leak either one.
        CancellationToken cancellationToken = default) =>
        WaitCore(client, state, timeout_seconds, ack_inbox_ids, progress, TickInterval, cancellationToken);

    /// <summary>Seconds credited per progress tick. Also the real tick cadence in production (spec: servers SHOULD rate-limit).</summary>
    private const int TickSeconds = 5;
    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(TickSeconds);

    // Separate from the tool method so tests can drive the cadence fast; a TimeSpan parameter on Wait itself
    // would show up in the tool's JSON schema.
    internal static async Task<string> WaitCore(
        BrokerClient client,
        SessionState state,
        int? timeout_seconds,
        string[]? ack_inbox_ids,
        IProgress<ProgressNotificationValue>? progress,
        TimeSpan tickInterval,
        CancellationToken cancellationToken = default)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        var t = Math.Clamp(timeout_seconds ?? 60, 1, 120);
        try
        {
            // Before the long poll, so an id acknowledged now cannot come back in this same result.
            await AckEchoedAsync(client, state, ack_inbox_ids, cancellationToken);

            var completed = false;
            // Linked, so a client cancellation stops the ticker as well as the broker call: a request the
            // client has abandoned must not keep emitting notifications/progress against its token.
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var ticker = TickAsync(progress, t, tickInterval, () => completed, cts.Token);
            InboxResponse? resp;
            try
            {
                resp = await client.WaitInboxAsync(state.PeerId, t, cancellationToken: cancellationToken);
            }
            finally
            {
                // Order matters: cancelling cannot stop a ticker already inside Report, so the flag goes first.
                completed = true;
                cts.Cancel();
                // Safe in a finally. TickAsync has exactly two failure sources — the cancelled Task.Delay and
                // IProgress.Report — and swallows both, so it cannot fault here and displace an in-flight
                // exception: neither a BrokerApiException the catch below shapes, nor a client cancellation
                // that must propagate. (`completed` is our own closure and cannot throw.)
                await ticker;
            }
            return Shape(state, resp, resp?.TimedOut ?? true);
        }
        catch (BrokerApiException ex) { return BrokerError(ex); }
        // No catch for OperationCanceledException: MCP 2026-07-28 says a server SHOULD NOT respond to a
        // cancelled request, and SDK 2.2.0 delivers that by suppressing the response when the handler
        // propagates. Composing a result here — timed_out or otherwise — would put a frame on the wire
        // for a request the client already withdrew.
    }

    /// <summary>
    /// Emits a progress notification every <paramref name="tickInterval"/> until cancelled, the wait completes,
    /// or the reported elapsed time would pass <paramref name="totalSeconds"/>. Returns normally on cancellation.
    /// </summary>
    internal static async Task TickAsync(
        IProgress<ProgressNotificationValue>? progress,
        int totalSeconds,
        TimeSpan tickInterval,
        Func<bool> completed,
        CancellationToken ct)
    {
        // Owned by the ticker and only ever incremented, so the reported progress is strictly increasing no
        // matter what the wall clock does. Not derived from DateTime/Stopwatch for exactly that reason.
        var elapsedSeconds = 0;
        try
        {
            while (true)
            {
                await Task.Delay(tickInterval, ct);
                elapsedSeconds += TickSeconds;
                if (elapsedSeconds > totalSeconds) return;
                // ponytail: check-then-act — `completed` can flip in the sub-millisecond gap before Report, so one
                // notification may still land just after the wait returns. Accepted: closing it needs a lock around
                // both the flag and every Report, which is a lot of machinery for one stray progress message.
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
                // IProgress<T>.Report carries no non-throwing guarantee. A throw must not fault this task:
                // WaitCore awaits it in a finally, where a fault would displace the real result — including
                // the shaped BrokerApiException JSON. One throw ends progress for this wait; the notification
                // is a nicety, the tool result is not.
                catch { return; }
            }
        }
        // The normal stop: WaitCore cancels us from its finally while we are parked on Task.Delay.
        catch (OperationCanceledException) { }
    }

    [McpServerTool(Name = "peer_test_push"), Description(
        "Diagnostic: attempt a push notification through this session's channel notifier and report what actually happened.\n" +
        "ok:true means a notification really went out, and message names the method used — if it does not surface in the client UI, the client is dropping it.\n" +
        "ok:false means nothing was sent, and message says why (most often: this protocol revision has no conforming unsolicited push, so delivery is via resource subscriptions or pull-mode peer_inbox).\n" +
        "Returns: { ok, message }, or { ok: false, error } if the notifier is not running or the push threw.")]
    public static async Task<string> TestPush(
        SessionState state,
        [Description("Optional test message content")] string? message = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        if (state.ChannelNotifier is null) return """{"error":"Channel notifier not running."}""";

        var content = message ?? $"Peers push test from peer {state.PeerId} at {DateTime.UtcNow:O}";

        try
        {
            // ok mirrors PushResult.Delivered, never the mere absence of a throw: suppression by protocol
            // revision is RETURNED, not thrown, and reporting it as success is the one failure mode this
            // tool cannot have. Detail is client-facing prose, surfaced verbatim.
            var push = await state.ChannelNotifier.SendTestNotificationAsync(content);
            return JsonSerializer.Serialize(new { ok = push.Delivered, message = push.Detail }, JsonOpts);
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new { ok = false, error = ex.Message }, JsonOpts);
        }
    }
}
