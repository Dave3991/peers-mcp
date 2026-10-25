using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Peers.Mcp.Lifecycle;
using Peers.Mcp.Reviews;

namespace Peers.Mcp.Tools;

[McpServerToolType]
internal sealed class ReviewTools
{
    [McpServerTool(Name = "review_packet"), Description(
        "Managed independent review: publish a complete immutable PLAN/CODE packet, fetch its verified private evidence files, or reply as its exact selected reviewer. PLAN approval precedes CODE. Fetch never approves; evidence claims are not executed. Non-ignored untracked files are captured up to 1 MiB each and 4 MiB in total; a larger one fails the capture and names the file. So does an untracked file that looks like a secret: .env or .env.* (not *.example/*.sample/*.template), *.pem, *.key, *.p12, *.pfx, id_rsa*, id_ed25519*, or content containing 'PRIVATE KEY-----'.")]
    public static async Task<string> Packet(BrokerClient client, SessionState state, McpServer server,
        ILoggerFactory loggerFactory, ReviewStorageOptions storage,
        [Description("publish (driver: needs request), fetch (needs packet_id), or reply (selected reviewer: needs packet_id and reply). Exact, lowercase."), AllowedValues("publish", "fetch", "reply")] string action,
        [Description("The review channel's channel_id.")] string channel_id,
        [Description("fetch/reply: the packet_id to fetch or answer; reply answers only the latest packet.")] string? packet_id = null,
        [Description("publish only: the PLAN or CODE request. evidence[].kind is build, test, smoke, or other.")] ReviewRequest? request = null,
        [Description("reply only: verdict APPROVE or CHANGES and findings whose severity is HIGH, MED, or LOW (both case-insensitive).")] ReviewReply? reply = null,
        CancellationToken cancellationToken = default)
    {
        ReviewJson.RequireText("channel_id", channel_id);
        ReviewJson.RequireOneOf("action", action, PacketActions);
        RequirePayload(action, "request", request is not null, required: action == "publish");
        RequirePayload(action, "packet_id", packet_id is not null, required: action is "fetch" or "reply");
        RequirePayload(action, "reply", reply is not null, required: action == "reply");
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            var workflow = new ReviewWorkflow(client, state, storage, channel_id);
            var result = action switch
            {
                "publish" => await workflow.PublishAsync(request!, cancellationToken),
                "fetch" => await workflow.FetchAsync(packet_id!, cancellationToken),
                _ => await workflow.ReplyAsync(packet_id!, reply!, cancellationToken),
            };
            return ReviewJson.Text(result);
        }
        catch (BrokerApiException ex) { throw new McpException(ex.Message, ex); }
        catch (JsonException ex) { throw new McpException("Invalid review evidence JSON.", ex); }
        catch (IOException ex) { throw new McpException("Review filesystem operation failed: " + ex.Message, ex); }
    }

    private static readonly string[] PacketActions = ["publish", "fetch", "reply"];
    private static void RequirePayload(string action, string field, bool present, bool required) =>
        ReviewJson.Require(present == required, required
            ? $"{field} is missing; action \"{action}\" requires it."
            : $"{field} must be omitted for action \"{action}\".");

    [McpServerTool(Name = "review_checkpoint"), Description(
        "Restore managed review state from authoritative channel history, inspect source revision, sync explicitly handled receipts, pause/resume (including explicit offline-driver adoption), or gate terminal CODE completion. A blocked gate returns completed:false; input/lock/broker faults are tool errors. Completion after CHANGES rounds adds a non-authoritative lesson_hint that saves nothing.")]
    public static async Task<string> Checkpoint(BrokerClient client, SessionState state, McpServer server,
        ILoggerFactory loggerFactory, ReviewStorageOptions storage, string action, string channel_id,
        string? source_root = null, string? expected_packet_id = null, string[]? ack_inbox_ids = null,
        string? reason = null, CancellationToken cancellationToken = default)
    {
        ReviewJson.Require(!string.IsNullOrWhiteSpace(channel_id) && action is "status" or "sync" or "complete" or "pause" or "resume",
            "Invalid review_checkpoint action/channel_id.");
        ReviewJson.Require((action == "complete" ? expected_packet_id is not null : expected_packet_id is null) &&
            (action == "sync" || ack_inbox_ids is null) &&
            (action is "pause" or "resume" || reason is null) &&
            (action != "pause" || !string.IsNullOrWhiteSpace(reason)), "Irrelevant or missing review_checkpoint parameters.");
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            return ReviewJson.Text(await new ReviewWorkflow(client, state, storage, channel_id)
                .CheckpointAsync(action, source_root, expected_packet_id, ack_inbox_ids, reason, cancellationToken));
        }
        catch (BrokerApiException ex) { throw new McpException(ex.Message, ex); }
        catch (JsonException ex) { throw new McpException("Invalid review checkpoint JSON.", ex); }
        catch (IOException ex) { throw new McpException("Review filesystem operation failed: " + ex.Message, ex); }
    }

    [McpServerTool(Name = "review_lesson"), Description(
        "Explicit optional promotion after intact terminal CODE review. prepare freezes one supported before/after lesson; begin_save grants one durable dispatch through existing Aletheia tools; record and reconcile handle ambiguity and visible duplicates. Never saves memory itself or promises exactly-once storage.")]
    public static async Task<string> Lesson(BrokerClient client, SessionState state, McpServer server,
        ILoggerFactory loggerFactory, ReviewStorageOptions storage, string action, string channel_id,
        ReviewLessonProposal? proposal = null, JsonElement? binding_result = null, string? attempt_id = null,
        ReviewLessonResult? result = null, CancellationToken cancellationToken = default)
    {
        ReviewJson.Require(!string.IsNullOrWhiteSpace(channel_id) && action switch
        {
            "prepare" => proposal is not null && binding_result is null && attempt_id is null && result is null,
            "begin_save" => proposal is null && binding_result is not null && attempt_id is null && result is null,
            "record" => proposal is null && binding_result is null && attempt_id is not null && result is not null &&
                result.Observations is null && (result.Failure is not null
                    ? result.SaveResult is null && result.Observation is null
                    : result.SaveResult is not null && result.Observation is not null),
            "reconcile" => proposal is null && binding_result is not null && result?.Observations is not null &&
                result.SaveResult is null && result.Observation is null && result.Failure is null,
            _ => false,
        }, "Invalid review_lesson action or contradictory payloads.");
        await SessionRegistration.EnsureRegisteredAsync(client, state, server, loggerFactory, cancellationToken);
        try
        {
            return ReviewJson.Text(await new ReviewWorkflow(client, state, storage, channel_id)
                .LessonAsync(action, proposal, binding_result, attempt_id, result, cancellationToken));
        }
        catch (BrokerApiException ex) { throw new McpException(ex.Message, ex); }
        catch (JsonException ex) { throw new McpException("Invalid lesson result JSON.", ex); }
        catch (IOException ex) { throw new McpException("Review filesystem operation failed: " + ex.Message, ex); }
    }
}
