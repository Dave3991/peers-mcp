using System.ComponentModel;
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
        "Managed independent review: publish a complete immutable PLAN/CODE packet, fetch its verified private evidence files, or reply as its exact selected reviewer. PLAN approval precedes CODE. Fetch never approves; evidence claims are not executed.")]
    public static async Task<string> Packet(BrokerClient client, SessionState state, McpServer server,
        ILoggerFactory loggerFactory, ReviewStorageOptions storage,
        string action, string channel_id, string? packet_id = null, ReviewRequest? request = null,
        ReviewReply? reply = null, CancellationToken cancellationToken = default)
    {
        ReviewJson.Require(!string.IsNullOrWhiteSpace(channel_id), "channel_id is required.");
        ReviewJson.Require(action switch
        {
            "publish" => request is not null && packet_id is null && reply is null,
            "fetch" => packet_id is not null && request is null && reply is null,
            "reply" => packet_id is not null && request is null && reply is not null,
            _ => false,
        }, "Invalid review_packet action or contradictory payloads.");
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

    [McpServerTool(Name = "review_checkpoint"), Description(
        "Restore managed review state from authoritative channel history, inspect source revision, sync explicitly handled receipts, pause/resume (including explicit offline-driver adoption), or gate terminal CODE completion. A blocked gate returns completed:false; input/lock/broker faults are tool errors.")]
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
