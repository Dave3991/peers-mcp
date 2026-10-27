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
        "Managed review: publish a PLAN/CODE packet, fetch its evidence, or reply as the selected reviewer.")]
    public static async Task<string> Packet(BrokerClient client, SessionState state, McpServer server,
        ILoggerFactory loggerFactory, ReviewStorageOptions storage,
        [Description(
            "publish (driver: needs request): publish a complete, immutable PLAN or CODE packet for independent review. PLAN approval precedes CODE.\n" +
            "fetch (needs packet_id): fetch the packet's verified private evidence files. A fetch never approves; evidence claims are not executed.\n" +
            "reply (selected reviewer: needs packet_id and reply): answer the packet as its exact selected reviewer.\n" +
            "Exact, lowercase."), AllowedValues("publish", "fetch", "reply")] string action,
        [Description("The review channel's channel_id.")] string channel_id,
        [Description("fetch/reply: the packet_id to fetch or answer; reply answers only the latest packet.")] string? packet_id = null,
        [Description(
            "publish only: the PLAN or CODE request. evidence[].kind is build, test, smoke, or other. " +
            "The capture includes non-ignored untracked files up to 1 MiB each and 4 MiB in total; a larger one fails the capture and names the file. " +
            "So does an untracked file that looks like a secret: .env, .envrc, .env.* (not *.example/*.sample/*.template), *.env, *.pem, *.key, *.p12, *.pfx, id_rsa*, id_ed25519*, " +
            "or content containing 'PRIVATE KEY-----' or 'PRIVATE KEY BLOCK-----'.")] ReviewRequest? request = null,
        [Description("reply only: verdict APPROVE or CHANGES and findings whose severity is HIGH, MED, or LOW (both case-insensitive).")] ReviewReply? reply = null,
        CancellationToken cancellationToken = default)
    {
        ReviewJson.RequireText("channel_id", channel_id);
        ToolArgs.RequireAction(action, PacketActions);
        ToolArgs.RequirePayload(action, "request", request is not null, required: action == "publish");
        ToolArgs.RequirePayload(action, "packet_id", packet_id is not null, required: action is "fetch" or "reply");
        ToolArgs.RequirePayload(action, "reply", reply is not null, required: action == "reply");
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

    [McpServerTool(Name = "review_checkpoint"), Description(
        "Managed review state: status, receipt sync, pause/resume, or completion of the approved CODE.")]
    public static async Task<string> Checkpoint(BrokerClient client, SessionState state, McpServer server,
        ILoggerFactory loggerFactory, ReviewStorageOptions storage,
        [Description(
            "status: restore managed review state from authoritative channel history and report the source revision.\n" +
            "sync: sync explicitly handled receipts.\n" +
            "complete (needs expected_packet_id): gate terminal completion of the approved CODE packet. A blocked gate returns completed:false; input, lock, and broker faults are tool errors. " +
            "Completion after CHANGES rounds adds a non-authoritative lesson_hint that saves nothing.\n" +
            "pause (needs reason). resume: give a reason to explicitly adopt an offline driver.\n" +
            "Exact, lowercase."), AllowedValues("status", "sync", "complete", "pause", "resume")] string action,
        [Description("The review channel's channel_id.")] string channel_id,
        [Description("Optional Git working tree of the review (default: this session's repo).")] string? source_root = null,
        [Description("complete only (required there): the approved CODE packet_id.")] string? expected_packet_id = null,
        [Description("sync only: ready_to_ack IDs returned by a previous sync. Acknowledge other receipts with wait.")] string[]? ack_inbox_ids = null,
        [Description("pause: required. resume: optional (give one to adopt an offline driver). Other actions: omit.")] string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ReviewJson.RequireText("channel_id", channel_id);
        ToolArgs.RequireAction(action, CheckpointActions);
        ToolArgs.RequirePayload(action, "expected_packet_id", expected_packet_id is not null, required: action == "complete");
        ReviewJson.Require(ack_inbox_ids is null || action == "sync",
            $"ack_inbox_ids lists {ack_inbox_ids?.Length} ID(s), but action \"{action}\" takes none; omit it. " +
            "Acknowledge ready_to_ack IDs with a separate review_checkpoint action \"sync\" call, and other receipts with wait ack_inbox_ids.");
        if (action == "pause") ReviewJson.RequireText("reason", reason);
        else if (action != "resume") ToolArgs.RequirePayload(action, "reason", reason is not null, required: false);
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

    private static readonly string[] CheckpointActions = ["status", "sync", "complete", "pause", "resume"];

    [McpServerTool(Name = "review_lesson"), Description(
        "Promote one reviewed lesson after intact CODE completion: prepare, begin_save, record, reconcile.")]
    public static async Task<string> Lesson(BrokerClient client, SessionState state, McpServer server,
        ILoggerFactory loggerFactory, ReviewStorageOptions storage,
        [Description(
            "Explicit, optional promotion after intact terminal CODE review.\n" +
            "prepare (needs proposal): freeze one supported before/after lesson.\n" +
            "begin_save (needs binding_result): grant one durable dispatch through the existing Aletheia tools.\n" +
            "record (needs attempt_id and result) and reconcile (needs binding_result and result.observations): handle ambiguity and visible duplicates.\n" +
            "The tool never saves memory itself and does not promise exactly-once storage."),
         AllowedValues("prepare", "begin_save", "record", "reconcile")] string action,
        string channel_id,
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
