using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Mcp;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Tools;

[McpServerToolType]
public sealed class MessageTools
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    [McpServerTool(Name = "peer_message"), Description(
        "Send a lightweight directed message to one or more peers (peer-id prefixes or repo-root paths) WITHOUT creating a scratchpad.\n" +
        "It lands in each recipient's peer_inbox (and pushes unless quiet=true), resolving targets the same way scratchpad reviewers do (prefix match, repo-root fallback, restarted-reviewer reroute). The sender never receives their own message.\n" +
        "When NOT to use: for longer async threads or code reviews that need replies, use scratchpad_create.\n" +
        "Returns: { recipients (online/offline/unresolved), delivery_summary }.")]
    public static async Task<string> Send(
        BrokerClient client,
        SessionState state,
        [Description("Targets: peer-id prefixes and/or repo-root paths to deliver to")] string[] targets,
        [Description("Message content (markdown, <= 100 KB)")] string content,
        [Description("Suppress push notification (still delivered to durable inbox)")] bool? quiet = null)
    {
        if (state.PeerId is null) return """{"error":"Not registered. Call peer_register first."}""";
        try
        {
            var resp = await client.SendMessageAsync(new SendMessageRequest
            {
                FromPeerId = state.PeerId,
                Targets = targets,
                Content = content,
                Quiet = quiet ?? false,
            });
            return JsonSerializer.Serialize(new
            {
                recipients = resp.Recipients,
                delivery_summary = ScratchpadTools.FormatDeliverySummary(resp.Recipients),
            }, JsonOpts);
        }
        catch (BrokerApiException ex)
        {
            return JsonSerializer.Serialize(new { error = ex.Message, status_code = (int)ex.StatusCode }, JsonOpts);
        }
    }
}
