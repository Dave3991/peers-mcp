using System.Text;

namespace Peers.Broker.Validation;

public static class RequestLimits
{
    public const int MaxEntryContentBytes = 100 * 1024;     // 100 KB
    public const int MaxArtifactContentBytes = 1024 * 1024; // 1 MB
    public const int MaxTitleLength = 500;
    public const int MaxSummaryLength = 2000;
    public const int MaxJsonArrayBytes = 10 * 1024; // 10 KB

    private static int Utf8Bytes(string? s) => s is null ? 0 : Encoding.UTF8.GetByteCount(s);

    public const int MaxChannelNameLength = 100;
    public const int MaxMentionCount = 50;
    public const int MaxIdempotencyKeyLength = 128;
    public const int MaxClaimPathCount = 100;
    public const int MaxClaimPathLength = 1024;
    public const int MaxRemoveMessageCount = 200;

    public static IResult? ValidateChannelCreate(Shared.Contracts.CreateChannelRequest req)
    {
        if (string.IsNullOrWhiteSpace(req.Name) || req.Name.Length > MaxChannelNameLength)
            return Results.BadRequest(new { error = $"Channel name must be 1..{MaxChannelNameLength} characters." });
        return null;
    }

    public static IResult? ValidateSendMessage(Shared.Contracts.SendMessageRequest req)
    {
        if (Utf8Bytes(req.Content) > MaxEntryContentBytes)
            return Results.Json(new { error = $"Message content exceeds {MaxEntryContentBytes / 1024} KB." }, statusCode: 413);
        if (req.To is { Length: > MaxMentionCount })
            return Results.BadRequest(new { error = $"A message can mention at most {MaxMentionCount} peers." });
        if (req.IdempotencyKey?.Length > MaxIdempotencyKeyLength)
            return Results.BadRequest(new { error = $"Idempotency key exceeds {MaxIdempotencyKeyLength} characters." });
        return null;
    }

    public static IResult? ValidateRemoveMessages(Shared.Contracts.RemoveMessagesRequest req)
    {
        if (req.MessageIds is not { Length: > 0 and <= MaxRemoveMessageCount }
            || req.MessageIds.Any(string.IsNullOrWhiteSpace))
            return Results.BadRequest(new { error = $"Provide 1..{MaxRemoveMessageCount} nonblank message IDs." });
        return null;
    }

    public static IResult? ValidateClaim(Shared.Contracts.ClaimPathsRequest req)
    {
        if (req.Paths.Length == 0)
            return Results.BadRequest(new { error = "Claim at least one path." });
        if (req.Paths.Length > MaxClaimPathCount)
            return Results.BadRequest(new { error = $"A claim covers at most {MaxClaimPathCount} paths; claim the directory instead." });
        if (req.Paths.Any(path => path?.Length > MaxClaimPathLength))
            return Results.BadRequest(new { error = $"A claimed path exceeds {MaxClaimPathLength} characters." });
        return null;
    }

    public static IResult? ValidatePublishArtifact(Shared.Contracts.PublishArtifactRequest req)
    {
        if (Utf8Bytes(req.Content) > MaxArtifactContentBytes)
            return Results.Json(new { error = $"Artifact content exceeds {MaxArtifactContentBytes / 1024} KB." }, statusCode: 413);
        if (Utf8Bytes(req.Tags) > MaxJsonArrayBytes)
            return Results.BadRequest(new { error = $"Tags JSON exceeds {MaxJsonArrayBytes} bytes." });
        return null;
    }

    public static IResult? ValidateRegisterPeer(Shared.Contracts.RegisterPeerRequest req)
    {
        if (req.Summary?.Length > MaxSummaryLength)
            return Results.BadRequest(new { error = $"Summary exceeds {MaxSummaryLength} characters." });
        if (Utf8Bytes(req.FocusPaths) > MaxJsonArrayBytes)
            return Results.BadRequest(new { error = $"Focus paths JSON exceeds {MaxJsonArrayBytes} bytes." });
        if (Utf8Bytes(req.Capabilities) > MaxJsonArrayBytes)
            return Results.BadRequest(new { error = $"Capabilities JSON exceeds {MaxJsonArrayBytes} bytes." });
        return null;
    }

    /// <summary>
    /// Mirrors the register limits, because a heartbeat writes the same mutable columns — until this
    /// existed the route validated nothing at all, so summary length and the JSON array caps were
    /// enforced on register and then freely exceeded on every update.
    ///
    /// An over-count (current > total) is deliberately NOT rejected: a run that discovers more work
    /// mid-flight would get a 400 in the middle of its task, which is worse than an odd-looking number.
    /// ProgressFormat clamps the display instead.
    /// </summary>
    public static IResult? ValidateHeartbeat(Shared.Contracts.HeartbeatRequest req)
    {
        if (req.Summary?.Length > MaxSummaryLength)
            return Results.BadRequest(new { error = $"Summary exceeds {MaxSummaryLength} characters." });
        if (Utf8Bytes(req.FocusPaths) > MaxJsonArrayBytes)
            return Results.BadRequest(new { error = $"Focus paths JSON exceeds {MaxJsonArrayBytes} bytes." });
        if (Utf8Bytes(req.Capabilities) > MaxJsonArrayBytes)
            return Results.BadRequest(new { error = $"Capabilities JSON exceeds {MaxJsonArrayBytes} bytes." });
        // null < 0 is false, so both guards are null-safe.
        if (req.ProgressCurrent < 0 || req.ProgressTotal < 0)
            return Results.BadRequest(new { error = "Progress counts must be zero or greater." });
        if (req.ProgressTotal is not null && req.ProgressCurrent is null)
            return Results.BadRequest(new { error = "progress_total requires progress_current; on its own it would be silently ignored." });
        return null;
    }
}
