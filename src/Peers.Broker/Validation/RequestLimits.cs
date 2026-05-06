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

    public static IResult? ValidateScratchpadCreate(Shared.Contracts.CreateScratchpadRequest req)
    {
        if (req.Title.Length > MaxTitleLength)
            return Results.BadRequest(new { error = $"Title exceeds {MaxTitleLength} characters." });
        if (Utf8Bytes(req.RequestedReviewers) > MaxJsonArrayBytes)
            return Results.BadRequest(new { error = $"Requested reviewers JSON exceeds {MaxJsonArrayBytes} bytes." });
        if (Utf8Bytes(req.ArtifactRefs) > MaxJsonArrayBytes)
            return Results.BadRequest(new { error = $"Artifact refs JSON exceeds {MaxJsonArrayBytes} bytes." });
        return null;
    }

    public static IResult? ValidateAppendEntry(Shared.Contracts.AppendEntryRequest req)
    {
        if (Utf8Bytes(req.Content) > MaxEntryContentBytes)
            return Results.Json(new { error = $"Entry content exceeds {MaxEntryContentBytes / 1024} KB." }, statusCode: 413);
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
}
