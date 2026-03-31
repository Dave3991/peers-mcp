namespace Peers.Broker.Validation;

public static class RequestLimits
{
    public const int MaxEntryContentBytes = 100 * 1024;     // 100 KB
    public const int MaxArtifactContentBytes = 1024 * 1024; // 1 MB
    public const int MaxTitleLength = 500;
    public const int MaxSummaryLength = 2000;
    public const int MaxJsonArrayLength = 10 * 1024; // 10 KB

    public static IResult? ValidateScratchpadCreate(Shared.Contracts.CreateScratchpadRequest req)
    {
        if (req.Title.Length > MaxTitleLength)
            return Results.BadRequest(new { error = $"Title exceeds {MaxTitleLength} characters." });
        if (req.RequestedReviewers?.Length > MaxJsonArrayLength)
            return Results.BadRequest(new { error = $"Requested reviewers JSON exceeds {MaxJsonArrayLength} bytes." });
        if (req.ArtifactRefs?.Length > MaxJsonArrayLength)
            return Results.BadRequest(new { error = $"Artifact refs JSON exceeds {MaxJsonArrayLength} bytes." });
        return null;
    }

    public static IResult? ValidateAppendEntry(Shared.Contracts.AppendEntryRequest req)
    {
        if (req.Content.Length > MaxEntryContentBytes)
            return Results.Json(new { error = $"Entry content exceeds {MaxEntryContentBytes / 1024} KB." }, statusCode: 413);
        return null;
    }

    public static IResult? ValidatePublishArtifact(Shared.Contracts.PublishArtifactRequest req)
    {
        if (req.Content.Length > MaxArtifactContentBytes)
            return Results.Json(new { error = $"Artifact content exceeds {MaxArtifactContentBytes / 1024} KB." }, statusCode: 413);
        if (req.Tags?.Length > MaxJsonArrayLength)
            return Results.BadRequest(new { error = $"Tags JSON exceeds {MaxJsonArrayLength} bytes." });
        return null;
    }

    public static IResult? ValidateRegisterPeer(Shared.Contracts.RegisterPeerRequest req)
    {
        if (req.Summary?.Length > MaxSummaryLength)
            return Results.BadRequest(new { error = $"Summary exceeds {MaxSummaryLength} characters." });
        if (req.FocusPaths?.Length > MaxJsonArrayLength)
            return Results.BadRequest(new { error = $"Focus paths JSON exceeds {MaxJsonArrayLength} bytes." });
        return null;
    }
}
