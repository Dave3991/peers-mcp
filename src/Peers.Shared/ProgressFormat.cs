namespace Peers.Shared;

/// <summary>
/// How peer work progress reads. Shared because the MCP wait ticker and the CLI both render it and
/// both need the same answer to "is this still moving?" — two copies of a two-minute literal in two
/// projects drift, and a stale reading is worse than none.
/// </summary>
public static class ProgressFormat
{
    /// <summary>
    /// How long a progress report stays believable. Matches PeerService.LeaseDuration BY INTENT —
    /// progress older than one lease is not moving — and deliberately is NOT that constant: liveness
    /// and progress-freshness are different questions that happen to share a number today.
    /// </summary>
    public static readonly TimeSpan FreshnessWindow = TimeSpan.FromMinutes(2);

    /// <summary>
    /// "7/12", or "7" when the total is unknown, or "" when nothing is being tracked.
    ///
    /// An over-count is CLAMPED, not rejected: a run that discovers more work mid-flight (a test sweep
    /// finding extra cases) would otherwise have to choose between a wrong total and a 400 in the
    /// middle of its task. Showing 12/12 is the honest reading of "at or past the estimate".
    /// </summary>
    public static string Counts(int? current, int? total)
    {
        if (current is not { } c)
            return "";
        if (total is not { } t)
            return c.ToString();
        return $"{Math.Min(c, t)}/{t}";
    }

    /// <summary>True when progress moved recently enough to be worth showing as live.</summary>
    public static bool IsFresh(DateTime? progressUpdatedAt, DateTime utcNow) =>
        progressUpdatedAt is { } at && utcNow - at <= FreshnessWindow;

    /// <summary>Counts for a human-facing list, with an age suffix once they go stale.</summary>
    public static string CountsWithAge(int? current, int? total, DateTime? progressUpdatedAt, DateTime utcNow)
    {
        var counts = Counts(current, total);
        if (counts.Length == 0 || IsFresh(progressUpdatedAt, utcNow))
            return counts;

        var age = utcNow - (progressUpdatedAt ?? utcNow);
        var minutes = (int)age.TotalMinutes;
        return minutes >= 60 ? $"{counts} ({minutes / 60}h ago)" : $"{counts} ({minutes}m ago)";
    }
}
