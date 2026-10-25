using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using ModelContextProtocol;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Reviews;

internal static class ReviewJson
{
    internal static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
        WriteIndented = false,
    };
    internal static byte[] Bytes<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, Options);
    internal static string Text<T>(T value) => JsonSerializer.Serialize(value, Options);
    internal static T Parse<T>(byte[] value) => JsonSerializer.Deserialize<T>(value, Options)
        ?? throw new McpException("Missing review JSON payload.");
    internal static string Hash(ReadOnlySpan<byte> value) => Convert.ToHexStringLower(SHA256.HashData(value));
    internal static string Hash(string value) => Hash(Encoding.UTF8.GetBytes(value));
    internal static void Require([System.Diagnostics.CodeAnalysis.DoesNotReturnIf(false)] bool condition, string message)
    {
        if (!condition) throw new McpException(message);
    }

    // Input validation names the field, the value it got, and what it accepts: an agent can only fix a
    // call whose error says which value to change. A bare "Invalid review finding" made agents guess.
    private static readonly JsonSerializerOptions ShowOptions = new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };
    internal static string Show(string? value) => value is null ? "null" :
        JsonSerializer.Serialize(value.Length <= 80 ? value : value[..80] + "…", ShowOptions);
    // A missing input property binds as "" (the DTOs mark required fields for the schema only), so
    // missing and empty read the same.
    private static string Got(string? value) => string.IsNullOrEmpty(value) ? "missing or empty" : Show(value);
    internal static void RequireText(string field, string? value) =>
        Require(!string.IsNullOrWhiteSpace(value), $"{field} is {Got(value)}; it must be non-blank text.");
    /// <summary>The canonical allowed spelling of <paramref name="value"/>, or an error that lists every allowed value.</summary>
    internal static string RequireOneOf(string field, string? value, string[] allowed, bool ignoreCase = false)
    {
        var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        foreach (var candidate in allowed)
            if (string.Equals(candidate, value, comparison)) return candidate;
        throw new McpException($"{field} is {Got(value)}; allowed values: {string.Join(", ", allowed)}{(ignoreCase ? " (case-insensitive)" : "")}.");
    }
    internal static readonly string[] Phases = ["PLAN", "CODE"];
    internal static readonly string[] EvidenceKinds = ["build", "test", "smoke", "other"];
    internal static readonly string[] Verdicts = ["APPROVE", "CHANGES"];
    internal static readonly string[] Severities = ["HIGH", "MED", "LOW"];
}

// Input DTOs mark required properties with [Required], not [JsonRequired]. [Required] only shapes the
// published JSON schema; [JsonRequired] would make a missing property fail argument binding inside the
// SDK, which answers with an opaque "An error occurred invoking" instead of a message naming the field.
// The workflow validates every field itself.
internal sealed class ReviewRequest
{
    [JsonPropertyName("phase"), Required, Description("PLAN or CODE (exact). PLAN/1 comes first; CODE needs the current reviewer's approved PLAN."), AllowedValues("PLAN", "CODE")]
    public string Phase { get; set; } = "";
    [JsonPropertyName("round"), Required, Description("Positive round number, counted per phase: the next PLAN or CODE round.")]
    public int Round { get; set; }
    [JsonPropertyName("task"), Required] public string Task { get; set; } = "";
    [JsonPropertyName("base_sha"), Required] public string BaseSha { get; set; } = "";
    [JsonPropertyName("reviewer_peer_id"), Required, Description("Exact peer_id of a different live peer (see `peers`).")]
    public string ReviewerPeerId { get; set; } = "";
    [JsonPropertyName("change_summary"), Required] public string ChangeSummary { get; set; } = "";
    [JsonPropertyName("uncertainties"), Required] public string Uncertainties { get; set; } = "";
    [JsonPropertyName("expected_previous_packet_id"), Description("packet_id of the latest packet of either phase; omit before the first request.")]
    public string? ExpectedPreviousPacketId { get; set; }
    [JsonPropertyName("source_root")] public string? SourceRoot { get; set; }
    [JsonPropertyName("plan_path"), Description("PLAN only: absolute path of a regular plan file.")]
    public string? PlanPath { get; set; }
    [JsonPropertyName("verification_revision"), Description("CODE only: revision_sha256 from review_checkpoint status, taken before the checks ran.")]
    public string? VerificationRevision { get; set; }
    [JsonPropertyName("evidence"), Description("Raw output files of the checks; CODE needs at least one entry with kind test or smoke.")]
    public ReviewEvidence[] Evidence { get; set; } = [];
}
internal sealed class ReviewEvidence
{
    [JsonPropertyName("path"), Required, Description("Absolute path of a regular file holding the raw output.")]
    public string Path { get; set; } = "";
    [JsonPropertyName("kind"), Required, Description("build, test, smoke, or other (exact, lowercase)."), AllowedValues("build", "test", "smoke", "other")]
    public string Kind { get; set; } = "";
    [JsonPropertyName("command"), Required] public string Command { get; set; } = "";
    [JsonPropertyName("cwd"), Required, Description("Absolute directory the command ran in.")]
    public string Cwd { get; set; } = "";
    // Nullable so a missing exit code is reported, not silently recorded as a success.
    [JsonPropertyName("exit_code"), Required, Description("The command's integer exit code.")]
    public int? ExitCode { get; set; }
}
internal sealed class ReviewReply
{
    [JsonPropertyName("verdict"), Required, Description("APPROVE or CHANGES (case-insensitive). APPROVE allows only optional findings; CHANGES needs at least one blocking finding (optional: false)."), AllowedValues("APPROVE", "CHANGES")]
    public string Verdict { get; set; } = "";
    [JsonPropertyName("summary"), Required] public string Summary { get; set; } = "";
    [JsonPropertyName("findings"), Required, Description("Findings; an empty array is a clean APPROVE.")]
    public ReviewFinding[] Findings { get; set; } = [];
}
internal sealed class ReviewFinding
{
    [JsonPropertyName("severity"), Required, Description("HIGH, MED, or LOW (case-insensitive)."), AllowedValues("HIGH", "MED", "LOW")]
    public string Severity { get; set; } = "";
    [JsonPropertyName("location"), Required] public string Location { get; set; } = "";
    [JsonPropertyName("finding"), Required] public string Finding { get; set; } = "";
    [JsonPropertyName("fix"), Required] public string Fix { get; set; } = "";
    [JsonPropertyName("optional"), Description("true for a non-blocking finding; default false (blocking).")]
    public bool Optional { get; set; }
}
internal sealed record ReviewPart(string ArtifactId, int Version, string ContentHash, int ByteLength);
internal sealed record ReviewBlob(string Encoding, string Sha256, long ByteLength, ReviewPart[] Parts);
internal sealed record ReviewPayloadFile(string Path, string? OriginalPath, string Kind, bool Executable,
    ReviewEvidence? Evidence, ReviewBlob Blob);
internal sealed record ReviewPayloadIndex(int ProtocolVersion, string ChangeSummary, string Uncertainties,
    ReviewPayloadFile[] Files);
internal sealed record ReviewManifest(int ProtocolVersion, string ChannelId, string DriverPeerId, string ReviewerPeerId,
    string Phase, int Round, string SourceRoot, string GitDir, string BaseSha, string HeadSha, string RevisionSha256,
    string TaskSha256, string? ApprovedPlanPacketId, ReviewBlob PayloadIndex);
internal sealed record ReviewMessageRef(string MessageId, long Seq, string Sha256, string AuthorPeerId);
internal sealed class ReviewPacketState
{
    public string PacketId { get; set; } = "";
    public string PacketSha256 { get; set; } = "";
    public ReviewManifest Manifest { get; set; } = null!;
    public string InputSha256 { get; set; } = "";
    public ReviewMessageRef? Request { get; set; }
    public ReviewMessageRef? Verdict { get; set; }
    public string? VerdictValue { get; set; }
    public RecipientDeliveryStatus? Delivery { get; set; }
}
internal sealed class ReviewPublication
{
    public string InputSha256 { get; set; } = "";
    public string? PacketId { get; set; }
    public string? PacketSha256 { get; set; }
    public ReviewManifest? Manifest { get; set; }
    public string? Content { get; set; }
    public bool Dispatched { get; set; }
    public List<ReviewPart> ConfirmedParts { get; set; } = [];
}
internal sealed class ReviewLedger
{
    public int ProtocolVersion { get; set; } = 1;
    public string ChannelId { get; set; } = "";
    public string TaskSha256 { get; set; } = "";
    public string SourceRoot { get; set; } = "";
    public string GitDir { get; set; } = "";
    public string BaseSha { get; set; } = "";
    public string DriverPeerId { get; set; } = "";
    public string ReviewerPeerId { get; set; } = "";
    public string State { get; set; } = "awaiting_review";
    public string? PrePauseState { get; set; }
    public bool Stale { get; set; }
    public string Integrity { get; set; } = "intact";
    public int PlanRound { get; set; }
    public int CodeRound { get; set; }
    public string? ApprovedPlanPacketId { get; set; }
    public string? LatestPacketId { get; set; }
    public long AppliedThroughSeq { get; set; }
    public List<ReviewPacketState> Packets { get; set; } = [];
    public ReviewPublication? Publication { get; set; }
    public string? VerificationRevision { get; set; }
    public List<string> DriverLineage { get; set; } = [];
    public int ControlCounter { get; set; }
    public ReviewControlIntent? ControlIntent { get; set; }
    public List<ReviewMessageRef> Controls { get; set; } = [];
    public ReviewMessageRef? Completion { get; set; }
    public string? CompletedRevision { get; set; }
    public ReviewLessonIntent? Lesson { get; set; }
    [JsonIgnore] public string? MirrorId { get; set; }
}
internal sealed class ReviewControlIntent
{
    public int Number { get; set; }
    public string Content { get; set; } = "";
    public long AfterSeq { get; set; }
    public bool Dispatched { get; set; }
}
internal sealed record ReviewReceipt(string PeerId, string InboxId, string EventType, bool Acknowledged);
internal sealed class ReviewPeerState
{
    public List<ReviewReceipt> Receipts { get; set; } = [];
    public ReviewLedger? Projection { get; set; }
    public List<ReviewReplyIntent> Replies { get; set; } = [];
}
internal sealed class ReviewReplyIntent
{
    public string PacketId { get; set; } = "";
    public string Content { get; set; } = "";
    public string TargetPeerId { get; set; } = "";
    public ReviewMessageRef? Message { get; set; }
    public RecipientDeliveryStatus? Delivery { get; set; }
}
internal sealed record ReviewMirror(int ProtocolVersion, long Generation, string PayloadSha256, ReviewBlob Payload);
internal sealed record ReviewMaterialization(ReviewManifest Manifest, ReviewPayloadIndex Index,
    string PacketSha256, string ManifestPath, string MapPath, ReviewMaterializedFile[] Files);
internal sealed record ReviewMaterializedFile(string Path, string LocalPath, string Kind, ReviewEvidence? Evidence,
    ReviewBlob Blob);
// Lesson intent is mirrored with control state, never inferred from notifications.
internal sealed class ReviewLessonIntent
{
    public string LessonId { get; set; } = "";
    public string ProposalSha256 { get; set; } = "";
    public string Marker { get; set; } = "";
    public string Body { get; set; } = "";
    public string[] Tags { get; set; } = [];
    public string Scope { get; set; } = "project";
    public string Status { get; set; } = "prepared";
    public string? AttemptId { get; set; }
    public string? WorkspaceBindingId { get; set; }
    public JsonElement? Binding { get; set; }
    public JsonElement? SaveRequest { get; set; }
    public string? CandidateId { get; set; }
    public string[] ObservationIds { get; set; } = [];
    public string? Failure { get; set; }
    public JsonElement? Warnings { get; set; }
}
