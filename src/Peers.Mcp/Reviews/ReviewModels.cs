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
}

internal sealed class ReviewRequest
{
    [JsonPropertyName("phase"), JsonRequired] public string Phase { get; set; } = "";
    [JsonPropertyName("round"), JsonRequired] public int Round { get; set; }
    [JsonPropertyName("task"), JsonRequired] public string Task { get; set; } = "";
    [JsonPropertyName("base_sha"), JsonRequired] public string BaseSha { get; set; } = "";
    [JsonPropertyName("reviewer_peer_id"), JsonRequired] public string ReviewerPeerId { get; set; } = "";
    [JsonPropertyName("change_summary"), JsonRequired] public string ChangeSummary { get; set; } = "";
    [JsonPropertyName("uncertainties"), JsonRequired] public string Uncertainties { get; set; } = "";
    [JsonPropertyName("expected_previous_packet_id")] public string? ExpectedPreviousPacketId { get; set; }
    [JsonPropertyName("source_root")] public string? SourceRoot { get; set; }
    [JsonPropertyName("plan_path")] public string? PlanPath { get; set; }
    [JsonPropertyName("verification_revision")] public string? VerificationRevision { get; set; }
    [JsonPropertyName("evidence")] public ReviewEvidence[] Evidence { get; set; } = [];
}
internal sealed class ReviewEvidence
{
    [JsonPropertyName("path"), JsonRequired] public string Path { get; set; } = "";
    [JsonPropertyName("kind"), JsonRequired] public string Kind { get; set; } = "";
    [JsonPropertyName("command"), JsonRequired] public string Command { get; set; } = "";
    [JsonPropertyName("cwd"), JsonRequired] public string Cwd { get; set; } = "";
    [JsonPropertyName("exit_code"), JsonRequired] public int ExitCode { get; set; }
}
internal sealed class ReviewReply
{
    [JsonPropertyName("verdict"), JsonRequired] public string Verdict { get; set; } = "";
    [JsonPropertyName("summary"), JsonRequired] public string Summary { get; set; } = "";
    [JsonPropertyName("findings"), JsonRequired] public ReviewFinding[] Findings { get; set; } = [];
}
internal sealed class ReviewFinding
{
    [JsonPropertyName("severity"), JsonRequired] public string Severity { get; set; } = "";
    [JsonPropertyName("location"), JsonRequired] public string Location { get; set; } = "";
    [JsonPropertyName("finding"), JsonRequired] public string Finding { get; set; } = "";
    [JsonPropertyName("fix"), JsonRequired] public string Fix { get; set; } = "";
    [JsonPropertyName("optional")] public bool Optional { get; set; }
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
