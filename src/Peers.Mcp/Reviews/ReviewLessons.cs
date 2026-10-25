using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using ModelContextProtocol;

namespace Peers.Mcp.Reviews;

internal sealed class ReviewLessonProposal
{
    [JsonPropertyName("cue")] public string Cue { get; set; } = "";
    [JsonPropertyName("lesson")] public string Lesson { get; set; } = "";
    [JsonPropertyName("effect")] public string Effect { get; set; } = "";
    [JsonPropertyName("confidence")] public string Confidence { get; set; } = "";
    [JsonPropertyName("confidence_reason")] public string ConfidenceReason { get; set; } = "";
    [JsonPropertyName("before")] public ReviewLessonReference[] Before { get; set; } = [];
    [JsonPropertyName("after")] public ReviewLessonReference[] After { get; set; } = [];
    [JsonPropertyName("scope")] public string Scope { get; set; } = "project";
}
internal sealed record ReviewLessonReference(
    [property: JsonPropertyName("packet_id")] string PacketId,
    [property: JsonPropertyName("path")] string Path,
    [property: JsonPropertyName("quote")] string Quote);
internal sealed class ReviewLessonResult
{
    [JsonPropertyName("save_result")] public JsonElement? SaveResult { get; set; }
    [JsonPropertyName("observation")] public JsonElement? Observation { get; set; }
    [JsonPropertyName("failure")] public ReviewLessonFailure? Failure { get; set; }
    [JsonPropertyName("observations")] public JsonElement[]? Observations { get; set; }
}
internal sealed record ReviewLessonFailure(
    [property: JsonPropertyName("stage")] string Stage,
    [property: JsonPropertyName("message")] string Message);
internal sealed record ReviewLessonView(string Status, string LessonId, string Marker, object? BindingRequest,
    string Body, string[] Tags, string Scope, string? AttemptId, string? ObservationId, string[] ObservationIds,
    JsonElement? BindingResult, string? IdentityConfidence, bool DispatchAllowed,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] JsonElement? SaveRequest,
    string? Failure, JsonElement? Warnings);

internal sealed partial class ReviewWorkflow
{
    private static string? CamelString(JsonElement value, string name) =>
        value.ValueKind == JsonValueKind.Object && value.TryGetProperty(name, out var property) && property.ValueKind == JsonValueKind.String ? property.GetString() : null;
    private static void NoApplicationError(JsonElement value)
    {
        ReviewJson.Require(value.ValueKind == JsonValueKind.Object &&
            (!value.TryGetProperty("error", out var error) || error.ValueKind == JsonValueKind.Null) &&
            (!value.TryGetProperty("hasError", out var hasError) || hasError.ValueKind != JsonValueKind.True), "Aletheia returned an application error or malformed result.");
    }
    private static void ValidateBinding(JsonElement binding)
    {
        NoApplicationError(binding);
        ReviewJson.Require(!string.IsNullOrWhiteSpace(CamelString(binding, "workspaceBindingId")) &&
            !string.IsNullOrWhiteSpace(CamelString(binding, "identityConfidence")) &&
            !string.IsNullOrWhiteSpace(CamelString(binding, "bindingStatus")) && CamelString(binding, "bindingStatus") != "quarantined",
            "Binding requires workspaceBindingId, identityConfidence and non-quarantined bindingStatus.");
    }
    internal static void FreezeBinding(ReviewLessonIntent intent, JsonElement binding)
    {
        ValidateBinding(binding);
        var id = CamelString(binding, "workspaceBindingId")!;
        ReviewJson.Require(intent.WorkspaceBindingId is null || intent.WorkspaceBindingId == id, "Frozen lesson workspace binding cannot change.");
        intent.WorkspaceBindingId = id;
        intent.Binding ??= binding.Clone(); // Includes low/observed identity confidence; never upgrade it.
    }
    internal static bool ExactObservation(ReviewLessonIntent intent, JsonElement observation)
    {
        NoApplicationError(observation);
        ReviewJson.Require(!string.IsNullOrWhiteSpace(CamelString(observation, "observationId")), "Observation verification requires an ID.");
        if (!observation.TryGetProperty("tags", out var tags) || tags.ValueKind != JsonValueKind.Array) return false;
        var values = tags.EnumerateArray().Select(t => t.ValueKind == JsonValueKind.String ? t.GetString()! : "\0").ToArray();
        return CamelString(observation, "workspaceBindingId") == intent.WorkspaceBindingId &&
            CamelString(observation, "profileScope") == intent.Scope && CamelString(observation, "body") == intent.Body &&
            values.ToHashSet(StringComparer.Ordinal).SetEquals(intent.Tags);
    }
    private static object LessonResult(ReviewLessonIntent intent, object? bindingRequest = null,
        bool dispatchAllowed = false, JsonElement? saveRequest = null) => new ReviewLessonView(
            intent.Status, intent.LessonId, intent.Marker, bindingRequest, intent.Body, intent.Tags, intent.Scope,
            intent.AttemptId, intent.ObservationIds.Length == 1 ? intent.ObservationIds[0] : null,
            intent.ObservationIds, intent.Binding,
            intent.Binding is { } binding ? CamelString(binding, "identityConfidence") : null,
            dispatchAllowed, saveRequest, intent.Failure, intent.Warnings);
    // Non-authoritative pointer at review_lesson after a completed lineage that needed CHANGES rounds.
    // It only reads pinned verdicts: no lesson intent, ledger field, or channel record is written.
    // It is built after the completion landed, so a broker fault here drops the hint instead of
    // reporting that finished completion as an error.
    private async Task<object?> LessonHintAsync(ReviewLedger ledger)
    {
        if (ledger.State != "completed") return null;
        var before = new List<object>();
        try
        {
            foreach (var packet in ledger.Packets)
            {
                if (packet.VerdictValue != "CHANGES" || packet.Verdict is not { } reference) continue;
                var verdict = await PinnedAsync(reference);
                before.Add(new { packet_id = packet.PacketId, phase = packet.Manifest.Phase, round = packet.Manifest.Round,
                    verdict_message_id = reference.MessageId, findings = verdict is null ? Array.Empty<string>() : FindingSummaries(verdict.Content) });
            }
        }
        catch (BrokerApiException) { return null; }
        return before.Count == 0 ? null : new
        {
            before, after = ledger.LatestPacketId,
            next_action = "Optional: run review_lesson prepare with a cue and lesson based on these findings; nothing is saved automatically.",
        };
    }
    // ReplyAsync renders one line per finding: "{n}. [OPTIONAL ]{SEVERITY} {location}: {finding} Fix: {fix}".
    private static string[] FindingSummaries(string verdict) => verdict.Split('\n')
        .Where(line => Regex.IsMatch(line, @"^\d+\. (OPTIONAL )?(HIGH|MED|LOW) "))
        .Select(line => line.Length <= 200 ? line : line[..(char.IsHighSurrogate(line[199]) ? 199 : 200)] + "…")
        .ToArray();
    internal async Task<object> LessonAsync(string action, ReviewLessonProposal? proposal, JsonElement? binding,
        string? attemptId, ReviewLessonResult? result, CancellationToken ct)
    {
        var (ledger, generation) = await ReadDriverAsync(ct);
        ReviewJson.Require(ledger is not null && ledger.DriverPeerId == PeerId, "Only the current or explicitly adopted driver may promote a lesson.");
        ReviewJson.Require(ledger.State == "completed" && ledger.Integrity == "intact" && ledger.Completion is not null &&
            Latest(ledger)?.Manifest.Phase == "CODE", "Lesson promotion requires terminal CODE completion and intact pinned PLAN/CODE/control evidence.");
        object? bindingRequest = null;
        ReviewLessonIntent? prepared = null;
        if (action == "prepare")
        {
            ReviewJson.Require(proposal is not null, "prepare requires proposal.");
            var p = proposal;
            var raw = ReviewJson.Text(p);
            // JSON escapes NUL, so inspect actual fields before structural no_entry exits.
            ReviewJson.Require((p.Before ?? []).Concat(p.After ?? []).All(r => r is not null), "Lesson references must be objects.");
            var strings = new[] { p.Cue, p.Lesson, p.Effect, p.Confidence, p.ConfidenceReason, p.Scope }
                .Concat((p.Before ?? []).Concat(p.After ?? []).SelectMany(r => new[] { r.PacketId, r.Path, r.Quote }));
            ReviewJson.Require(!strings.Any(s => s?.Contains('\0') == true), "NUL is not permitted in lesson quotes/body/tags.");
            ReviewJson.Require(p.Scope is "project" or "environment" or "user" && p.Effect is "helped" or "hurt" or "no_observed_effect" or "confounded" &&
                p.Confidence is "high" or "medium" or "low", "Invalid lesson scope/effect/confidence.");
            var proposalHash = ReviewJson.Hash(raw);
            if (ledger.Lesson is not null) ReviewJson.Require(ledger.Lesson.ProposalSha256 == proposalHash, "This completed packet already has a different frozen lesson proposal.");
            var before = p.Before ?? []; var after = p.After ?? [];
            if (p.Effect is "confounded" or "no_observed_effect" || p.Confidence == "low" ||
                string.IsNullOrWhiteSpace(p.Cue) || string.IsNullOrWhiteSpace(p.Lesson) || string.IsNullOrWhiteSpace(p.ConfidenceReason) ||
                before.Length == 0 || after.Length == 0 ||
                before.Select(ReviewJson.Text).ToHashSet(StringComparer.Ordinal).SetEquals(after.Select(ReviewJson.Text)))
                return new { status = "no_entry", reason = "A supported contrast, substantive cue/lesson/reason, and at least medium confidence are required." };
            var references = new List<string>();
            foreach (var reference in before.Concat(after))
            {
                ReviewJson.Require(!string.IsNullOrEmpty(reference.Quote), "Lesson evidence quotes must be literal nonempty UTF-8 text.");
                var packet = ledger.Packets.SingleOrDefault(x => x.PacketId == reference.PacketId);
                ReviewJson.Require(packet is not null, "Lesson reference must name a retained channel packet.");
                var materialized = await Artifacts.MaterializeAsync(packet.PacketId, PeerStore(ledger.ReviewerPeerId), packet.PacketSha256, ct);
                var file = materialized.Files.SingleOrDefault(f => f.Path == reference.Path);
                ReviewJson.Require(file is not null, "Lesson reference has no such logical payload path.");
                string text;
                try { text = new UTF8Encoding(false, true).GetString(await File.ReadAllBytesAsync(file.LocalPath, ct)); }
                catch (DecoderFallbackException ex) { throw new McpException("Lesson evidence must be strict UTF-8, never binary/base64.", ex); }
                ReviewJson.Require(!text.Contains('\0') && text.Contains(reference.Quote, StringComparison.Ordinal), "Lesson quote does not match retained literal text.");
                references.Add($"Packet: {packet.PacketId}\nPath: {reference.Path}\nParts: {string.Join(", ", file.Blob.Parts.Select(x => x.ArtifactId))}\nQuote:\n{reference.Quote}");
            }
            var id = ReviewJson.Hash(ReviewJson.Bytes(new { protocol_version = 1, packet_id = ledger.LatestPacketId, proposal = p, scope = p.Scope }));
            var current = Latest(ledger)!;
            var body = $"Cue: {p.Cue}\nLesson: {p.Lesson}\nEffect: {p.Effect}\nConfidence: {p.Confidence}\nConfidence reason: {p.ConfidenceReason}\n" +
                $"Source root: {ledger.SourceRoot}\nApproved digest: {current.PacketSha256}\nRequest: {current.Request!.MessageId}\nVerdict: {current.Verdict!.MessageId}\n" +
                "Before:\n" + string.Join("\n\n", references.Take(before.Length)) + "\nAfter:\n" + string.Join("\n\n", references.Skip(before.Length));
            ReviewJson.Require(!body.Contains('\0'), "NUL is not permitted in lesson body.");
            prepared = ledger.Lesson ?? new ReviewLessonIntent { LessonId = id, ProposalSha256 = proposalHash,
                Marker = "peers-lesson:" + id, Body = body, Tags = ["peers-lesson:" + id, "source:peers", "kind:workflow-lesson"], Scope = p.Scope };
            var remotes = (await ReviewSnapshot.GitTextAsync(ledger.SourceRoot, "remote")).Split('\n');
            var remote = remotes.Contains("origin", StringComparer.Ordinal) ? await ReviewSnapshot.GitTextAsync(ledger.SourceRoot, "remote", "get-url", "origin") : null;
            var first = (await ReviewSnapshot.GitTextAsync(ledger.SourceRoot, "rev-list", "--max-parents=0", "HEAD")).Split('\n')[0];
            bindingRequest = JsonSerializer.SerializeToElement(new { workingDir = ledger.SourceRoot, gitRemoteUrl = remote, gitFirstCommitSha = first });
        }
        var store = DriverStore;
        using (store.Acquire())
        {
            store.Load<ReviewLedger>();
            ReviewJson.Require(store.Generation == generation, "Review lesson state changed during verification; retry.");
            await RecheckAsync(ledger, artifacts: false, ct);
            ReviewJson.Require(ledger.Integrity == "intact", "Lesson completion evidence changed before mutation.");
            if (prepared is not null) ledger.Lesson = prepared;
            var intent = ledger.Lesson ?? throw new McpException("Prepare a supported lesson before saving/reconciling.");
            if (action == "prepare") { await PersistAsync(store, ledger); return LessonResult(intent, bindingRequest); }
            if (action == "begin_save")
            {
                FreezeBinding(intent, binding!.Value);
                if (intent.AttemptId is not null) { await PersistAsync(store, ledger); return LessonResult(intent); }
                ReviewJson.Require(intent.Status == "prepared", "Only a never-granted prepared intent may begin a save.");
                intent.AttemptId = Guid.NewGuid().ToString("N"); intent.Status = "saving";
                intent.SaveRequest = JsonSerializer.SerializeToElement(new { workspaceBindingId = intent.WorkspaceBindingId,
                    body = intent.Body, profileScope = intent.Scope, tags = intent.Tags });
                await PersistAsync(store, ledger); // Durable local AND mirrored intent before granting one dispatch.
                return LessonResult(intent, dispatchAllowed: true, saveRequest: intent.SaveRequest);
            }
            if (action == "record")
            {
                ReviewJson.Require(intent.AttemptId is not null && attemptId == intent.AttemptId, "record requires the granted attempt_id.");
                if (result!.Failure is { } failure)
                {
                    ReviewJson.Require(failure.Stage is "save" or "verify" && !string.IsNullOrWhiteSpace(failure.Message), "Invalid lesson failure stage/message.");
                    intent.Status = "unknown"; intent.Failure = failure.Stage + ": " + failure.Message;
                }
                else
                {
                    try
                    {
                        var save = result.SaveResult!.Value; var observation = result.Observation!.Value;
                        NoApplicationError(save);
                        var id = CamelString(save, "observationId");
                        ReviewJson.Require(!string.IsNullOrWhiteSpace(id) && CamelString(save, "profileScope") == intent.Scope &&
                            ExactObservation(intent, observation) && CamelString(observation, "observationId") == id &&
                            (intent.CandidateId is null || intent.CandidateId == id), "Saved observation verification does not match frozen lesson/binding/scope.");
                        intent.CandidateId = id; intent.Status = "saving";
                        if (save.TryGetProperty("relations", out var relations)) intent.Warnings = relations.Clone();
                    }
                    catch (McpException ex) { intent.Status = "unknown"; intent.Failure = ex.Message; }
                }
                await PersistAsync(store, ledger); return LessonResult(intent);
            }
            FreezeBinding(intent, binding!.Value);
            ReviewJson.Require(intent.AttemptId is null ? attemptId is null : attemptId == intent.AttemptId, "reconcile requires the matching granted attempt_id.");
            var observations = result!.Observations!;
            ReviewJson.Require(observations.Length < 200, "A full marker search page cannot prove uniqueness/completeness.");
            var ids = new HashSet<string>(StringComparer.Ordinal);
            foreach (var observation in observations)
            {
                ReviewJson.Require(ExactObservation(intent, observation), "Lesson marker collision or observation mismatch; no save is authorized.");
                ReviewJson.Require(ids.Add(CamelString(observation, "observationId")!), "Duplicate observation IDs do not prove distinct matches.");
            }
            intent.ObservationIds = ids.Order(StringComparer.Ordinal).ToArray();
            if (ids.Count > 1) intent.Status = "duplicate";
            else if (ids.Count == 1)
            {
                if (intent.CandidateId is not null && !ids.Contains(intent.CandidateId))
                { intent.Status = "unknown"; intent.Failure = "Marker match differs from recorded save candidate."; }
                else { intent.Status = "saved"; intent.Failure = null; }
            }
            else intent.Status = intent.AttemptId is null ? "prepared" : "unknown";
            await PersistAsync(store, ledger); return LessonResult(intent);
        }
    }
}
