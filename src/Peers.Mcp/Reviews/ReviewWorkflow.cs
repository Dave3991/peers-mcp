using System.Text;
using System.Text.Json;
using ModelContextProtocol;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Reviews;

internal sealed partial class ReviewWorkflow(BrokerClient client, SessionState session, ReviewStorageOptions options, string channelId)
{
    internal string PeerId => session.PeerId ?? throw new McpException("Peer is not registered.");
    internal ReviewArtifacts Artifacts => new(client, channelId, PeerId);
    internal ReviewStore DriverStore => new(options, channelId, "driver");
    internal ReviewStore PeerStore(string? reviewer = null) => new(options, channelId,
        (reviewer == PeerId ? "reviewer-" : "observer-") + ReviewStore.Hash(PeerId));
    private static ReviewMessageRef Reference(MessageDto message) =>
        new(message.MessageId, message.Seq, ReviewJson.Hash(message.Content), message.AuthorPeerId);
    private static string Record(object value, string prose) => "PEERS_REVIEW_V1\n" + ReviewJson.Text(value) + "\n" + prose;
    private static JsonElement? ParseRecord(MessageDto message)
    {
        if (message.Quiet || !message.Content.StartsWith("PEERS_REVIEW_V1\n", StringComparison.Ordinal)) return null;
        var end = message.Content.IndexOf('\n', 16);
        if (end < 0) return null;
        try { return JsonDocument.Parse(message.Content[16..end]).RootElement.Clone(); }
        catch (JsonException) { return null; }
    }
    private static string? Field(JsonElement record, string name) => record.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;
    private static int Number(JsonElement record, string name) => record.TryGetProperty(name, out var value) && value.TryGetInt32(out var n) ? n : 0;

    internal async Task<List<MessageDto>> MessagesAsync(long since = 0)
    {
        var messages = new List<MessageDto>();
        while (true)
        {
            var page = await client.ReadChannelMessagesAsync(channelId, PeerId, since, 200)
                ?? throw new McpException("Review channel is missing.");
            ReviewJson.Require(page.ChannelId == channelId, "Review channel identity mismatch.");
            foreach (var message in page.Messages.OrderBy(m => m.Seq))
            {
                ReviewJson.Require(message.ChannelId == channelId && message.Seq > since, "Invalid review channel page ordering.");
                messages.Add(message); since = message.Seq;
            }
            if (!page.Truncated) return messages;
            ReviewJson.Require(page.Messages.Length > 0, "Truncated review history did not advance.");
        }
    }
    internal async Task<MessageDto?> PinnedAsync(ReviewMessageRef reference)
    {
        var page = await client.ReadChannelMessagesAsync(channelId, PeerId, reference.Seq - 1, 1);
        var message = page?.Messages.FirstOrDefault();
        return message is not null && message.MessageId == reference.MessageId && message.Seq == reference.Seq &&
            message.AuthorPeerId == reference.AuthorPeerId && !message.Quiet &&
            ReviewJson.Hash(message.Content) == reference.Sha256 ? message : null;
    }
    internal async Task<ReviewLedger?> MirrorAsync()
    {
        Peers.Shared.Models.Artifact? artifact;
        try { artifact = await client.GetArtifactByNameAsync(Artifacts.Namespace, "checkpoint"); }
        catch (BrokerApiException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound) { return null; }
        if (artifact is null) return null;
        ReviewArtifacts.VerifyArtifact(artifact);
        ReviewJson.Require(artifact.Namespace == Artifacts.Namespace && artifact.Name == "checkpoint", "Review checkpoint identity mismatch.");
        var mirror = ReviewJson.Parse<ReviewMirror>(Encoding.UTF8.GetBytes(artifact.Content));
        var payload = ReviewJson.Parse<ReviewLedger>(await Artifacts.ReadBlobAsync(mirror.Payload));
        ReviewJson.Require(mirror.ProtocolVersion == 1 && mirror.Generation > 0 && payload.ChannelId == channelId &&
            mirror.PayloadSha256 == mirror.Payload.Sha256 && mirror.PayloadSha256 == ReviewJson.Hash(ReviewJson.Bytes(payload)) &&
            artifact.RepoRoot == payload.SourceRoot, "Review checkpoint digest/lineage mismatch.");
        payload.MirrorId = artifact.ArtifactId;
        return payload;
    }
    internal async Task<ReviewLedger?> LoadAsync(ReviewStore store)
    {
        ReviewLedger? local;
        try { local = store.Load<ReviewLedger>(); }
        catch (McpException)
        {
            var recovered = await MirrorAsync();
            if (recovered is null) throw;
            var artifact = await Artifacts.GetAsync(recovered.MirrorId!);
            var mirror = ReviewJson.Parse<ReviewMirror>(Encoding.UTF8.GetBytes(artifact.Content));
            store.Save(recovered, recovered.MirrorId, mirror.Generation);
            return recovered;
        }
        var latest = await MirrorAsync();
        if (latest is null) return local;
        var source = await Artifacts.GetAsync(latest.MirrorId!);
        var remote = ReviewJson.Parse<ReviewMirror>(Encoding.UTF8.GetBytes(source.Content));
        if (local is null || remote.Generation > store.Generation)
        {
            store.Save(latest, latest.MirrorId, remote.Generation);
            return latest;
        }
        ReviewJson.Require(remote.Generation != store.Generation || remote.PayloadSha256 == store.PayloadDigest,
            "Equal review generation has conflicting payload digests.");
        return local;
    }
    internal async Task PersistAsync(ReviewStore store, ReviewLedger ledger)
    {
        store.Save(ledger);
        var payload = await Artifacts.PublishBlobAsync(ReviewJson.Bytes(ledger), ledger.SourceRoot);
        var mirror = new ReviewMirror(1, store.Generation, store.PayloadDigest!, payload);
        var artifact = await Artifacts.PublishAsync("checkpoint", "config", ReviewJson.Text(mirror), ledger.SourceRoot);
        ledger.MirrorId = artifact.ArtifactId;
        store.Save(ledger, artifact.ArtifactId, store.Generation);
    }
    private static ReviewPacketState? Latest(ReviewLedger ledger) => ledger.Packets.LastOrDefault(p => p.PacketId == ledger.LatestPacketId);
    private static void IntegrityFailure(ReviewLedger ledger, string value)
    {
        if (ledger.Integrity != "withdrawn") ledger.Integrity = value;
        if (ledger.State != "completed") ledger.State = ledger.Integrity;
    }
    internal async Task RecheckAsync(ReviewLedger ledger, bool artifacts = true, CancellationToken ct = default)
    {
        bool Matches(MessageDto message, ReviewPacketState packet, string kind)
        {
            var parsed = ParseRecord(message);
            return parsed is { } record && Field(record, "kind") == kind &&
                Field(record, "packet_id") == packet.PacketId && Field(record, "packet_sha256") == packet.PacketSha256 &&
                Field(record, "phase") == packet.Manifest.Phase && Number(record, "round") == packet.Manifest.Round;
        }
        foreach (var packet in ledger.Packets)
        {
            var request = packet.Request is null ? null : await PinnedAsync(packet.Request);
            if (request is null) IntegrityFailure(ledger, "withdrawn");
            else if (!Matches(request, packet, "request") || request.AuthorPeerId != packet.Manifest.DriverPeerId ||
                !request.Mentions.SequenceEqual(new[] { packet.Manifest.ReviewerPeerId })) IntegrityFailure(ledger, "unverifiable");
            if (ReviewJson.Hash(ReviewJson.Bytes(packet.Manifest)) != packet.PacketSha256 ||
                packet.Manifest.ChannelId != ledger.ChannelId || packet.Manifest.SourceRoot != ledger.SourceRoot ||
                packet.Manifest.GitDir != ledger.GitDir || packet.Manifest.BaseSha != ledger.BaseSha ||
                packet.Manifest.TaskSha256 != ledger.TaskSha256) IntegrityFailure(ledger, "unverifiable");
            if (packet.Verdict is not null)
            {
                var verdict = await PinnedAsync(packet.Verdict);
                if (verdict is null) IntegrityFailure(ledger, "withdrawn");
                else if (!Matches(verdict, packet, "verdict") || verdict.AuthorPeerId != packet.Manifest.ReviewerPeerId ||
                    verdict.AuthorPeerId == packet.Manifest.DriverPeerId || verdict.Seq <= packet.Request?.Seq ||
                    !verdict.Content.EndsWith("\nVERDICT: " + packet.VerdictValue, StringComparison.Ordinal) ||
                    packet.VerdictValue is not ("APPROVE" or "CHANGES") || verdict.Mentions.Length != 1 ||
                    !(verdict.Mentions[0] == packet.Manifest.DriverPeerId || verdict.Mentions[0] == ledger.DriverPeerId ||
                      ledger.DriverLineage.Contains(verdict.Mentions[0], StringComparer.Ordinal))) IntegrityFailure(ledger, "unverifiable");
            }
            else if (packet.VerdictValue is not null) IntegrityFailure(ledger, "unverifiable");
            if (artifacts)
            {
                try { await Artifacts.MaterializeAsync(packet.PacketId, PeerStore(ledger.ReviewerPeerId), packet.PacketSha256, ct); }
                catch (McpException) { IntegrityFailure(ledger, "unverifiable"); }
            }
        }
        foreach (var control in ledger.Controls)
        {
            var message = await PinnedAsync(control);
            if (message is null) { IntegrityFailure(ledger, "withdrawn"); continue; }
            var parsed = ParseRecord(message);
            if (parsed is not { } record || Field(record, "kind") != "control" ||
                Field(record, "driver_peer_id") != message.AuthorPeerId) IntegrityFailure(ledger, "unverifiable");
        }
        if (ledger.State == "completed")
        {
            var completion = ledger.Completion is null ? null : await PinnedAsync(ledger.Completion);
            if (completion is null) IntegrityFailure(ledger, "withdrawn");
            else if (ParseRecord(completion) is not { } record || Field(record, "operation") != "complete" ||
                Field(record, "packet_id") != ledger.LatestPacketId ||
                !ledger.Controls.Any(c => c.MessageId == completion.MessageId)) IntegrityFailure(ledger, "unverifiable");
        }
    }
    internal async Task ReduceAsync(ReviewLedger ledger, CancellationToken ct = default, bool verifyArtifacts = true)
    {
        foreach (var message in await MessagesAsync(ledger.AppliedThroughSeq))
        {
            var parsed = ParseRecord(message);
            if (parsed is { } record)
            {
                var kind = Field(record, "kind");
                if (kind == "request" && ledger.State != "completed" && message.AuthorPeerId == ledger.DriverPeerId)
                {
                    var id = Field(record, "packet_id");
                    var digest = Field(record, "packet_sha256");
                    if (id is not null && digest is not null && !ledger.Packets.Any(p => p.PacketId == id))
                    {
                        var root = await Artifacts.GetAsync(id);
                        var manifest = ReviewJson.Parse<ReviewManifest>(Encoding.UTF8.GetBytes(root.Content));
                        var phase = Field(record, "phase"); var round = Number(record, "round");
                        var expected = phase == "PLAN" ? ledger.PlanRound + 1 : ledger.CodeRound + 1;
                        if (root.ContentHash == digest && manifest.ChannelId == channelId && manifest.DriverPeerId == ledger.DriverPeerId &&
                            manifest.TaskSha256 == ledger.TaskSha256 && manifest.SourceRoot == ledger.SourceRoot && manifest.GitDir == ledger.GitDir &&
                            manifest.BaseSha == ledger.BaseSha && manifest.Phase == phase && manifest.Round == round && round == expected &&
                            message.Mentions.SequenceEqual(new[] { manifest.ReviewerPeerId }) &&
                            ((phase == "PLAN") || (phase == "CODE" && ledger.ApprovedPlanPacketId is not null &&
                              manifest.ApprovedPlanPacketId == ledger.ApprovedPlanPacketId && manifest.ReviewerPeerId == ledger.ReviewerPeerId)))
                        {
                            var packet = new ReviewPacketState { PacketId = id, PacketSha256 = digest, Manifest = manifest,
                                Request = Reference(message), InputSha256 = ledger.Publication?.PacketId == id ? ledger.Publication.InputSha256 : "" };
                            ledger.Packets.Add(packet); ledger.LatestPacketId = id; ledger.ReviewerPeerId = manifest.ReviewerPeerId;
                            if (phase == "PLAN") { ledger.PlanRound = round; ledger.ApprovedPlanPacketId = null; }
                            else ledger.CodeRound = round;
                            ledger.State = "awaiting_review"; ledger.Stale = false;
                            if (ledger.Publication?.PacketId == id) ledger.Publication = null;
                        }
                    }
                }
                else if (kind == "verdict" && ledger.State != "completed")
                {
                    var packet = Latest(ledger);
                    if (packet is not null && Field(record, "packet_id") == packet.PacketId &&
                        Field(record, "packet_sha256") == packet.PacketSha256 && Field(record, "phase") == packet.Manifest.Phase &&
                        Number(record, "round") == packet.Manifest.Round && message.AuthorPeerId == packet.Manifest.ReviewerPeerId &&
                        message.AuthorPeerId != ledger.DriverPeerId && message.Mentions.Length == 1 &&
                        (message.Mentions[0] == packet.Manifest.DriverPeerId || ledger.DriverLineage.Contains(message.Mentions[0], StringComparer.Ordinal) || message.Mentions[0] == ledger.DriverPeerId) &&
                        message.Seq > packet.Request!.Seq)
                    {
                        var verdict = message.Content.EndsWith("\nVERDICT: APPROVE", StringComparison.Ordinal) ? "APPROVE" :
                            message.Content.EndsWith("\nVERDICT: CHANGES", StringComparison.Ordinal) ? "CHANGES" : null;
                        if (verdict is not null && packet.Verdict is null)
                        {
                            packet.Verdict = Reference(message); packet.VerdictValue = verdict;
                            var nextState = ledger.Stale ? "stale" : verdict == "APPROVE" ? "approved" : "changes_requested";
                            if (ledger.State == "paused") ledger.PrePauseState = nextState; else ledger.State = nextState;
                            if (packet.Manifest.Phase == "PLAN" && verdict == "APPROVE") ledger.ApprovedPlanPacketId = packet.PacketId;
                        }
                    }
                }
                else if (kind == "control") ApplyControl(ledger, message, record);
            }
            ledger.AppliedThroughSeq = message.Seq;
        }
        await RecheckAsync(ledger, artifacts: verifyArtifacts, ct);
    }
    private static void ApplyControl(ReviewLedger ledger, MessageDto message, JsonElement record)
    {
        var operation = Field(record, "operation");
        var driver = Field(record, "driver_peer_id"); var previous = Field(record, "previous_driver_peer_id");
        if (Field(record, "packet_id") != ledger.LatestPacketId || driver is null || message.AuthorPeerId != driver ||
            driver == ledger.ReviewerPeerId || previous != ledger.DriverPeerId || message.Mentions.Length != 0) return;
        if (operation == "resume")
        {
            ledger.DriverPeerId = driver;
            if (!ledger.DriverLineage.Contains(driver, StringComparer.Ordinal)) ledger.DriverLineage.Add(driver);
            if (ledger.State == "paused") ledger.State = ledger.PrePauseState ?? "awaiting_review";
        }
        else if (driver != ledger.DriverPeerId || ledger.State == "completed") return;
        else if (operation == "pause")
        {
            if (ledger.State != "paused") ledger.PrePauseState = ledger.State;
            ledger.State = "paused";
        }
        else if (operation == "complete")
        {
            var packet = Latest(ledger);
            if (packet?.Manifest.Phase != "CODE" || packet.VerdictValue != "APPROVE" || ledger.Stale || ledger.Integrity != "intact" ||
                ledger.ApprovedPlanPacketId != packet.Manifest.ApprovedPlanPacketId) return;
            ledger.State = "completed"; ledger.Completion = Reference(message); ledger.CompletedRevision = packet.Manifest.RevisionSha256;
        }
        else return;
        ledger.Controls.Add(Reference(message));
        if (ledger.ControlIntent?.Content != message.Content) ledger.ControlCounter++;
        if (ledger.ControlIntent?.Content == message.Content) ledger.ControlIntent = null;
    }
    internal async Task<ReviewLedger?> ProjectionAsync(CancellationToken ct = default, bool verifyArtifacts = true)
    {
        var ledger = await MirrorAsync();
        if (ledger is not null) await ReduceAsync(ledger, ct, verifyArtifacts);
        return ledger;
    }
    private async Task<MessageDto> ConfirmSendAsync(SendMessageResponse response, string content, string author, string? target)
    {
        string[] mentions = target is null ? [] : [target];
        ReviewJson.Require(response.ChannelId == channelId && response.Mentions.SequenceEqual(mentions), "Review send was rerouted or has unexpected mentions.");
        var reference = new ReviewMessageRef(response.MessageId, response.Seq, ReviewJson.Hash(content), author);
        var stored = await PinnedAsync(reference);
        ReviewJson.Require(stored is not null && stored.Content == content && stored.Mentions.SequenceEqual(mentions),
            "Stored review send differs, was deleted, or cannot be verified; never recreate this round.");
        return stored!;
    }
    private static bool Delivered(SendMessageResponse response, string target) => response.Delivery is { } delivery &&
        delivery.OnlinePeerIds.SequenceEqual(new[] { target }) && delivery.OfflinePeerIds.Length == 0 && delivery.UnresolvedTargets.Length == 0;
    private static object PacketResult(ReviewLedger ledger, ReviewPacketState packet, SendMessageResponse? response = null) => new
    {
        packet_id = packet.PacketId, packet_sha256 = packet.PacketSha256, phase = packet.Manifest.Phase,
        round = packet.Manifest.Round, state = ledger.State, message_id = response?.MessageId,
        seq = response?.Seq, deduplicated = response?.Deduplicated, delivery = response?.Delivery,
    };
    private static FileStream ExplicitFile(string path)
    {
        ReviewJson.Require(Path.IsPathFullyQualified(path), "Plan/evidence paths must be explicit absolute files.");
        var info = new FileInfo(path);
        ReviewJson.Require(info.Exists && info.LinkTarget is null && (info.Attributes & (FileAttributes.Directory | FileAttributes.ReparsePoint)) == 0,
            "Plan/evidence inputs must be regular files, never directories or symlinks.");
        for (var parent = info.Directory; parent is not null; parent = parent.Parent)
            ReviewJson.Require(parent.LinkTarget is null, "Plan/evidence input paths must not follow symlink targets.");
        return new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
    }
    internal async Task<object> PublishAsync(ReviewRequest request, CancellationToken ct)
    {
        ReviewJson.Require(request.Phase is "PLAN" or "CODE" && request.Round > 0 && !string.IsNullOrWhiteSpace(request.Task) &&
            !string.IsNullOrWhiteSpace(request.ReviewerPeerId), "Publish requires phase PLAN|CODE, positive round, task and exact reviewer_peer_id.");
        ReviewJson.Require(request.Evidence is not null && request.ChangeSummary is not null && request.Uncertainties is not null, "Missing request fields.");
        ReviewJson.Require(request.Evidence.All(e => e is not null), "Evidence entries must be objects.");
        ReviewJson.Require(request.Phase == "PLAN" ? request.PlanPath is not null && request.VerificationRevision is null :
            request.PlanPath is null && request.VerificationRevision is not null && request.Evidence.Any(e => e.Kind is "test" or "smoke"),
            "PLAN requires plan_path; CODE requires verification_revision and test/smoke evidence.");
        foreach (var evidence in request.Evidence)
            ReviewJson.Require(evidence.Kind is "build" or "test" or "smoke" or "other" && !string.IsNullOrWhiteSpace(evidence.Command) &&
                Path.IsPathFullyQualified(evidence.Cwd), "Invalid evidence kind/command/cwd.");
        var roster = await client.ListPeersAsync();
        ReviewJson.Require(request.ReviewerPeerId != PeerId && roster.Any(p => p.PeerId == request.ReviewerPeerId), "Reviewer must be a different exact live peer ID.");
        var root = request.SourceRoot ?? session.RepoRoot ?? Directory.GetCurrentDirectory();
        var source = await ReviewSnapshot.CaptureAsync(root, request.BaseSha, ct);
        var inputHash = ReviewJson.Hash(ReviewJson.Bytes(new { request, revision_sha256 = source.RevisionSha256,
            inputs = await InputHashesAsync(request, ct) }));
        var verifiedProjection = await ProjectionAsync(ct);
        var store = DriverStore;
        ReviewLedger ledger;
        using (store.Acquire())
        {
            ledger = await LoadAsync(store) ?? new ReviewLedger
            {
                ChannelId = channelId, TaskSha256 = ReviewJson.Hash(request.Task), SourceRoot = source.SourceRoot,
                GitDir = source.GitDir, BaseSha = source.BaseSha, DriverPeerId = PeerId, ReviewerPeerId = request.ReviewerPeerId,
            };
            await ReduceAsync(ledger, ct, verifyArtifacts: false);
            ReviewJson.Require(verifiedProjection is null || verifiedProjection.Integrity == "intact", "Pinned review evidence cannot authorize a new round.");
            if (ledger.DriverPeerId == PeerId && ledger.State != "completed" && Latest(ledger)?.Manifest.Phase == "CODE" &&
                source.RevisionSha256 != Latest(ledger)!.Manifest.RevisionSha256)
            {
                ledger.Stale = true; ledger.State = "stale";
                await PersistAsync(store, ledger);
            }
            ReviewJson.Require(ledger.DriverPeerId == PeerId, "Only the current driver may publish; use explicit resume to adopt offline work.");
            ReviewJson.Require(ledger.State != "completed" && ledger.Integrity == "intact", "Completed/withdrawn/unverifiable channels cannot publish new requests.");
            ReviewJson.Require(ledger.TaskSha256 == ReviewJson.Hash(request.Task) && ledger.SourceRoot == source.SourceRoot &&
                ledger.GitDir == source.GitDir && ledger.BaseSha == source.BaseSha, "Task/base/source lineage changed; create a new channel.");
            var existing = ledger.Packets.SingleOrDefault(p => p.Manifest.Phase == request.Phase && p.Manifest.Round == request.Round);
            if (existing is not null)
            {
                ReviewJson.Require(existing.PacketId == ledger.LatestPacketId && existing.InputSha256 == inputHash, "Changed same-round publication conflicts; use a new round.");
                var message = await PinnedAsync(existing.Request!);
                ReviewJson.Require(message is not null, "Deleted review requests cannot be recreated; create a new channel.");
                ledger.Publication = null;
                if (existing.Delivery is null) { ledger.PrePauseState = ledger.State; ledger.State = "paused"; }
                await PersistAsync(store, ledger);
                return PacketResult(ledger, existing, new SendMessageResponse { ChannelId = channelId, MessageId = message!.MessageId,
                    Seq = message.Seq, Mentions = message.Mentions, Deduplicated = true, Delivery = existing.Delivery });
            }
            ReviewJson.Require(request.ExpectedPreviousPacketId == ledger.LatestPacketId, "expected_previous_packet_id must match the latest packet of either phase.");
            ReviewJson.Require(request.Round == (request.Phase == "PLAN" ? ledger.PlanRound : ledger.CodeRound) + 1 &&
                (ledger.Packets.Count != 0 || request.Phase == "PLAN" && request.Round == 1), "Round counters are monotonic per phase; first request must be PLAN/1.");
            if (request.Phase == "CODE")
            {
                ReviewJson.Require(ledger.ApprovedPlanPacketId is not null && request.ReviewerPeerId == ledger.ReviewerPeerId,
                    "CODE requires the current reviewer's approved PLAN.");
                ReviewJson.Require(request.VerificationRevision == source.RevisionSha256 && ledger.VerificationRevision == request.VerificationRevision,
                    "Verification revision must be issued by review_checkpoint before checks and still match Git-visible source.");
            }
            if (ledger.Publication is { Dispatched: true } frozen)
            {
                IntegrityFailure(ledger, "unverifiable"); await PersistAsync(store, ledger);
                throw new McpException("Request dispatch is ambiguous or deleted; create a new channel, never recreate the round.");
            }
            ledger.Publication = new ReviewPublication { InputSha256 = inputHash };
            await PersistAsync(store, ledger);
        }
        var intent = ledger.Publication!;
        async Task ConfirmPart(ReviewPart part)
        {
            using (store.Acquire())
            {
                var current = store.Load<ReviewLedger>()!;
                ReviewJson.Require(current.Publication?.InputSha256 == inputHash && !current.Publication.Dispatched,
                    "Review capture intent was replaced.");
                current.Publication.ConfirmedParts.Add(part); store.Save(current);
            }
            await Task.CompletedTask;
        }
        var files = new List<ReviewPayloadFile>();
        async Task Add(string logical, byte[] bytes, string kind, string? original = null, bool executable = false, ReviewEvidence? evidence = null)
        {
            var blob = await Artifacts.PublishBlobAsync(bytes, source.SourceRoot, ConfirmPart, ct);
            files.Add(new(logical, original, kind, executable, evidence, blob));
        }
        async Task AddFile(string logical, string path, string kind, ReviewEvidence? evidence = null)
        {
            await using var input = ExplicitFile(path);
            var blob = await Artifacts.PublishBlobAsync(input, source.SourceRoot, ConfirmPart, ct);
            files.Add(new(logical, path, kind, false, evidence, blob));
        }
        await Add("task.txt", Encoding.UTF8.GetBytes(request.Task), "task");
        if (request.PlanPath is not null) await AddFile("plan.md", request.PlanPath, "plan");
        await Add("tracked.diff", source.TrackedDiff, "diff");
        foreach (var file in source.Untracked) await Add("untracked/" + file.Path, file.Bytes, file.Kind, file.Path, file.Executable);
        for (var i = 0; i < request.Evidence.Length; i++)
        {
            var evidence = request.Evidence[i];
            await AddFile($"evidence/{i:D4}-{evidence.Kind}.log", evidence.Path, "evidence", evidence);
        }
        var payload = new ReviewPayloadIndex(1, request.ChangeSummary, request.Uncertainties, files.ToArray());
        var index = await Artifacts.PublishBlobAsync(ReviewJson.Bytes(payload), source.SourceRoot, ConfirmPart, ct);
        var manifest = new ReviewManifest(1, channelId, PeerId, request.ReviewerPeerId, request.Phase, request.Round,
            source.SourceRoot, source.GitDir, source.BaseSha, source.HeadSha, source.RevisionSha256, ReviewJson.Hash(request.Task),
            request.Phase == "CODE" ? ledger.ApprovedPlanPacketId : null, index);
        var manifestBytes = ReviewJson.Bytes(manifest); var digest = ReviewJson.Hash(manifestBytes);
        var packet = await Artifacts.PublishAsync("packet-" + digest, "config", Encoding.UTF8.GetString(manifestBytes), source.SourceRoot);
        var after = await ReviewSnapshot.CaptureAsync(source.SourceRoot, source.BaseSha, ct);
        ReviewJson.Require(after.RevisionSha256 == source.RevisionSha256, "Source changed during capture; no review request was sent.");
        ReviewJson.Require(inputHash == ReviewJson.Hash(ReviewJson.Bytes(new { request, revision_sha256 = after.RevisionSha256,
            inputs = await InputHashesAsync(request, ct) })), "Plan/evidence inputs changed during capture; no review request was sent.");
        var prose = $"Immutable packet {packet.ArtifactId}; task/plan/full diff/untracked/raw evidence are pinned by its payload index.\n";
        if (Encoding.UTF8.GetByteCount(prose + request.Task) < 95_000) prose += request.Task;
        else prose += "Full task text is verbatim in the pinned task.txt blob.";
        var content = Record(new { kind = "request", phase = request.Phase, round = request.Round, packet_id = packet.ArtifactId, packet_sha256 = digest }, prose);
        using (store.Acquire())
        {
            ledger = store.Load<ReviewLedger>()!;
            ReviewJson.Require(ledger.Publication?.InputSha256 == inputHash && !ledger.Publication.Dispatched, "Review capture intent was replaced.");
            intent = ledger.Publication; intent.PacketId = packet.ArtifactId; intent.PacketSha256 = digest;
            intent.Manifest = manifest; intent.Content = content; intent.Dispatched = true;
            await PersistAsync(store, ledger);
            // The durable dispatched intent survives lost responses. History, not a transport error,
            // decides whether this exact request landed or remains ambiguous.
            var response = await client.SendMessageAsync(channelId, new SendMessageRequest
            {
                AuthorPeerId = PeerId, To = [request.ReviewerPeerId], Quiet = false, Content = content,
                IdempotencyKey = $"peers-review-v1:{request.Phase}:{request.Round}",
            }) ?? throw new McpException("Review send returned no result.");
            await ConfirmSendAsync(response, content, PeerId, request.ReviewerPeerId);
            await ReduceAsync(ledger, ct, verifyArtifacts: false);
            var published = Latest(ledger);
            ReviewJson.Require(published?.PacketId == packet.ArtifactId, "Review request was not accepted by channel reduction.");
            published!.InputSha256 = inputHash;
            published.Delivery = response.Delivery;
            ledger.Publication = null;
            if (!Delivered(response, request.ReviewerPeerId)) { ledger.PrePauseState = ledger.State; ledger.State = "paused"; }
            await PersistAsync(store, ledger);
            return PacketResult(ledger, published, response);
        }
    }
    private static async Task<string[]> InputHashesAsync(ReviewRequest request, CancellationToken ct)
    {
        var hashes = new List<string>();
        foreach (var path in (request.PlanPath is null ? Array.Empty<string>() : new[] { request.PlanPath }).Concat(request.Evidence.Select(e => e.Path)))
        {
            await using var input = ExplicitFile(path);
            hashes.Add(Convert.ToHexStringLower(await System.Security.Cryptography.SHA256.HashDataAsync(input, ct)));
        }
        return hashes.ToArray();
    }
    internal async Task<object> FetchAsync(string packetId, CancellationToken ct)
    {
        var ledger = await ProjectionAsync(ct) ?? throw new McpException("No recoverable review checkpoint.");
        var packet = ledger.Packets.SingleOrDefault(p => p.PacketId == packetId) ?? throw new McpException("Packet has no authoritative channel request.");
        ReviewJson.Require(ledger.Integrity == "intact" && packet.Request is not null && await PinnedAsync(packet.Request) is not null,
            "Packet request/evidence was deleted or is unverifiable.");
        var materialized = await Artifacts.MaterializeAsync(packetId, PeerStore(packet.Manifest.ReviewerPeerId), packet.PacketSha256, ct);
        var task = materialized.Files.Single(f => f.Path == "task.txt");
        return new { packet_id = packetId, packet_sha256 = packet.PacketSha256, phase = packet.Manifest.Phase,
            round = packet.Manifest.Round, state = ledger.State, revision_sha256 = packet.Manifest.RevisionSha256,
            task_path = task.LocalPath, manifest_path = materialized.ManifestPath, materialization_map_path = materialized.MapPath,
            evidence_dir = Path.GetDirectoryName(materialized.ManifestPath), file_count = materialized.Files.Length,
            byte_count = materialized.Files.Sum(f => f.Blob.ByteLength), approved_plan_packet_id = packet.Manifest.ApprovedPlanPacketId,
            files = materialized.Files };
    }
    internal async Task<object> ReplyAsync(string packetId, ReviewReply reply, CancellationToken ct)
    {
        ReviewJson.Require(reply.Verdict is "APPROVE" or "CHANGES" && !string.IsNullOrWhiteSpace(reply.Summary) && reply.Findings is not null,
            "Reply requires APPROVE|CHANGES, summary and findings.");
        ReviewJson.Require(reply.Findings.All(f => f is not null), "Findings must be objects.");
        foreach (var finding in reply.Findings)
            ReviewJson.Require(finding.Severity is "HIGH" or "MED" or "LOW" && !string.IsNullOrWhiteSpace(finding.Location) &&
                !string.IsNullOrWhiteSpace(finding.Finding) && !string.IsNullOrWhiteSpace(finding.Fix), "Invalid review finding.");
        ReviewJson.Require(reply.Verdict == "APPROVE" ? reply.Findings.All(f => f.Optional) : reply.Findings.Any(f => !f.Optional),
            "APPROVE cannot contain blocking findings; CHANGES requires a blocking finding.");
        var ledger = await ProjectionAsync(ct) ?? throw new McpException("No recoverable review checkpoint.");
        ReviewJson.Require(ledger.LatestPacketId == packetId && ledger.ReviewerPeerId == PeerId && ledger.DriverPeerId != PeerId &&
            ledger.State != "completed" && ledger.Integrity == "intact", "Only the selected independent reviewer may reply to the latest intact packet.");
        var packet = Latest(ledger)!;
        await Artifacts.MaterializeAsync(packetId, PeerStore(PeerId), packet.PacketSha256, ct);
        var prose = reply.Summary + "\n" + string.Join("\n", reply.Findings.Select((f, i) =>
            $"{i + 1}. {(f.Optional ? "OPTIONAL " : "")}{f.Severity} {f.Location}: {f.Finding} Fix: {f.Fix}")) + "\nVERDICT: " + reply.Verdict;
        var content = Record(new { kind = "verdict", phase = packet.Manifest.Phase, round = packet.Manifest.Round,
            packet_id = packetId, packet_sha256 = packet.PacketSha256 }, prose);
        var store = PeerStore(PeerId);
        using (store.Acquire())
        {
            var prior = store.Load<ReviewPeerState>() ?? new ReviewPeerState();
            ledger = await ProjectionAsync(ct, verifyArtifacts: false) ?? throw new McpException("Review checkpoint disappeared.");
            ReviewJson.Require(ledger.LatestPacketId == packetId && ledger.ReviewerPeerId == PeerId && ledger.Integrity == "intact",
                "Latest review request changed before reply.");
            var intent = prior.Replies.SingleOrDefault(r => r.PacketId == packetId);
            if (intent is not null)
            {
                ReviewJson.Require(intent.Content == content, "Changed same-packet verdict conflicts; require a new round.");
                var stored = intent.Message is null
                    ? (await MessagesAsync()).SingleOrDefault(m => m.AuthorPeerId == PeerId && !m.Quiet && m.Content == content && m.Mentions.SequenceEqual(new[] { intent.TargetPeerId }))
                    : await PinnedAsync(intent.Message);
                ReviewJson.Require(stored is not null, "Verdict dispatch was deleted/ambiguous; never recreate this round.");
                intent.Message = Reference(stored);
                if (intent.Delivery is null) ledger.State = "paused";
                prior.Projection = ledger; store.Save(prior);
                return PacketResult(ledger, Latest(ledger)!, new SendMessageResponse { ChannelId = channelId,
                    MessageId = stored.MessageId, Seq = stored.Seq, Mentions = stored.Mentions,
                    Delivery = intent.Delivery, Deduplicated = true });
            }
            intent = new ReviewReplyIntent { PacketId = packetId, Content = content, TargetPeerId = ledger.DriverPeerId };
            prior.Replies.Add(intent); prior.Projection = ledger; store.Save(prior);
            var response = await client.SendMessageAsync(channelId, new SendMessageRequest
            {
                AuthorPeerId = PeerId, To = [ledger.DriverPeerId], Quiet = false, Content = content,
                IdempotencyKey = "peers-review-v1:verdict:" + packetId,
            }) ?? throw new McpException("Review verdict send returned no result.");
            var confirmed = await ConfirmSendAsync(response, content, PeerId, ledger.DriverPeerId);
            intent.Message = Reference(confirmed); intent.Delivery = response.Delivery;
            await ReduceAsync(ledger, ct, verifyArtifacts: false);
            if (!Delivered(response, ledger.DriverPeerId)) ledger.State = "paused";
            prior.Projection = ledger; store.Save(prior);
            return PacketResult(ledger, Latest(ledger)!, response);
        }
    }
}
