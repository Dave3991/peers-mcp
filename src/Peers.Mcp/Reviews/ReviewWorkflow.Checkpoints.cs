using System.Text.Json;
using ModelContextProtocol;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Mcp.Reviews;

internal sealed partial class ReviewWorkflow
{
    private async Task<(ReviewLedger? Ledger, long Generation)> ReadDriverAsync(CancellationToken ct)
    {
        var store = DriverStore;
        ReviewLedger? ledger;
        long generation;
        using (store.Acquire()) { ledger = await LoadAsync(store); generation = store.Generation; }
        if (ledger is not null) await ReduceAsync(ledger, ct);
        return (ledger, generation);
    }
    private async Task PersistObservedAsync(ReviewLedger ledger, long generation)
    {
        var store = DriverStore;
        using (store.Acquire())
        {
            store.Load<ReviewLedger>();
            ReviewJson.Require(store.Generation == generation, "Review state changed during evidence verification; retry.");
            if (store.PayloadDigest != ReviewJson.Hash(ReviewJson.Bytes(ledger))) await PersistAsync(store, ledger);
        }
    }
    private async Task<ReviewSourceSnapshot> SourceAsync(ReviewLedger ledger, string? root, CancellationToken ct)
    {
        var source = await ReviewSnapshot.CaptureAsync(root ?? ledger.SourceRoot, ledger.BaseSha, ct);
        ReviewJson.Require(source.SourceRoot == ledger.SourceRoot && source.GitDir == ledger.GitDir && source.BaseSha == ledger.BaseSha,
            "Source root/git-dir/base lineage mismatch.");
        if (ledger.State != "completed" && Latest(ledger)?.Manifest is { Phase: "CODE" } packet &&
            source.RevisionSha256 != packet.RevisionSha256)
        {
            ledger.Stale = true;
            if (ledger.State == "paused") ledger.PrePauseState = "stale";
            else if (ledger.Integrity == "intact") ledger.State = "stale";
        }
        return source;
    }
    private static object StatusResult(ReviewLedger ledger, ReviewSourceSnapshot? source, object[]? unrelated = null,
        string[]? ready = null, string[]? acknowledged = null, string? reason = null, bool? completed = null) => new
    {
        phase = Latest(ledger)?.Manifest.Phase, round = Latest(ledger)?.Manifest.Round,
        packet_id = ledger.LatestPacketId, packet_sha256 = Latest(ledger)?.PacketSha256,
        reviewer_peer_id = ledger.ReviewerPeerId, driver_peer_id = ledger.DriverPeerId, state = ledger.State,
        request = Latest(ledger)?.Request, verdict = Latest(ledger)?.Verdict,
        approved_plan_packet_id = ledger.ApprovedPlanPacketId, revision_sha256 = source?.RevisionSha256,
        integrity = ledger.Integrity, source_changed_since_completion = ledger.State == "completed" && source is not null &&
            source.RevisionSha256 != ledger.CompletedRevision,
        next_action = ledger.State switch
        {
            "completed" => "Explicit optional lesson promotion; use a new channel for new work.",
            "stale" or "changes_requested" => "Run intended checks and publish a new managed round.",
            "approved" => Latest(ledger)?.Manifest.Phase == "PLAN" ? "Obtain checkpoint revision, run checks, publish CODE." : "Complete the exact approved CODE packet.",
            "paused" => "Resolve delivery/pause and explicitly resume.",
            "withdrawn" or "unverifiable" => "Evidence cannot authorize completion; create a new channel.",
            _ => "Wait for independent review; sync handled receipts.",
        },
        applied_through_seq = ledger.AppliedThroughSeq, completion = ledger.Completion,
        ready_to_ack = ready ?? [], unrelated_receipts = unrelated ?? [], acknowledged_inbox_ids = acknowledged ?? [],
        completed, reason,
    };
    internal async Task<object> CheckpointAsync(string action, string? sourceRoot, string? expectedPacketId,
        string[]? ackIds, string? reason, CancellationToken ct)
    {
        var mirrored = await MirrorAsync();
        ReviewLedger? ledger;
        long generation = 0;
        var isDriver = mirrored?.DriverPeerId == PeerId;
        if (isDriver || action == "resume") (ledger, generation) = await ReadDriverAsync(ct);
        else ledger = await ProjectionAsync(ct);
        if (ledger is null)
        {
            ReviewJson.Require(action is "status" or "sync", "No managed review exists for this channel.");
            ReviewJson.Require(ackIds is null || ackIds.Length == 0, "No previously handled receipts exist for this channel.");
            await MessagesAsync(); // Require an actual existing channel, even before its first request.
            var source = await ReviewSnapshot.CaptureAsync(sourceRoot ?? session.RepoRoot ?? Directory.GetCurrentDirectory(), "HEAD", ct);
            return new { state = "not_started", revision_sha256 = source.RevisionSha256, source_root = source.SourceRoot,
                next_action = "Publish PLAN/1 on this channel.", ready_to_ack = Array.Empty<string>(), unrelated_receipts = Array.Empty<object>() };
        }
        isDriver = ledger.DriverPeerId == PeerId;
        ReviewSourceSnapshot? current = null;
        if (isDriver) current = await SourceAsync(ledger, sourceRoot, ct);
        if (action == "status")
        {
            if (isDriver)
            {
                ledger.VerificationRevision = current!.RevisionSha256;
                await PersistObservedAsync(ledger, generation);
            }
            return StatusResult(ledger, current);
        }
        if (action == "sync")
        {
            if (isDriver)
            {
                ledger.VerificationRevision = current!.RevisionSha256;
                await PersistObservedAsync(ledger, generation);
            }
            return await SyncReceiptsAsync(ledger, current, ackIds, ct);
        }
        ReviewJson.Require(action == "resume" || isDriver, "Only the current driver may control managed completion.");
        if (action == "complete")
        {
            var packet = Latest(ledger);
            var gate = ledger.State == "completed" ? ledger.Integrity == "intact" && ledger.Completion is not null :
                ledger.State != "paused" && ledger.Integrity == "intact" && !ledger.Stale && packet?.Manifest.Phase == "CODE" &&
                packet.VerdictValue == "APPROVE" && packet.Verdict is not null && packet.Manifest.ReviewerPeerId != ledger.DriverPeerId &&
                ledger.ApprovedPlanPacketId is not null && packet.Manifest.ApprovedPlanPacketId == ledger.ApprovedPlanPacketId &&
                ledger.Packets.Any(p => p.PacketId == ledger.ApprovedPlanPacketId && p.VerdictValue == "APPROVE" &&
                    p.Manifest.ReviewerPeerId == ledger.ReviewerPeerId) && current?.RevisionSha256 == packet.Manifest.RevisionSha256;
            gate &= expectedPacketId == ledger.LatestPacketId && packet?.Manifest.Phase == "CODE";
            if (!gate)
            {
                await PersistObservedAsync(ledger, generation);
                return StatusResult(ledger, current, reason: "Completion requires the current intact independently approved PLAN/CODE lineage, unchanged source, and no stale/pause latch.", completed: false);
            }
            if (ledger.State == "completed") return StatusResult(ledger, current, completed: true);
        }
        if (action == "pause") ReviewJson.Require(ledger.State != "completed", "Completed work cannot be paused.");
        if (action == "resume" && !isDriver)
        {
            ReviewJson.Require(PeerId != ledger.ReviewerPeerId && !string.IsNullOrWhiteSpace(reason), "Reviewer cannot adopt; a new driver must explicitly resume with reason.");
            ReviewJson.Require(!(await client.ListPeersAsync()).Any(p => p.PeerId == ledger.DriverPeerId), "Previous driver is still live; adoption refused.");
            current = await SourceAsync(ledger, sourceRoot ?? session.RepoRoot ?? Directory.GetCurrentDirectory(), ct);
        }
        return await ControlAsync(ledger, generation, action, reason, current, ct);
    }
    private async Task<object> ControlAsync(ReviewLedger ledger, long generation, string operation, string? reason,
        ReviewSourceSnapshot? source, CancellationToken ct)
    {
        // Pinned facts and all immutable payloads were hydrated before acquiring this mutation lock.
        var store = DriverStore;
        using (store.Acquire())
        {
            store.Load<ReviewLedger>();
            ReviewJson.Require(store.Generation == generation, "Review state changed before control dispatch; retry.");
            await RecheckAsync(ledger, artifacts: false, ct);
            if (operation == "complete")
            {
                source = await SourceAsync(ledger, ledger.SourceRoot, ct);
                if (ledger.Stale)
                {
                    await PersistAsync(store, ledger);
                    return StatusResult(ledger, source, reason: "Source changed before completion dispatch.", completed: false);
                }
            }
            if (operation == "complete" && ledger.Integrity != "intact")
            {
                await PersistAsync(store, ledger);
                return StatusResult(ledger, source, reason: "Pinned evidence was removed before completion.", completed: false);
            }
            var content = Record(new { kind = "control", operation, packet_id = ledger.LatestPacketId,
                previous_driver_peer_id = ledger.DriverPeerId, driver_peer_id = PeerId, reason }, reason ?? operation);
            if (ledger.ControlIntent is { Dispatched: true } prior)
            {
                ReviewJson.Require(prior.Content == content, "A different control dispatch is unresolved.");
                var stored = (await MessagesAsync(prior.AfterSeq)).SingleOrDefault(m => m.AuthorPeerId == PeerId && !m.Quiet && m.Content == prior.Content);
                if (stored is null)
                {
                    IntegrityFailure(ledger, "unverifiable"); await PersistAsync(store, ledger);
                    throw new McpException("Control dispatch is ambiguous/deleted; it cannot be recreated.");
                }
                await ReduceAsync(ledger, ct, verifyArtifacts: false); ledger.ControlIntent = null; await PersistAsync(store, ledger);
                return StatusResult(ledger, source, completed: operation == "complete" ? ledger.State == "completed" : null);
            }
            ledger.ControlIntent = new ReviewControlIntent
            {
                Number = ++ledger.ControlCounter, Content = content, AfterSeq = ledger.AppliedThroughSeq, Dispatched = true,
            };
            // Adoption intent is persisted by the explicit successor while retaining previous-driver lineage until the loud record lands.
            await PersistAsync(store, ledger);
            // Controls are loud but unmentioned: completion/adoption does not require the historical
            // reviewer to stay online, and recipient replacement cannot rewrite control lineage.
            var response = await client.SendMessageAsync(channelId, new SendMessageRequest
            {
                AuthorPeerId = PeerId, Quiet = false, Content = content,
                IdempotencyKey = "peers-review-v1:control:" + ledger.ControlIntent.Number,
            }) ?? throw new McpException("Control send returned no result.");
            await ConfirmSendAsync(response, content, PeerId, target: null);
            // Once the send is confirmed, later cancellation/broker faults leave the intent recoverable.
            await ReduceAsync(ledger, ct, verifyArtifacts: false);
            ReviewJson.Require(ledger.Controls.Any(c => c.MessageId == response.MessageId), "Control was not accepted by authoritative history.");
            ledger.ControlIntent = null;
            await PersistAsync(store, ledger);
            return StatusResult(ledger, source, completed: operation == "complete" ? true : null);
        }
    }
    private async Task<object> SyncReceiptsAsync(ReviewLedger ledger, ReviewSourceSnapshot? source, string[]? ackIds, CancellationToken ct)
    {
        var store = PeerStore(ledger.ReviewerPeerId);
        using (store.Acquire())
        {
            var state = store.Load<ReviewPeerState>() ?? new ReviewPeerState();
            var acknowledged = new List<string>();
            if (ackIds is not null)
            {
                ReviewJson.Require(ackIds.Distinct(StringComparer.Ordinal).Count() == ackIds.Length &&
                    ackIds.All(id => state.Receipts.Any(r => r.PeerId == PeerId && r.InboxId == id)), "Unknown inbox ID; acknowledge only this peer's previous durable handled result.");
                // Persisted handled state precedes this RPC; replay repeats explicit IDs, never a transition or ack-all.
                await client.AckInboxAsync(PeerId, ackIds, cancellationToken: ct);
                for (var i = 0; i < state.Receipts.Count; i++)
                    if (ackIds.Contains(state.Receipts[i].InboxId, StringComparer.Ordinal))
                        state.Receipts[i] = state.Receipts[i] with { Acknowledged = true };
                acknowledged.AddRange(ackIds); store.Save(state);
            }
            var inbox = await client.GetInboxAsync(PeerId, limit: 500) ?? throw new McpException("Inbox returned no result.");
            var ready = new List<string>(); var unrelated = new List<object>();
            foreach (var item in inbox.Items)
            {
                ReviewJson.Require(item.PeerId == PeerId, "Inbox peer identity mismatch.");
                JsonElement payload;
                try { payload = JsonDocument.Parse(item.Event.Payload).RootElement.Clone(); }
                catch (JsonException) { unrelated.Add(new { inbox_id = item.InboxId, type = item.Event.Type }); continue; }
                var handled = false;
                if (Field(payload, "channel_id") == channelId)
                {
                    if (item.Event.Type == EventTypes.ChannelMessageAdded && payload.TryGetProperty("seq", out var seqValue) && seqValue.TryGetInt64(out var seq))
                    {
                        var page = await client.ReadChannelMessagesAsync(channelId, PeerId, seq - 1, 1);
                        var original = page?.Messages.FirstOrDefault();
                        if (original is null || original.Seq != seq || original.MessageId != Field(payload, "message_id") ||
                            original.AuthorPeerId != Field(payload, "author_peer_id") || original.Content != Field(payload, "content"))
                        { IntegrityFailure(ledger, "withdrawn"); }
                        else handled = seq <= ledger.AppliedThroughSeq;
                    }
                    else if (item.Event.Type is EventTypes.ChannelMessageRemoved or EventTypes.ChannelCleared)
                    { await RecheckAsync(ledger, artifacts: false, ct); handled = true; }
                }
                else if (item.Event.Type == EventTypes.ArtifactPublished && Field(payload, "ns") == Artifacts.Namespace) handled = true;
                if (!handled) { unrelated.Add(new { inbox_id = item.InboxId, type = item.Event.Type }); continue; }
                if (!state.Receipts.Any(r => r.InboxId == item.InboxId && r.PeerId == PeerId))
                    state.Receipts.Add(new(PeerId, item.InboxId, item.Event.Type, false));
                ready.Add(item.InboxId);
            }
            state.Projection = ledger; store.Save(state);
            return StatusResult(ledger, source, unrelated.ToArray(), ready.ToArray(), acknowledged.ToArray());
        }
    }
}
