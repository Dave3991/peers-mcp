using System.IO.Pipelines;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Peers.Broker.Hosting;
using Peers.Mcp;
using Peers.Mcp.Reviews;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.IntegrationTests;

public sealed class ReviewWorkflowTests
{
    [Fact]
    public async Task Independent_rounds_hydrate_complete_evidence_and_latch_stale_until_new_CODE()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var plan = await f.PublishPlan(); var planId = plan["packet_id"]!.GetValue<string>();
        var fetched = await f.Reviewer.Call("review_packet", new { action = "fetch", channel_id = f.ChannelId, packet_id = planId });
        Assert.Equal(plan["packet_sha256"]!.GetValue<string>(), ReviewJson.Hash(await File.ReadAllBytesAsync(fetched["manifest_path"]!.GetValue<string>())));
        Assert.Equal(f.TaskText, await File.ReadAllTextAsync(fetched["task_path"]!.GetValue<string>()));
        await f.Reply(planId, "APPROVE");
        await File.WriteAllTextAsync(Path.Combine(f.Root, "value.txt"), "2\n");
        var binary = new byte[] { 0, 255, 128, 42 };
        await File.WriteAllBytesAsync(Path.Combine(f.Root, "new.bin"), binary);
        await File.WriteAllTextAsync(f.LogPath, string.Concat(Enumerable.Repeat("Čeština 🧪\n", 100000)) + "# pass 1\n");
        var code1 = await f.PublishCode(1, planId); var id1 = code1["packet_id"]!.GetValue<string>();
        fetched = await f.Reviewer.Call("review_packet", new { action = "fetch", channel_id = f.ChannelId, packet_id = id1 });
        var files = fetched["files"]!.AsArray();
        Assert.Equal(await File.ReadAllBytesAsync(f.LogPath), await File.ReadAllBytesAsync(files.Single(x => x!["path"]!.GetValue<string>() == "evidence/0000-test.log")!["local_path"]!.GetValue<string>()));
        Assert.Equal(binary, await File.ReadAllBytesAsync(files.Single(x => x!["path"]!.GetValue<string>() == "untracked/new.bin")!["local_path"]!.GetValue<string>()));
        var rawChat = await f.Broker.SendMessageAsync(f.ChannelId, new SendMessageRequest { AuthorPeerId = f.Reviewer.Id, To = [f.Driver.Id], Content = "Raw chat\nVERDICT: APPROVE" });
        Assert.False((await f.Complete(id1))["completed"]!.GetValue<bool>());
        await f.Reviewer.Error("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = id1,
            reply = new { verdict = "APPROVE", summary = "Invalid blocking approval", findings = new[] { new { severity = "HIGH", location = "test", finding = "missing coverage", fix = "run intended test", optional = false } } } });
        await f.Reply(id1, "CHANGES");
        var code2 = await f.PublishCode(2, id1); var id2 = code2["packet_id"]!.GetValue<string>();
        await f.Reply(id2, "APPROVE", new string('x', 4000));
        var sync = await f.Status("sync"); Assert.Equal("approved", sync["state"]!.GetValue<string>());
        var handled = sync["ready_to_ack"]!.AsArray().Select(x => x!.GetValue<string>()).ToArray(); Assert.NotEmpty(handled);
        // Raw chat is no review record, so sync leaves its receipt for the agent to read.
        Assert.DoesNotContain(await f.ReceiptOf(rawChat!.MessageId), handled);
        await f.Broker.AckInboxAsync(f.Driver.Id, handled); // Ordinary wait/ack cannot erase channel approval.
        await File.WriteAllTextAsync(Path.Combine(f.Root, "value.txt"), "3\n");
        Assert.Equal("stale", (await f.Status())["state"]!.GetValue<string>());
        await File.WriteAllTextAsync(Path.Combine(f.Root, "value.txt"), "2\n");
        Assert.False((await f.Complete(id2))["completed"]!.GetValue<bool>());
        var code3 = await f.PublishCode(3, id2); var id3 = code3["packet_id"]!.GetValue<string>();
        await f.Reply(id3, "APPROVE"); Assert.True((await f.Complete(id3))["completed"]!.GetValue<bool>());
        await f.Git("add", "."); await f.Git("commit", "-qm", "approved");
        var status = await f.Status(); Assert.Equal("completed", status["state"]!.GetValue<string>()); Assert.True(status["source_changed_since_completion"]!.GetValue<bool>());
        await f.Broker.RemoveMessagesAsync(f.ChannelId, new RemoveMessagesRequest { MessageIds = [status["completion"]!["message_id"]!.GetValue<string>()] });
        status = await f.Status(); Assert.Equal("completed", status["state"]!.GetValue<string>()); Assert.Equal("withdrawn", status["integrity"]!.GetValue<string>());
        Assert.False((await f.Complete(id3))["completed"]!.GetValue<bool>());
        await f.Driver.Error("review_lesson", new { action = "prepare", channel_id = f.ChannelId, proposal = f.Proposal(planId, id3) });
    }

    [Fact]
    public async Task Capture_drift_sends_no_request_and_deleted_round_cannot_be_recreated()
    {
        await using var f = await ReviewFixture.CreateAsync();
        f.Handler.ChangeOnBlob = () => File.WriteAllText(Path.Combine(f.Root, "value.txt"), "changed during upload\n");
        await f.Driver.Error("review_packet", new { action = "publish", channel_id = f.ChannelId, request = f.PlanRequest() });
        Assert.Empty((await f.Broker.ReadChannelMessagesAsync(f.ChannelId, f.Driver.Id))!.Messages);
        var plan = await f.PublishPlan(); var id = plan["packet_id"]!.GetValue<string>();
        Assert.Equal(id, (await f.PublishPlan())["packet_id"]!.GetValue<string>());
        await f.Driver.Error("review_packet", new { action = "publish", channel_id = f.ChannelId, request = f.PlanRequest(task: "different task") });
        await f.Broker.RemoveMessagesAsync(f.ChannelId, new RemoveMessagesRequest { MessageIds = [plan["message_id"]!.GetValue<string>()] });
        await f.Driver.Error("review_packet", new { action = "publish", channel_id = f.ChannelId, request = f.PlanRequest() });
        Assert.Empty((await f.Broker.ReadChannelMessagesAsync(f.ChannelId, f.Driver.Id))!.Messages);
    }

    [Fact]
    public async Task Reviewer_replacement_requires_its_own_PLAN_and_rejects_wrong_author_old_round_and_changed_lineage()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var plan = await f.PublishPlan(); var p1 = plan["packet_id"]!.GetValue<string>(); await f.Reply(p1, "APPROVE");
        await using var replacement = await f.NewPeer("replacement");
        var p2 = await f.Driver.Call("review_packet", new { action = "publish", channel_id = f.ChannelId,
            request = f.PlanRequest(round: 2, previous: p1, reviewer: replacement.Id) }); var p2id = p2["packet_id"]!.GetValue<string>();
        var revision = (await f.Status())["revision_sha256"]!.GetValue<string>();
        await f.Driver.Error("review_packet", new { action = "publish", channel_id = f.ChannelId,
            request = f.CodeRequest(1, p2id, revision, replacement.Id) });
        await f.Reviewer.Error("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = p2id, reply = new { verdict = "APPROVE", summary = "Old reviewer", findings = Array.Empty<object>() } });
        await replacement.Call("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = p2id, reply = new { verdict = "APPROVE", summary = "Replacement reviewed PLAN", findings = Array.Empty<object>() } });
        var c1 = await f.Driver.Call("review_packet", new { action = "publish", channel_id = f.ChannelId, request = f.CodeRequest(1, p2id, revision, replacement.Id) });
        var c1id = c1["packet_id"]!.GetValue<string>();
        await f.Broker.SendMessageAsync(f.ChannelId, new SendMessageRequest { AuthorPeerId = f.Reviewer.Id, To = [f.Driver.Id], Content = $"PEERS_REVIEW_V1\n{{\"kind\":\"verdict\",\"phase\":\"CODE\",\"round\":1,\"packet_id\":\"{c1id}\",\"packet_sha256\":\"{c1["packet_sha256"]}\"}}\nWrong author\nVERDICT: APPROVE" });
        Assert.False((await f.Complete(c1id))["completed"]!.GetValue<bool>());
        await replacement.Call("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = c1id, reply = new { verdict = "APPROVE", summary = "Current CODE review", findings = Array.Empty<object>() } });
        await f.Git("commit", "--allow-empty", "-qm", "head changed");
        Assert.False((await f.Complete(c1id))["completed"]!.GetValue<bool>());
    }

    [Fact]
    public async Task Explicit_adoption_restore_and_lost_ack_preserve_completion_receipts_and_one_save_grant()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var plan = await f.PublishPlan(); var p = plan["packet_id"]!.GetValue<string>(); await f.Reply(p, "APPROVE");
        var code = await f.PublishCode(1, p); var c = code["packet_id"]!.GetValue<string>(); await f.Reply(c, "APPROVE");
        var sync = await f.Status("sync"); var ids = sync["ready_to_ack"]!.AsArray().Select(x => x!.GetValue<string>()).ToArray();
        f.Handler.LoseNextAck = true;
        await f.Driver.Error("review_checkpoint", new { action = "sync", channel_id = f.ChannelId, ack_inbox_ids = ids });
        var replay = await f.Driver.Call("review_checkpoint", new { action = "sync", channel_id = f.ChannelId, ack_inbox_ids = ids });
        Assert.Equal(sync["verdict"]!["message_id"]!.GetValue<string>(), replay["verdict"]!["message_id"]!.GetValue<string>());
        Assert.NotEmpty(replay["unrelated_receipts"]!.AsArray());
        await f.Driver.Error("review_checkpoint", new { action = "sync", channel_id = f.ChannelId, ack_inbox_ids = new[] { "unknown" } });
        Assert.True((await f.Complete(c))["completed"]!.GetValue<bool>());
        await using var successor = await f.NewPeer("successor");
        await successor.Error("review_checkpoint", new { action = "resume", channel_id = f.ChannelId, source_root = f.Root, reason = "adopt" });
        await f.Reviewer.Error("review_checkpoint", new { action = "resume", channel_id = f.ChannelId, source_root = f.Root, reason = "reviewer cannot adopt" });
        await f.Broker.DeregisterAsync(f.Driver.Id);
        var adopted = await successor.Call("review_checkpoint", new { action = "resume", channel_id = f.ChannelId, source_root = f.Root, reason = "offline driver handoff" });
        Assert.Equal("completed", adopted["state"]!.GetValue<string>());
        File.Delete(Path.Combine(f.Storage, "review-workflows", ReviewJson.Hash(f.ChannelId), "driver", "state.json"));
        Assert.Equal("completed", (await successor.Call("review_checkpoint", new { action = "status", channel_id = f.ChannelId }))["state"]!.GetValue<string>());
        var proposal = f.Proposal(p, c);
        var noEntry = await successor.Call("review_lesson", new { action = "prepare", channel_id = f.ChannelId, proposal = new { cue = "x", lesson = "x", confidence = "high", confidence_reason = "x", effect = "confounded", before = Array.Empty<object>(), after = Array.Empty<object>() } });
        Assert.Equal("no_entry", noEntry["status"]!.GetValue<string>());
        var prepared = await successor.Call("review_lesson", new { action = "prepare", channel_id = f.ChannelId, proposal });
        Assert.Equal(f.Root, prepared["binding_request"]!["workingDir"]!.GetValue<string>());
        var binding = new { workspaceBindingId = "wb_" + new string('1', 32), identityConfidence = "low", bindingStatus = "observed" };
        var grant = await successor.Call("review_lesson", new { action = "begin_save", channel_id = f.ChannelId, binding_result = binding });
        Assert.True(grant["dispatch_allowed"]!.GetValue<bool>());
        var repeat = await successor.Call("review_lesson", new { action = "begin_save", channel_id = f.ChannelId, binding_result = binding });
        Assert.False(repeat["dispatch_allowed"]!.GetValue<bool>());
        await successor.Error("review_lesson", new { action = "prepare", channel_id = f.ChannelId, proposal = f.Proposal(p, c, "NUL\0cue") });
        var reconcile = await successor.Call("review_lesson", new { action = "reconcile", channel_id = f.ChannelId, binding_result = binding, attempt_id = grant["attempt_id"]!.GetValue<string>(), result = new { observations = Array.Empty<object>() } });
        Assert.Equal("unknown", reconcile["status"]!.GetValue<string>());
    }

    [Fact]
    public async Task Lost_request_and_control_responses_recover_without_poisoning_intact_rounds()
    {
        await using var f = await ReviewFixture.CreateAsync();
        f.Handler.LoseNextSendKind = "request";
        await f.Driver.Error("review_packet", new { action = "publish", channel_id = f.ChannelId, request = f.PlanRequest() });
        var recovered = await f.PublishPlan(); var p = recovered["packet_id"]!.GetValue<string>();
        Assert.True(recovered["deduplicated"]!.GetValue<bool>());
        Assert.Single((await f.Broker.ReadChannelMessagesAsync(f.ChannelId, f.Driver.Id))!.Messages);
        await f.Reply(p, "APPROVE");
        var code = await f.PublishCode(1, p); var c = code["packet_id"]!.GetValue<string>();
        await f.Reply(c, "APPROVE");
        f.Handler.LoseNextSendKind = "control";
        await f.Driver.Error("review_checkpoint", new { action = "complete", channel_id = f.ChannelId, expected_packet_id = c });
        var status = await f.Status();
        Assert.Equal("completed", status["state"]!.GetValue<string>());
        Assert.Equal("intact", status["integrity"]!.GetValue<string>());
        Assert.True((await f.Complete(c))["completed"]!.GetValue<bool>());
        await f.Broker.DeregisterAsync(f.Driver.Id);
        await using var successor = await f.NewPeer("successor-after-lost-complete");
        Assert.Equal("completed", (await successor.Call("review_checkpoint", new { action = "resume", channel_id = f.ChannelId, source_root = f.Root, reason = "recover confirmed completion" }))["state"]!.GetValue<string>());
    }

    // A lost request response leaves delivery unknown, so every deduplicated retry pauses the round. A
    // verdict reduced between two retries is held in PrePauseState; the second retry must not overwrite
    // it with "paused", or resume restores "paused" and the approval is lost.
    [Fact]
    public async Task Second_deduplicated_retry_keeps_a_verdict_that_arrived_between_retries()
    {
        await using var f = await ReviewFixture.CreateAsync();
        f.Handler.LoseNextSendKind = "request";
        await f.Driver.Error("review_packet", new { action = "publish", channel_id = f.ChannelId, request = f.PlanRequest() });
        var first = await f.PublishPlan(); var p = first["packet_id"]!.GetValue<string>();
        Assert.True(first["deduplicated"]!.GetValue<bool>());
        Assert.Equal("paused", first["state"]!.GetValue<string>());
        await f.Reply(p, "APPROVE");
        var second = await f.PublishPlan();
        Assert.True(second["deduplicated"]!.GetValue<bool>());
        Assert.Equal("paused", second["state"]!.GetValue<string>());
        Assert.Equal("approved", (await f.Driver.Call("review_checkpoint", new { action = "resume", channel_id = f.ChannelId }))["state"]!.GetValue<string>());
    }

    // Sync counts as handled only the messages the review ledger holds. A plain question in the review
    // channel appears in no review result, so it must stay an unrelated receipt the agent reads, never
    // one it acknowledges unseen.
    [Fact]
    public async Task Sync_keeps_a_plain_question_in_the_review_channel_out_of_ready_to_ack()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var p = (await f.PublishPlan())["packet_id"]!.GetValue<string>();
        var question = await f.Broker.SendMessageAsync(f.ChannelId, new SendMessageRequest
            { AuthorPeerId = f.Reviewer.Id, To = [f.Driver.Id], Content = "Question: should value.txt keep its trailing newline?" });
        await f.Reply(p, "APPROVE");

        var sync = await f.Status("sync");
        Assert.Equal("approved", sync["state"]!.GetValue<string>());
        Assert.True(sync["applied_through_seq"]!.GetValue<long>() > question!.Seq);
        var ready = sync["ready_to_ack"]!.AsArray().Select(x => x!.GetValue<string>()).ToArray();
        var unrelated = sync["unrelated_receipts"]!.AsArray().Select(x => x!["inbox_id"]!.GetValue<string>()).ToArray();
        var questionReceipt = await f.ReceiptOf(question.MessageId);
        Assert.DoesNotContain(questionReceipt, ready);
        Assert.Contains(questionReceipt, unrelated);
        Assert.Contains(await f.ReceiptOf(sync["verdict"]!["message_id"]!.GetValue<string>()), ready);
    }

    // A well-formed record the reducer rejected is no more visible than chat: here the selected reviewer
    // sends a late verdict for PLAN/1 after CODE/1 is out. It parses as PEERS_REVIEW_V1, but the ledger
    // never holds it, so sync must leave it for the agent to read.
    [Fact]
    public async Task Sync_keeps_a_rejected_late_verdict_for_an_older_round_out_of_ready_to_ack()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var plan = await f.PublishPlan(); var p = plan["packet_id"]!.GetValue<string>(); await f.Reply(p, "APPROVE");
        var planVerdict = (await f.Status())["verdict"]!["message_id"]!.GetValue<string>();
        await f.PublishCode(1, p);
        var late = await f.Broker.SendMessageAsync(f.ChannelId, new SendMessageRequest { AuthorPeerId = f.Reviewer.Id, To = [f.Driver.Id],
            Content = $"PEERS_REVIEW_V1\n{{\"kind\":\"verdict\",\"phase\":\"PLAN\",\"round\":1,\"packet_id\":\"{p}\",\"packet_sha256\":\"{plan["packet_sha256"]!.GetValue<string>()}\"}}\nLate second look at PLAN/1\nVERDICT: CHANGES" });

        var sync = await f.Status("sync");
        Assert.Equal("awaiting_review", sync["state"]!.GetValue<string>()); // Rejected: it changed nothing.
        Assert.True(sync["applied_through_seq"]!.GetValue<long>() >= late!.Seq);
        var ready = sync["ready_to_ack"]!.AsArray().Select(x => x!.GetValue<string>()).ToArray();
        var unrelated = sync["unrelated_receipts"]!.AsArray().Select(x => x!["inbox_id"]!.GetValue<string>()).ToArray();
        var lateReceipt = await f.ReceiptOf(late.MessageId);
        Assert.DoesNotContain(lateReceipt, ready);
        Assert.Contains(lateReceipt, unrelated);
        Assert.Contains(await f.ReceiptOf(planVerdict), ready);
    }

    // Agents type severity and verdict in any case. Both match case-insensitively and are stored in their
    // canonical spelling, so the record still reduces and the lesson hint still parses the finding line.
    [Fact]
    public async Task Reply_accepts_lowercase_severity_and_verdict_and_stores_them_canonically()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var p = (await f.PublishPlan())["packet_id"]!.GetValue<string>();
        await f.Reviewer.Call("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = p,
            reply = new { verdict = "changes", summary = "The plan names no test", findings = new[] { new { severity = "high", location = "plan.md", finding = "No test named", fix = "Name the test", optional = false } } } });
        var status = await f.Status();
        Assert.Equal("changes_requested", status["state"]!.GetValue<string>());
        var stored = Assert.Single((await f.Broker.ReadChannelMessagesAsync(f.ChannelId, f.Driver.Id))!.Messages,
            m => m.MessageId == status["verdict"]!["message_id"]!.GetValue<string>());
        Assert.Contains("\n1. HIGH plan.md: No test named Fix: Name the test\n", stored.Content);
        Assert.EndsWith("\nVERDICT: CHANGES", stored.Content);
    }

    // Observed in a replay of real agents: 59 of 68 replies were rejected with a bare "Invalid review
    // finding". An error must name the field, the value it got, and every allowed value.
    [Fact]
    public async Task Reply_errors_name_the_field_the_value_and_the_allowed_values()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var p = (await f.PublishPlan())["packet_id"]!.GetValue<string>();
        var finding = new { severity = "HIGH", location = "plan.md", finding = "No test named", fix = "Name the test", optional = false };
        var verdict = await f.Reviewer.Error("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = p,
            reply = new { verdict = "REQUEST_CHANGES", summary = "The plan names no test", findings = new[] { finding } } });
        Assert.Contains("reply.verdict is \"REQUEST_CHANGES\"; allowed values: APPROVE, CHANGES (case-insensitive).", verdict);
        var severity = await f.Reviewer.Error("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = p,
            reply = new { verdict = "CHANGES", summary = "The plan names no test", findings = new[] { finding with { severity = "CRITICAL" } } } });
        Assert.Contains("reply.findings[0].severity is \"CRITICAL\"; allowed values: HIGH, MED, LOW (case-insensitive).", severity);
        Assert.Single((await f.Broker.ReadChannelMessagesAsync(f.ChannelId, f.Driver.Id))!.Messages); // Only the PLAN request.
    }

    [Fact]
    public async Task Publish_error_for_a_wrong_evidence_kind_names_kind_and_its_allowed_values()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var error = await f.Driver.Error("review_packet", new { action = "publish", channel_id = f.ChannelId, request = new
        {
            phase = "CODE", round = 1, task = f.TaskText, base_sha = f.Base, reviewer_peer_id = f.Reviewer.Id, change_summary = "code",
            uncertainties = "none", source_root = f.Root, verification_revision = "revision",
            evidence = new[] { new { path = f.LogPath, kind = "unit", command = "dotnet test", cwd = f.Root, exit_code = 0 } },
        } });
        Assert.Contains("request.evidence[0].kind is \"unit\"; allowed values: build, test, smoke, other.", error);
    }

    // A missing field or a JSON value of the wrong type used to fail argument binding inside the SDK,
    // which answered only "An error occurred invoking 'review_packet'." Both now name the field.
    [Fact]
    public async Task Missing_and_mistyped_reply_fields_are_named_instead_of_an_opaque_binding_error()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var p = (await f.PublishPlan())["packet_id"]!.GetValue<string>();
        var missing = await f.Reviewer.Error("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = p,
            reply = new { verdict = "CHANGES", summary = "s", findings = new[] { new { severity = "HIGH", location = "x", finding = "y" } } } });
        Assert.Contains("reply.findings[0].fix is missing or empty; it must be non-blank text.", missing);
        var mistyped = await f.Reviewer.Error("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = p,
            reply = new { verdict = "CHANGES", summary = "s", findings = new[] { new { severity = "HIGH", location = "x", finding = "y", fix = "z", optional = "no" } } } });
        Assert.Contains("reply.findings[0].optional is \"no\"; it must be true or false.", mistyped);
        var topLevel = await f.Reviewer.Error("review_packet", new { action = "fetch", channel_id = f.ChannelId, packet_id = 5 });
        Assert.Contains("packet_id is 5; it must be a string.", topLevel);
    }

    [Fact]
    public async Task Repeated_pause_and_resume_preserve_approval_with_offline_or_replaced_reviewer()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var plan = await f.PublishPlan(); var p = plan["packet_id"]!.GetValue<string>(); await f.Reply(p, "APPROVE");
        var code = await f.PublishCode(1, p); var c = code["packet_id"]!.GetValue<string>(); await f.Reply(c, "APPROVE");
        await using var replacementSession = await f.NewPeer("reviewer");
        await f.Broker.DeregisterAsync(f.Reviewer.Id);
        var pause = new { action = "pause", channel_id = f.ChannelId, reason = "user pause" };
        Assert.Equal("paused", (await f.Driver.Call("review_checkpoint", pause))["state"]!.GetValue<string>());
        Assert.Equal("paused", (await f.Driver.Call("review_checkpoint", pause))["state"]!.GetValue<string>());
        Assert.Equal("approved", (await f.Driver.Call("review_checkpoint", new { action = "resume", channel_id = f.ChannelId }))["state"]!.GetValue<string>());
        Assert.True((await f.Complete(c))["completed"]!.GetValue<bool>());
        var controls = (await f.Broker.ReadChannelMessagesAsync(f.ChannelId, f.Driver.Id))!.Messages.Where(m => m.Content.Contains("\"kind\":\"control\"", StringComparison.Ordinal));
        Assert.All(controls, message => { Assert.False(message.Quiet); Assert.Empty(message.Mentions); });
    }

    [Fact]
    public async Task Deleted_reviewer_verdict_is_not_recreated_by_retry()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var plan = await f.PublishPlan(); var p = plan["packet_id"]!.GetValue<string>(); await f.Reply(p, "APPROVE");
        var status = await f.Status();
        await f.Broker.RemoveMessagesAsync(f.ChannelId, new RemoveMessagesRequest { MessageIds = [status["verdict"]!["message_id"]!.GetValue<string>()] });
        await f.Reviewer.Error("review_packet", new { action = "reply", channel_id = f.ChannelId, packet_id = p,
            reply = new { verdict = "APPROVE", summary = "Independent evidence review", findings = Array.Empty<object>() } });
        Assert.Single((await f.Broker.ReadChannelMessagesAsync(f.ChannelId, f.Driver.Id))!.Messages);
        Assert.Equal("withdrawn", (await f.Status())["state"]!.GetValue<string>());
    }

    [Fact]
    public async Task Checkpoint_larger_than_one_artifact_restores_complete_lesson_evidence_without_a_round_cap()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var plan = await f.PublishPlan(); var p = plan["packet_id"]!.GetValue<string>(); await f.Reply(p, "APPROVE");
        var quote = new string('x', 1_100_000);
        await File.WriteAllTextAsync(f.LogPath, quote);
        var code = await f.PublishCode(1, p); var c = code["packet_id"]!.GetValue<string>(); await f.Reply(c, "APPROVE");
        Assert.True((await f.Complete(c))["completed"]!.GetValue<bool>());
        var proposal = new { cue = "Large retained contrast", lesson = "Preserve complete evidence", effect = "helped", confidence = "high", confidence_reason = "Literal retained evidence",
            before = new[] { new { packet_id = p, path = "task.txt", quote = "Set value to two" } },
            after = new[] { new { packet_id = c, path = "evidence/0000-test.log", quote } } };
        var prepared = await f.Driver.Call("review_lesson", new { action = "prepare", channel_id = f.ChannelId, proposal });
        Assert.Contains(quote, prepared["body"]!.GetValue<string>());
        File.Delete(Path.Combine(f.Storage, "review-workflows", ReviewJson.Hash(f.ChannelId), "driver", "state.json"));
        Assert.Equal("completed", (await f.Status())["state"]!.GetValue<string>());
        var recovered = await f.Driver.Call("review_lesson", new { action = "prepare", channel_id = f.ChannelId, proposal });
        Assert.Equal(prepared["lesson_id"]!.GetValue<string>(), recovered["lesson_id"]!.GetValue<string>());
        Assert.Equal(prepared["body"]!.GetValue<string>(), recovered["body"]!.GetValue<string>());
    }

    [Fact]
    public async Task Completion_after_a_CHANGES_round_hints_at_an_optional_lesson_and_saves_nothing()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var p = (await f.PublishPlan())["packet_id"]!.GetValue<string>(); await f.Reply(p, "APPROVE");
        var c1 = (await f.PublishCode(1, p))["packet_id"]!.GetValue<string>(); await f.Reply(c1, "CHANGES");
        var changes = (await f.Status())["verdict"]!["message_id"]!.GetValue<string>();
        var c2 = (await f.PublishCode(2, c1))["packet_id"]!.GetValue<string>(); await f.Reply(c2, "APPROVE");
        var completed = await f.Complete(c2);
        Assert.True(completed["completed"]!.GetValue<bool>());
        var hint = completed["lesson_hint"]!;
        var before = Assert.Single(hint["before"]!.AsArray())!;
        Assert.Equal(c1, before["packet_id"]!.GetValue<string>());
        Assert.Equal(changes, before["verdict_message_id"]!.GetValue<string>());
        Assert.Equal("1. HIGH verification: Missing coverage Fix: Run intended checks", Assert.Single(before["findings"]!.AsArray())!.GetValue<string>());
        Assert.Equal(c2, hint["after"]!.GetValue<string>());
        Assert.Equal("Optional: run review_lesson prepare with a cue and lesson based on these findings; nothing is saved automatically.", hint["next_action"]!.GetValue<string>());
        // Read-only: no lesson intent exists to save, and a repeated completion reports the same hint.
        Assert.Contains("Prepare a supported lesson", await f.Driver.Error("review_lesson", new { action = "begin_save", channel_id = f.ChannelId,
            binding_result = new { workspaceBindingId = "wb_" + new string('1', 32), identityConfidence = "low", bindingStatus = "observed" } }));
        Assert.True(JsonNode.DeepEquals(hint, (await f.Complete(c2))["lesson_hint"]));
    }

    [Fact]
    public async Task First_round_approval_completes_without_a_lesson_hint()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var p = (await f.PublishPlan())["packet_id"]!.GetValue<string>(); await f.Reply(p, "APPROVE");
        var c = (await f.PublishCode(1, p))["packet_id"]!.GetValue<string>(); await f.Reply(c, "APPROVE");
        var completed = await f.Complete(c);
        Assert.True(completed["completed"]!.GetValue<bool>());
        Assert.Null(completed["lesson_hint"]);
    }

    // The hint is read after the completion was sent and persisted; a broker fault at that moment
    // drops the hint and must not turn the finished completion into a tool error.
    [Fact]
    public async Task Completion_survives_a_broker_fault_while_reading_the_lesson_hint()
    {
        await using var f = await ReviewFixture.CreateAsync();
        var p = (await f.PublishPlan())["packet_id"]!.GetValue<string>(); await f.Reply(p, "APPROVE");
        var c1 = (await f.PublishCode(1, p))["packet_id"]!.GetValue<string>(); await f.Reply(c1, "CHANGES");
        var c2 = (await f.PublishCode(2, c1))["packet_id"]!.GetValue<string>(); await f.Reply(c2, "APPROVE");
        f.Handler.FailReadAfterPersistedSendKind = "control";
        var completed = await f.Complete(c2);
        Assert.Null(f.Handler.FailReadAfterPersistedSendKind); // The fault fired on the hint's read.
        Assert.True(completed["completed"]!.GetValue<bool>());
        Assert.Equal("completed", completed["state"]!.GetValue<string>());
        Assert.Null(completed["lesson_hint"]);
        Assert.NotNull((await f.Complete(c2))["lesson_hint"]);
    }
}

internal sealed class ReviewFixture : IAsyncDisposable
{
    internal string Temporary { get; } = Path.Combine(Path.GetTempPath(), "peers-protocol-" + Guid.NewGuid().ToString("N"));
    internal string Root { get; private set; } = "";
    internal string Storage => Path.Combine(Temporary, "state");
    internal string LogPath => Path.Combine(Path.GetDirectoryName(Root)!, "test.log");
    internal string PlanPath => Path.Combine(Path.GetDirectoryName(Root)!, "plan.md");
    internal string TaskText => "Set value to two; preserve Čeština 🧪.";
    internal string Base { get; private set; } = "";
    internal string ChannelId { get; private set; } = "";
    internal BrokerClient Broker { get; private set; } = null!;
    internal ReviewFaultHandler Handler { get; private set; } = null!;
    internal ReviewTestPeer Driver { get; private set; } = null!;
    internal ReviewTestPeer Reviewer { get; private set; } = null!;
    private WebApplication _app = null!;
    private readonly List<ReviewTestPeer> _peers = [];
    internal static async Task<ReviewFixture> CreateAsync()
    {
        var f = new ReviewFixture(); Directory.CreateDirectory(f.Temporary); f.Root = Path.Combine(f.Temporary, "repo"); Directory.CreateDirectory(f.Root);
        await f.Git("init", "-q"); f.Root = await ReviewSnapshot.GitTextAsync(f.Root, "rev-parse", "--show-toplevel");
        await f.Git("config", "user.email", "fixture@example.invalid"); await f.Git("config", "user.name", "fixture");
        await File.WriteAllTextAsync(Path.Combine(f.Root, "value.txt"), "1\n"); await f.Git("add", "."); await f.Git("commit", "-qm", "baseline");
        f.Base = await ReviewSnapshot.GitTextAsync(f.Root, "rev-parse", "HEAD");
        await File.WriteAllTextAsync(f.PlanPath, "Set value to two. Inspect actual test counts.\n"); await File.WriteAllTextAsync(f.LogPath, "# tests 1\n# pass 1\n");
        var builder = WebApplication.CreateBuilder(); builder.WebHost.UseTestServer(); BrokerHost.AddBrokerServices(builder, Path.Combine(f.Temporary, "peers.db"));
        f._app = builder.Build(); BrokerHost.MapBrokerRoutes(f._app); await f._app.StartAsync();
        f.Handler = new ReviewFaultHandler(f._app.GetTestServer().CreateHandler());
        f.Broker = new BrokerClient(new HttpClient(f.Handler) { BaseAddress = new Uri("http://localhost") });
        f.Driver = await f.NewPeer("driver"); f.Reviewer = await f.NewPeer("reviewer");
        f.ChannelId = (await f.Broker.CreateChannelAsync(new CreateChannelRequest { Name = "review", RepoRoot = f.Root, CreatedByPeerId = f.Driver.Id })).ChannelId;
        return f;
    }
    internal async Task<ReviewTestPeer> NewPeer(string name)
    {
        var id = (await Broker.RegisterAsync(new RegisterPeerRequest { Tool = name, MachineId = "fixture", RepoRoot = Root, Branch = "main" })).PeerId;
        var peer = await ReviewTestPeer.Create(Broker, id, Root, Storage); _peers.Add(peer); return peer;
    }
    internal object PlanRequest(int round = 1, string? previous = null, string? reviewer = null, string? task = null) => new
    { phase = "PLAN", round, task = task ?? TaskText, base_sha = Base, reviewer_peer_id = reviewer ?? Reviewer.Id,
        change_summary = "plan", uncertainties = "counts require inspection", expected_previous_packet_id = previous, source_root = Root, plan_path = PlanPath };
    internal object CodeRequest(int round, string previous, string revision, string? reviewer = null) => new
    { phase = "CODE", round, task = TaskText, base_sha = Base, reviewer_peer_id = reviewer ?? Reviewer.Id, change_summary = "code", uncertainties = "evidence is a driver claim",
        expected_previous_packet_id = previous, source_root = Root, verification_revision = revision,
        evidence = new[] { new { path = LogPath, kind = "test", command = "native driver test", cwd = Root, exit_code = 0 } } };
    internal Task<JsonNode> PublishPlan() => Driver.Call("review_packet", new { action = "publish", channel_id = ChannelId, request = PlanRequest() });
    internal async Task<JsonNode> PublishCode(int round, string previous)
    {
        var revision = (await Status())["revision_sha256"]!.GetValue<string>();
        return await Driver.Call("review_packet", new { action = "publish", channel_id = ChannelId, request = CodeRequest(round, previous, revision) });
    }
    internal Task<JsonNode> Reply(string id, string verdict, string summary = "Independent evidence review") => Reviewer.Call("review_packet", new
    { action = "reply", channel_id = ChannelId, packet_id = id, reply = new { verdict, summary, findings = verdict == "APPROVE" ? Array.Empty<object>() : new object[] { new { severity = "HIGH", location = "verification", finding = "Missing coverage", fix = "Run intended checks", optional = false } } } });
    internal Task<JsonNode> Status(string action = "status") => Driver.Call("review_checkpoint", new { action, channel_id = ChannelId });
    internal Task<JsonNode> Complete(string id) => Driver.Call("review_checkpoint", new { action = "complete", channel_id = ChannelId, expected_packet_id = id });
    /// <summary>The driver's inbox receipt for one channel message.</summary>
    internal async Task<string> ReceiptOf(string messageId) => (await Broker.GetInboxAsync(Driver.Id, limit: 500))!.Items
        .Single(i => i.Event.Type == Peers.Shared.Models.EventTypes.ChannelMessageAdded && i.Event.Payload.Contains(messageId, StringComparison.Ordinal)).InboxId;
    internal object Proposal(string before, string after, string cue = "When test counts matter") => new { cue, lesson = "Inspect counts", effect = "helped", confidence = "high", confidence_reason = "Fixture contrast",
        before = new[] { new { packet_id = before, path = "task.txt", quote = "Set value to two" } }, after = new[] { new { packet_id = after, path = "evidence/0000-test.log", quote = "# pass 1" } } };
    internal Task<byte[]> Git(params string[] args) => ReviewSnapshot.RunGitAsync(Root, args);
    public async ValueTask DisposeAsync()
    {
        foreach (var peer in _peers) await peer.DisposeAsync(); Broker.Dispose(); await _app.DisposeAsync(); Directory.Delete(Temporary, true);
    }
}
internal sealed class ReviewTestPeer : IAsyncDisposable
{
    internal string Id { get; private set; } = "";
    private McpClient _client = null!; private McpServer _server = null!; private Task _run = null!;
    private ServiceProvider _services = null!; private SessionState _state = null!;
    internal static async Task<ReviewTestPeer> Create(BrokerClient broker, string id, string root, string storage)
    {
        var peer = new ReviewTestPeer { Id = id, _state = new SessionState { PeerId = id, RepoRoot = root } };
        var services = new ServiceCollection(); services.AddLogging(); services.AddSingleton(broker); services.AddSingleton(peer._state); services.AddSingleton(new ReviewStorageOptions(storage));
        peer._services = services.BuildServiceProvider(); var options = ServerOptionsFactory.Create(peer._services);
        var input = new Pipe(); var output = new Pipe();
        var transport = new StreamServerTransport(input.Reader.AsStream(), output.Writer.AsStream(), "review-fixture", loggerFactory: null);
        peer._server = McpServer.Create(transport, options, loggerFactory: null, peer._services); peer._run = peer._server.RunAsync();
        peer._client = await McpClient.CreateAsync(new StreamClientTransport(input.Writer.AsStream(), output.Reader.AsStream(), loggerFactory: null));
        return peer;
    }
    private static IReadOnlyDictionary<string, object?> Args(object value) => JsonSerializer.Deserialize<Dictionary<string, object?>>(JsonSerializer.Serialize(value))!;
    internal async Task<JsonNode> Call(string name, object value)
    {
        var result = await _client.CallToolAsync(name, Args(value)); var text = string.Join("\n", result.Content.OfType<TextContentBlock>().Select(x => x.Text));
        Assert.False(result.IsError is true, text); return JsonNode.Parse(text)!;
    }
    internal async Task<string> Error(string name, object value)
    {
        var result = await _client.CallToolAsync(name, Args(value)); var text = string.Join("\n", result.Content.OfType<TextContentBlock>().Select(x => x.Text));
        Assert.True(result.IsError is true, text); return text;
    }
    private bool _disposed;
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return; _disposed = true;
        await _client.DisposeAsync(); await _state.DisposeAsync(); await _server.DisposeAsync(); await _run.WaitAsync(TimeSpan.FromSeconds(5)); await _services.DisposeAsync();
    }
}
internal sealed class ReviewFaultHandler(HttpMessageHandler inner) : DelegatingHandler(inner)
{
    internal bool LoseNextAck { get; set; }
    internal Action? ChangeOnBlob { get; set; }
    internal string? LoseNextSendKind { get; set; }
    // Fails the first channel read after the checkpoint that follows a managed send of this kind: the
    // read a completion makes once its control is sent and its ledger persisted.
    internal string? FailReadAfterPersistedSendKind { get; set; }
    private bool _sendSeen, _failNextRead;
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        if (_failNextRead && request.Method == HttpMethod.Get && request.RequestUri!.AbsolutePath.EndsWith("/messages", StringComparison.Ordinal))
        { _failNextRead = false; FailReadAfterPersistedSendKind = null; throw new HttpRequestException("Broker read failed after completion was persisted"); }
        if (ChangeOnBlob is not null && request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath == "/artifacts")
        {
            var json = await request.Content!.ReadFromJsonAsync<JsonElement>(ct);
            if (json.GetProperty("name").GetString()!.StartsWith("blob-", StringComparison.Ordinal)) { var action = ChangeOnBlob; ChangeOnBlob = null; action(); }
        }
        var response = await base.SendAsync(request, ct);
        if (LoseNextSendKind is not null && request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath.EndsWith("/messages", StringComparison.Ordinal))
        {
            var json = await request.Content!.ReadFromJsonAsync<JsonElement>(ct);
            var content = json.GetProperty("content").GetString()!;
            if (content.StartsWith("PEERS_REVIEW_V1\n", StringComparison.Ordinal) && content.Contains($"\"kind\":\"{LoseNextSendKind}\"", StringComparison.Ordinal))
            { LoseNextSendKind = null; response.Dispose(); throw new HttpRequestException("Managed send committed; response lost"); }
        }
        if (FailReadAfterPersistedSendKind is { } failKind && request.Method == HttpMethod.Post)
        {
            var path = request.RequestUri!.AbsolutePath;
            if (!_sendSeen && path.EndsWith("/messages", StringComparison.Ordinal))
            {
                var content = (await request.Content!.ReadFromJsonAsync<JsonElement>(ct)).GetProperty("content").GetString()!;
                _sendSeen = content.StartsWith("PEERS_REVIEW_V1\n", StringComparison.Ordinal) && content.Contains($"\"kind\":\"{failKind}\"", StringComparison.Ordinal);
            }
            else if (_sendSeen && path == "/artifacts" &&
                (await request.Content!.ReadFromJsonAsync<JsonElement>(ct)).GetProperty("name").GetString() == "checkpoint")
            { _sendSeen = false; _failNextRead = true; }
        }
        if (LoseNextAck && request.RequestUri!.AbsolutePath.EndsWith("/inbox/ack", StringComparison.Ordinal))
        { LoseNextAck = false; response.Dispose(); throw new HttpRequestException("Ack committed; response lost"); }
        return response;
    }
}
