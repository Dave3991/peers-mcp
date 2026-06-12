using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Services;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

public sealed class RetentionTests : IDisposable
{
    private readonly TestDb _db = new();

    private (PeerRepository peers, InboxRepository inbox, EventRepository events, PeerService peerSvc, EventService eventSvc) CreateRepos()
    {
        var peers = new PeerRepository(_db.Factory);
        var inbox = new InboxRepository(_db.Factory);
        var events = new EventRepository(_db.Factory);
        var eventSvc = new EventService(events, peers, inbox);
        var peerSvc = new PeerService(peers, eventSvc);
        return (peers, inbox, events, peerSvc, eventSvc);
    }

    /// <summary>
    /// Helper: set expired_at on a peer to a specific value via raw SQL.
    /// </summary>
    private Task SetExpiredAtAsync(string peerId, DateTime expiredAt)
        => _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE peers SET expired_at = @v WHERE peer_id = @id";
            cmd.Parameters.AddWithValue("@v", expiredAt.ToString("O"));
            cmd.Parameters.AddWithValue("@id", peerId);
            await cmd.ExecuteNonQueryAsync();
        }, CancellationToken.None);

    /// <summary>
    /// Helper: count peer_inbox rows for a peer via raw SQL.
    /// </summary>
    private Task<long> CountInboxRowsAsync(string peerId)
        => _db.Factory.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM peer_inbox WHERE peer_id = @id";
            cmd.Parameters.AddWithValue("@id", peerId);
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt64(result!);
        }, CancellationToken.None);

    /// <summary>
    /// Helper: count unacked peer_inbox rows for a peer via raw SQL.
    /// </summary>
    private Task<long> CountUnackedInboxRowsAsync(string peerId)
        => _db.Factory.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM peer_inbox WHERE peer_id = @id AND acked_at IS NULL";
            cmd.Parameters.AddWithValue("@id", peerId);
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt64(result!);
        }, CancellationToken.None);

    // (a) A peer with expired_at older than cutoff is deleted, AND its peer_inbox rows are gone.
    [Fact]
    public async Task PurgeExpiredBeforeAsync_DeletesPeerAndInboxRows_WhenExpiredBeforeCutoff()
    {
        var (peers, inbox, events, _, _) = CreateRepos();

        // Register and expire a peer
        var peerId = (await peers.InsertAsync(new Peers.Shared.Models.Peer
        {
            PeerId = "test-peer-a",
            Tool = "claude-code",
            MachineId = "machine-a",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None)).PeerId;

        // Set expired_at to 2 days ago
        var oldExpiry = DateTime.UtcNow.AddDays(-2);
        await SetExpiredAtAsync(peerId, oldExpiry);

        // Insert an inbox row for this peer
        var eventId = await events.InsertAsync("test", "{}", ct: CancellationToken.None);
        await inbox.InsertAsync(peerId, eventId, ct: CancellationToken.None);

        // Verify inbox row exists
        Assert.Equal(1, await CountInboxRowsAsync(peerId));

        // Purge with cutoff = 1 day ago (peer expired 2 days ago — before the cutoff)
        var cutoff = DateTime.UtcNow.AddDays(-1);
        var deleted = await peers.PurgeExpiredBeforeAsync(cutoff, CancellationToken.None);

        Assert.Equal(1, deleted);

        // Peer should be gone
        var peer = await peers.GetAsync(peerId, CancellationToken.None);
        Assert.Null(peer);

        // Inbox rows should be gone
        Assert.Equal(0, await CountInboxRowsAsync(peerId));
    }

    // (b) A peer whose expired_at is NEWER than the cutoff survives the purge.
    [Fact]
    public async Task PurgeExpiredBeforeAsync_SurvivedByPeer_WhenExpiredAfterCutoff()
    {
        var (peers, _, _, _, _) = CreateRepos();

        var peerId = (await peers.InsertAsync(new Peers.Shared.Models.Peer
        {
            PeerId = "test-peer-b",
            Tool = "claude-code",
            MachineId = "machine-b",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None)).PeerId;

        // Set expired_at to 30 minutes ago (recent)
        await SetExpiredAtAsync(peerId, DateTime.UtcNow.AddMinutes(-30));

        // Purge with cutoff = 1 day ago (peer expired only 30 min ago — after the cutoff)
        var cutoff = DateTime.UtcNow.AddDays(-1);
        var deleted = await peers.PurgeExpiredBeforeAsync(cutoff, CancellationToken.None);

        Assert.Equal(0, deleted);

        // Peer should still exist
        var peer = await peers.GetAsync(peerId, CancellationToken.None);
        Assert.NotNull(peer);
    }

    // (c) An event pinned by an unacked inbox row of a purged peer becomes prunable after the purge.
    [Fact]
    public async Task PurgeExpiredBeforeAsync_UnpinsEvent_AllowingSubsequentPrune()
    {
        var (peers, inbox, events, _, _) = CreateRepos();

        var peerId = (await peers.InsertAsync(new Peers.Shared.Models.Peer
        {
            PeerId = "test-peer-c",
            Tool = "claude-code",
            MachineId = "machine-c",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None)).PeerId;

        await SetExpiredAtAsync(peerId, DateTime.UtcNow.AddDays(-2));

        // Insert an event and an unacked inbox row pinning it
        var eventId = await events.InsertAsync("pinned-event", "{}", ct: CancellationToken.None);
        await inbox.InsertAsync(peerId, eventId, ct: CancellationToken.None);

        // PruneBeforeAsync with eventId+1 should NOT delete the event while unacked row exists
        var prunedBefore = await events.PruneBeforeAsync(eventId + 1, CancellationToken.None);
        Assert.Equal(0, prunedBefore);

        // Verify event still exists
        var eventsAfterFirstPrune = await events.GetSinceAsync(eventId - 1, 10, CancellationToken.None);
        Assert.Contains(eventsAfterFirstPrune, e => e.EventId == eventId);

        // Purge the peer (removes the inbox row that pins the event)
        var cutoff = DateTime.UtcNow.AddDays(-1);
        var deleted = await peers.PurgeExpiredBeforeAsync(cutoff, CancellationToken.None);
        Assert.Equal(1, deleted);

        // Now PruneBeforeAsync should be able to delete the event
        var prunedAfter = await events.PruneBeforeAsync(eventId + 1, CancellationToken.None);
        Assert.Equal(1, prunedAfter);

        // Verify event is gone
        var eventsAfterSecondPrune = await events.GetSinceAsync(eventId - 1, 10, CancellationToken.None);
        Assert.DoesNotContain(eventsAfterSecondPrune, e => e.EventId == eventId);
    }

    // (d) EnforceUnackedCapAsync acks the oldest unacked rows beyond the cap for one peer.
    [Fact]
    public async Task EnforceUnackedCapAsync_AcksOldestBeyondCap_KeepsNewestRows()
    {
        var (peers, inbox, events, _, _) = CreateRepos();

        var peerId = (await peers.InsertAsync(new Peers.Shared.Models.Peer
        {
            PeerId = "test-peer-d",
            Tool = "claude-code",
            MachineId = "machine-d",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None)).PeerId;

        const int cap = 3;
        const int overflow = 2;
        const int total = cap + overflow;

        // Insert total=5 events + inbox rows
        var eventIds = new List<long>();
        for (var i = 0; i < total; i++)
        {
            var eventId = await events.InsertAsync($"event-{i}", "{}", ct: CancellationToken.None);
            await inbox.InsertAsync(peerId, eventId, ct: CancellationToken.None);
            eventIds.Add(eventId);
        }

        // Verify all 5 are unacked
        Assert.Equal(total, await CountUnackedInboxRowsAsync(peerId));

        // Enforce cap of 3 — should ack the 2 oldest (lowest event_ids)
        var capped = await inbox.EnforceUnackedCapAsync(cap, CancellationToken.None);
        Assert.Equal(overflow, capped);

        // Exactly cap (3) rows remain unacked
        var remaining = await inbox.GetUnackedAsync(peerId, 100, CancellationToken.None);
        Assert.Equal(cap, remaining.Count);

        // The remaining unacked rows are the newest (highest event_ids)
        var newestIds = eventIds.OrderByDescending(id => id).Take(cap).ToHashSet();
        foreach (var item in remaining)
            Assert.Contains(item.Event.EventId, newestIds);

        // The oldest (lowest event_ids) are now acked
        var oldestIds = eventIds.OrderBy(id => id).Take(overflow).ToHashSet();
        Assert.DoesNotContain(remaining, item => oldestIds.Contains(item.Event.EventId));
    }

    /// <summary>
    /// Helper: insert a peer row with an explicit expired_at directly via raw SQL,
    /// bypassing RegisterAsync so we can create two distinct expired peers with the same identity.
    /// </summary>
    private Task InsertExpiredPeerAsync(string peerId, string tool, string machineId, string repoRoot,
        string branch, DateTime expiredAt)
        => _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO peers (peer_id, tool, machine_id, repo_root, branch,
                    lease_expires_at, expired_at, created_at, updated_at)
                VALUES (@id, @tool, @machine, @repo, @branch,
                    datetime('now', '-1 minute'), @expired, datetime('now'), datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", peerId);
            cmd.Parameters.AddWithValue("@tool", tool);
            cmd.Parameters.AddWithValue("@machine", machineId);
            cmd.Parameters.AddWithValue("@repo", repoRoot);
            cmd.Parameters.AddWithValue("@branch", branch);
            cmd.Parameters.AddWithValue("@expired", expiredAt.ToString("O"));
            await cmd.ExecuteNonQueryAsync();
        }, CancellationToken.None);

    // (e) Resume-after-purge: two expired peers with same identity — only one resumable after purging the older.
    [Fact]
    public async Task RegisterAsync_ResumesCorrectly_AfterOldDuplicateIsPurged()
    {
        var (peers, _, _, peerSvc, _) = CreateRepos();

        const string tool = "claude-code";
        const string machineId = "machine-e";
        const string repoRoot = "/repo/e";
        const string branch = "main";

        var identity = new RegisterPeerRequest
        {
            Tool = tool,
            MachineId = machineId,
            RepoRoot = repoRoot,
            Branch = branch,
        };

        // Insert two distinct expired peers with the same identity directly (bypassing RegisterAsync
        // to avoid implicit resume collapsing them to a single peer_id).
        var oldPeerId = "retain-old-peer";
        var recentPeerId = "retain-recent-peer";

        // oldPeer: expired 10 days ago
        await InsertExpiredPeerAsync(oldPeerId, tool, machineId, repoRoot, branch, DateTime.UtcNow.AddDays(-10));
        // recentPeer: expired 1 hour ago
        await InsertExpiredPeerAsync(recentPeerId, tool, machineId, repoRoot, branch, DateTime.UtcNow.AddHours(-1));

        // Two expired matches — implicit register should NOT resume (count != 1).
        var noResume = await peerSvc.RegisterAsync(identity, CancellationToken.None);
        Assert.False(noResume.Resumed);

        // Clean up the live registration so it doesn't block the resume attempt below.
        await peerSvc.DeregisterAsync(noResume.PeerId, CancellationToken.None);
        // Set it expired with a very old date so it doesn't become a new expired candidate
        await SetExpiredAtAsync(noResume.PeerId, DateTime.UtcNow.AddDays(-30));

        // Purge with cutoff = 5 days ago — removes oldPeer (10d) and noResume (30d), leaves recentPeer (1h)
        var cutoff = DateTime.UtcNow.AddDays(-5);
        var purgedCount = await peers.PurgeExpiredBeforeAsync(cutoff, CancellationToken.None);
        Assert.True(purgedCount >= 2); // oldPeer + noResume purged

        // Now exactly one expired match remains (recentPeer) → implicit register should resume it.
        var resumed = await peerSvc.RegisterAsync(identity, CancellationToken.None);
        Assert.True(resumed.Resumed);
        Assert.Equal(recentPeerId, resumed.PeerId);
    }

    /// <summary>
    /// Helper: overwrite acked_at on a peer_inbox row identified by (peerId, eventId) via raw SQL.
    /// </summary>
    private Task SetAckedAtAsync(string peerId, long eventId, DateTime ackedAt)
        => _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE peer_inbox SET acked_at = @v WHERE peer_id = @peer AND event_id = @event";
            cmd.Parameters.AddWithValue("@v", ackedAt.ToString("O"));
            cmd.Parameters.AddWithValue("@peer", peerId);
            cmd.Parameters.AddWithValue("@event", eventId);
            await cmd.ExecuteNonQueryAsync();
        }, CancellationToken.None);

    // (f) PurgeAckedBeforeAsync deletes only rows whose acked_at is older than the age window,
    //     leaving recently-acked rows and unacked rows untouched.
    [Fact]
    public async Task PurgeAckedBeforeAsync_DeletesOldAckedRows_KeepsRecentAckedAndUnacked()
    {
        var (peers, inbox, events, _, _) = CreateRepos();

        var peerId = (await peers.InsertAsync(new Peers.Shared.Models.Peer
        {
            PeerId = "test-peer-f",
            Tool = "claude-code",
            MachineId = "machine-f",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None)).PeerId;

        // Insert 3 events and 3 inbox rows
        var eventId1 = await events.InsertAsync("event-f1", "{}", ct: CancellationToken.None);
        var eventId2 = await events.InsertAsync("event-f2", "{}", ct: CancellationToken.None);
        var eventId3 = await events.InsertAsync("event-f3", "{}", ct: CancellationToken.None);

        await inbox.InsertAsync(peerId, eventId1, ct: CancellationToken.None);
        await inbox.InsertAsync(peerId, eventId2, ct: CancellationToken.None);
        await inbox.InsertAsync(peerId, eventId3, ct: CancellationToken.None);

        // Row 1: ack via repo, then backdate acked_at to 2 days ago (well outside the 24h window)
        var unacked = await inbox.GetUnackedAsync(peerId, 100, CancellationToken.None);
        var inboxId1 = unacked.First(i => i.Event.EventId == eventId1).InboxId;
        await inbox.AckAsync(peerId, [inboxId1], CancellationToken.None);
        await SetAckedAtAsync(peerId, eventId1, DateTime.UtcNow.AddDays(-2));

        // Row 2: ack via repo — acked_at stays as now (within the 24h window)
        var inboxId2 = unacked.First(i => i.Event.EventId == eventId2).InboxId;
        await inbox.AckAsync(peerId, [inboxId2], CancellationToken.None);

        // Row 3: left unacked

        // Verify pre-conditions: 3 total rows, 1 unacked
        Assert.Equal(3, await CountInboxRowsAsync(peerId));
        Assert.Equal(1, await CountUnackedInboxRowsAsync(peerId));

        // Purge rows acked more than 24 hours ago — only row 1 qualifies
        var deleted = await inbox.PurgeAckedBeforeAsync(TimeSpan.FromHours(24), CancellationToken.None);

        // Only the old acked row should have been deleted
        Assert.Equal(1, deleted);

        // 2 rows remain: the recently-acked row and the unacked row
        Assert.Equal(2, await CountInboxRowsAsync(peerId));

        // Exactly 1 unacked row remains (row 3)
        Assert.Equal(1, await CountUnackedInboxRowsAsync(peerId));
    }

    public void Dispose() => _db.Dispose();
}
