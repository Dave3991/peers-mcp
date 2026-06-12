using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Tests.Helpers;
using Peers.Shared.Models;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

public sealed class DbTimeTests : IDisposable
{
    private readonly TestDb _db = new();

    // ──────────────────────────────────────────────
    // 1. DbTime.ParseUtc — space-separated format
    // ──────────────────────────────────────────────

    [Fact]
    public void ParseUtc_SpaceSeparatedFormat_ReturnsUtcKind()
    {
        var result = DbTime.ParseUtc("2026-06-12 09:02:29");
        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(new DateTime(2026, 6, 12, 9, 2, 29, DateTimeKind.Utc), result);
    }

    // ──────────────────────────────────────────────
    // 2. DbTime.ParseUtc — ISO 8601 round-trip format
    // ──────────────────────────────────────────────

    [Fact]
    public void ParseUtc_IsoRoundTripFormat_ReturnsUtcKind()
    {
        var original = new DateTime(2026, 6, 12, 9, 2, 29, DateTimeKind.Utc);
        var iso = original.ToString("O");
        var result = DbTime.ParseUtc(iso);
        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(original, result);
    }

    // ──────────────────────────────────────────────
    // 3. PeerRepository round-trip — all timestamp Kind == Utc
    // ──────────────────────────────────────────────

    [Fact]
    public async Task PeerRepository_TimestampFields_HaveUtcKind()
    {
        var peers = new PeerRepository(_db.Factory);
        var peer = await peers.InsertAsync(new Peer
        {
            PeerId = "dbtime-peer-1",
            Tool = "claude-code",
            MachineId = "machine-test",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

        var loaded = await peers.GetAsync(peer.PeerId, CancellationToken.None);
        Assert.NotNull(loaded);
        Assert.Equal(DateTimeKind.Utc, loaded.LeaseExpiresAt.Kind);
        Assert.Equal(DateTimeKind.Utc, loaded.CreatedAt.Kind);
        Assert.Equal(DateTimeKind.Utc, loaded.UpdatedAt.Kind);
    }

    // ──────────────────────────────────────────────
    // 4. ArtifactRepository round-trip — CreatedAt Kind == Utc
    // ──────────────────────────────────────────────

    [Fact]
    public async Task ArtifactRepository_CreatedAt_HasUtcKind()
    {
        var artifacts = new ArtifactRepository(_db.Factory);

        // We need a peer to satisfy published_by_peer_id FK (or just use a string; no FK enforcement in SQLite)
        var (artifact, _, _) = await artifacts.PublishAsync(new Artifact
        {
            ArtifactId = Guid.NewGuid().ToString("N"),
            Namespace = "test-ns",
            Name = "test-artifact",
            Version = 1,
            Type = "spec",
            Content = "hello world",
            ContentHash = "",
            ContentBytes = 0,
            PublishedByPeerId = "peer-test",
            CreatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

        var loaded = await artifacts.GetByIdAsync(artifact.ArtifactId, CancellationToken.None);
        Assert.NotNull(loaded);
        Assert.Equal(DateTimeKind.Utc, loaded.CreatedAt.Kind);

        // Also test ListAsync path
        var list = await artifacts.ListAsync(ns: "test-ns", ct: CancellationToken.None);
        Assert.NotEmpty(list);
        foreach (var meta in list)
            Assert.Equal(DateTimeKind.Utc, meta.CreatedAt.Kind);
    }

    // ──────────────────────────────────────────────
    // 5. ScratchpadRepository round-trip — CreatedAt Kind == Utc
    // ──────────────────────────────────────────────

    [Fact]
    public async Task ScratchpadRepository_CreatedAt_HasUtcKind()
    {
        var pads = new ScratchpadRepository(_db.Factory);

        var pad = await pads.InsertAsync(new Scratchpad
        {
            ScratchpadId = Guid.NewGuid().ToString("N"),
            Type = "review",
            Title = "Test pad",
            Status = "open",
            CreatedByPeerId = "peer-test",
            CreatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

        // Append an entry so GetEntriesAsync has something to return
        var entry = await pads.AppendEntryAsync(new ScratchpadEntry
        {
            EntryId = Guid.NewGuid().ToString("N"),
            ScratchpadId = pad.ScratchpadId,
            AuthorPeerId = "peer-test",
            Content = "test content",
            ContentBytes = 12,
            CreatedAt = DateTime.UtcNow,
        }, CancellationToken.None);
        Assert.NotNull(entry);

        var loaded = await pads.GetAsync(pad.ScratchpadId, CancellationToken.None);
        Assert.NotNull(loaded);
        Assert.Equal(DateTimeKind.Utc, loaded.CreatedAt.Kind);

        var entries = await pads.GetEntriesAsync(pad.ScratchpadId, ct: CancellationToken.None);
        Assert.NotEmpty(entries);
        foreach (var e in entries)
            Assert.Equal(DateTimeKind.Utc, e.CreatedAt.Kind);
    }

    // ──────────────────────────────────────────────
    // 6. EventRepository + InboxRepository — all timestamp fields Kind == Utc
    // ──────────────────────────────────────────────

    [Fact]
    public async Task EventAndInboxRepository_TimestampFields_HaveUtcKind()
    {
        var events = new EventRepository(_db.Factory);
        var inbox = new InboxRepository(_db.Factory);
        var peers = new PeerRepository(_db.Factory);

        // Need a peer for the inbox
        var peer = await peers.InsertAsync(new Peer
        {
            PeerId = "dbtime-inbox-peer",
            Tool = "claude-code",
            MachineId = "machine-inbox",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

        var eventId = await events.InsertAsync("test.event", "{}", ct: CancellationToken.None);
        await inbox.InsertAsync(peer.PeerId, eventId, ct: CancellationToken.None);

        // EventRepository.GetSinceAsync
        var eventList = await events.GetSinceAsync(eventId - 1, 10, CancellationToken.None);
        var ev = Assert.Single(eventList, e => e.EventId == eventId);
        Assert.Equal(DateTimeKind.Utc, ev.CreatedAt.Kind);

        // InboxRepository.GetUnackedAsync
        var inboxItems = await inbox.GetUnackedAsync(peer.PeerId, 10, CancellationToken.None);
        var item = Assert.Single(inboxItems, i => i.Event.EventId == eventId);
        Assert.Equal(DateTimeKind.Utc, item.CreatedAt.Kind);
        Assert.Equal(DateTimeKind.Utc, item.Event.CreatedAt.Kind);
    }

    // ──────────────────────────────────────────────
    // 7. JSON serialization — lease timestamp ends in "Z", not "+00:00"
    // ──────────────────────────────────────────────

    [Fact]
    public async Task JsonSerializer_LeaseExpiresAt_EndsWithZ()
    {
        var peers = new PeerRepository(_db.Factory);
        var peer = await peers.InsertAsync(new Peer
        {
            PeerId = "dbtime-json-peer",
            Tool = "claude-code",
            MachineId = "machine-json",
            LeaseExpiresAt = DateTime.UtcNow.AddMinutes(5),
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
        }, CancellationToken.None);

        var loaded = await peers.GetAsync(peer.PeerId, CancellationToken.None);
        Assert.NotNull(loaded);

        var json = JsonSerializer.Serialize(loaded);
        Assert.Contains("Z", json);
        Assert.DoesNotContain("+00:00", json);
    }

    // ──────────────────────────────────────────────
    // 8. Back-compat: rows written with SQLite datetime('now') (space format)
    //    are read back correctly by the repo
    // ──────────────────────────────────────────────

    [Fact]
    public async Task PeerRepository_SpaceFormatTimestamps_ReadCorrectly()
    {
        // Insert a peer using raw SQL with datetime('now') so the column stores
        // the "YYYY-MM-DD HH:MM:SS" space-separated format
        await _db.Factory.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO peers (peer_id, tool, machine_id,
                    lease_expires_at, created_at, updated_at)
                VALUES ('space-fmt-peer', 'claude-code', 'machine-space',
                    datetime('now', '+5 minutes'), datetime('now'), datetime('now'))
                """;
            await cmd.ExecuteNonQueryAsync();
        }, CancellationToken.None);

        var peers = new PeerRepository(_db.Factory);
        var loaded = await peers.GetAsync("space-fmt-peer", CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal(DateTimeKind.Utc, loaded.LeaseExpiresAt.Kind);
        Assert.Equal(DateTimeKind.Utc, loaded.CreatedAt.Kind);
        Assert.Equal(DateTimeKind.Utc, loaded.UpdatedAt.Kind);
    }

    public void Dispose() => _db.Dispose();
}
