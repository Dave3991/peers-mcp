using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence;
using Peers.Shared.Models;

namespace Peers.Broker.Persistence.Repositories;

public sealed class ScratchpadRepository(SqliteConnectionFactory db)
{
    public async Task<Scratchpad> InsertAsync(Scratchpad pad, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO scratchpads (scratchpad_id, type, title, repo_root, status, requested_reviewers,
                    artifact_refs, created_by_peer_id, created_at)
                VALUES (@id, @type, @title, @repo, 'open', @reviewers, @artifacts, @peer, datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", pad.ScratchpadId);
            cmd.Parameters.AddWithValue("@type", pad.Type);
            cmd.Parameters.AddWithValue("@title", pad.Title);
            cmd.Parameters.AddWithValue("@repo", (object?)pad.RepoRoot ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@reviewers", (object?)pad.RequestedReviewers ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@artifacts", (object?)pad.ArtifactRefs ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@peer", pad.CreatedByPeerId);
            await cmd.ExecuteNonQueryAsync(ct);
            return pad;
        }, ct);
    }

    public async Task<ScratchpadEntry?> AppendEntryAsync(ScratchpadEntry entry, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            if (entry.IdempotencyKey is not null)
            {
                using var check = conn.CreateCommand();
                check.CommandText = "SELECT entry_id FROM scratchpad_entries WHERE scratchpad_id = @pad AND idempotency_key = @key";
                check.Parameters.AddWithValue("@pad", entry.ScratchpadId);
                check.Parameters.AddWithValue("@key", entry.IdempotencyKey);
                var existing = await check.ExecuteScalarAsync(ct);
                if (existing is not null) return null; // idempotent skip
            }

            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO scratchpad_entries (entry_id, scratchpad_id, author_peer_id, content, idempotency_key, content_bytes, created_at)
                VALUES (@id, @pad, @peer, @content, @key, @bytes, datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", entry.EntryId);
            cmd.Parameters.AddWithValue("@pad", entry.ScratchpadId);
            cmd.Parameters.AddWithValue("@peer", entry.AuthorPeerId);
            cmd.Parameters.AddWithValue("@content", entry.Content);
            cmd.Parameters.AddWithValue("@key", (object?)entry.IdempotencyKey ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@bytes", entry.ContentBytes);
            await cmd.ExecuteNonQueryAsync(ct);
            return entry;
        }, ct);
    }

    public async Task<Scratchpad?> GetAsync(string scratchpadId, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM scratchpads WHERE scratchpad_id = @id";
            cmd.Parameters.AddWithValue("@id", scratchpadId);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            return await r.ReadAsync(ct) ? ReadScratchpad(r) : null;
        }, ct);
    }

    public async Task<List<ScratchpadEntry>> GetEntriesAsync(string scratchpadId, long? sinceEventId = null,
        int limit = 200, int offset = 0, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT * FROM scratchpad_entries
                WHERE scratchpad_id = @pad
                ORDER BY created_at
                LIMIT @limit OFFSET @offset
                """;
            cmd.Parameters.AddWithValue("@pad", scratchpadId);
            cmd.Parameters.AddWithValue("@limit", limit);
            cmd.Parameters.AddWithValue("@offset", offset);

            var entries = new List<ScratchpadEntry>();
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                entries.Add(new ScratchpadEntry
                {
                    EntryId = r.GetString(r.GetOrdinal("entry_id")),
                    ScratchpadId = r.GetString(r.GetOrdinal("scratchpad_id")),
                    AuthorPeerId = r.GetString(r.GetOrdinal("author_peer_id")),
                    Content = r.GetString(r.GetOrdinal("content")),
                    IdempotencyKey = r.IsDBNull(r.GetOrdinal("idempotency_key")) ? null : r.GetString(r.GetOrdinal("idempotency_key")),
                    ContentBytes = r.GetInt32(r.GetOrdinal("content_bytes")),
                    CreatedAt = r.GetUtc("created_at"),
                });
            }
            return entries;
        }, ct);
    }

    public async Task<List<Scratchpad>> ListAsync(string? type = null, string? repoRoot = null,
        string? status = null, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            var where = new List<string>();
            if (type is not null) { where.Add("type = @type"); cmd.Parameters.AddWithValue("@type", type); }
            if (repoRoot is not null) { where.Add("repo_root = @repo"); cmd.Parameters.AddWithValue("@repo", repoRoot); }
            if (status is not null) { where.Add("status = @status"); cmd.Parameters.AddWithValue("@status", status); }

            var whereClause = where.Count > 0 ? "WHERE " + string.Join(" AND ", where) : "";
            cmd.CommandText = $"SELECT * FROM scratchpads {whereClause} ORDER BY created_at DESC";

            var pads = new List<Scratchpad>();
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct)) pads.Add(ReadScratchpad(r));
            return pads;
        }, ct);
    }

    public async Task ResolveAsync(string scratchpadId, string? resolution, CancellationToken ct = default)
    {
        await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                UPDATE scratchpads SET status = 'resolved', resolution = @res
                WHERE scratchpad_id = @id
                """;
            cmd.Parameters.AddWithValue("@id", scratchpadId);
            cmd.Parameters.AddWithValue("@res", (object?)resolution ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    public async Task CloseAsync(string scratchpadId, string? resolution, CancellationToken ct = default)
    {
        await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                UPDATE scratchpads SET status = 'closed', closed_at = datetime('now'), resolution = @res
                WHERE scratchpad_id = @id
                """;
            cmd.Parameters.AddWithValue("@id", scratchpadId);
            cmd.Parameters.AddWithValue("@res", (object?)resolution ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    private static Scratchpad ReadScratchpad(SqliteDataReader r) => new()
    {
        ScratchpadId = r.GetString(r.GetOrdinal("scratchpad_id")),
        Type = r.GetString(r.GetOrdinal("type")),
        Title = r.GetString(r.GetOrdinal("title")),
        RepoRoot = r.IsDBNull(r.GetOrdinal("repo_root")) ? null : r.GetString(r.GetOrdinal("repo_root")),
        Status = r.GetString(r.GetOrdinal("status")),
        RequestedReviewers = r.IsDBNull(r.GetOrdinal("requested_reviewers")) ? null : r.GetString(r.GetOrdinal("requested_reviewers")),
        ArtifactRefs = r.IsDBNull(r.GetOrdinal("artifact_refs")) ? null : r.GetString(r.GetOrdinal("artifact_refs")),
        CreatedByPeerId = r.GetString(r.GetOrdinal("created_by_peer_id")),
        CreatedAt = r.GetUtc("created_at"),
        ClosedAt = r.GetUtcOrNull("closed_at"),
        Resolution = r.IsDBNull(r.GetOrdinal("resolution")) ? null : r.GetString(r.GetOrdinal("resolution")),
    };
}
