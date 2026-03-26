using Microsoft.Data.Sqlite;
using Peers.Shared.Models;

namespace Peers.Broker.Persistence.Repositories;

public sealed class PeerRepository(SqliteConnectionFactory db)
{
    public async Task<Peer> InsertAsync(Peer peer, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO peers (peer_id, tool, machine_id, repo_root, worktree_root, branch, commit_sha,
                    role, summary, focus_paths, capabilities, parent_peer_id, lease_expires_at, created_at, updated_at)
                VALUES (@id, @tool, @machine, @repo, @worktree, @branch, @commit,
                    @role, @summary, @focus, @caps, @parent, @lease, datetime('now'), datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", peer.PeerId);
            cmd.Parameters.AddWithValue("@tool", peer.Tool);
            cmd.Parameters.AddWithValue("@machine", peer.MachineId);
            cmd.Parameters.AddWithValue("@repo", (object?)peer.RepoRoot ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@worktree", (object?)peer.WorktreeRoot ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@branch", (object?)peer.Branch ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@commit", (object?)peer.CommitSha ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@role", (object?)peer.Role ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@summary", (object?)peer.Summary ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@focus", (object?)peer.FocusPaths ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@caps", (object?)peer.Capabilities ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@parent", (object?)peer.ParentPeerId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@lease", peer.LeaseExpiresAt.ToString("O"));
            await cmd.ExecuteNonQueryAsync(ct);
            return peer;
        }, ct);
    }

    public async Task RenewLeaseAsync(string peerId, DateTime newExpiry, string? summary, string? role,
        string? branch, string? commitSha, string? focusPaths, CancellationToken ct = default)
    {
        await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                UPDATE peers SET
                    lease_expires_at = @lease,
                    expired_at = NULL,
                    summary = COALESCE(@summary, summary),
                    role = COALESCE(@role, role),
                    branch = COALESCE(@branch, branch),
                    commit_sha = COALESCE(@commit, commit_sha),
                    focus_paths = COALESCE(@focus, focus_paths),
                    updated_at = datetime('now')
                WHERE peer_id = @id
                """;
            cmd.Parameters.AddWithValue("@id", peerId);
            cmd.Parameters.AddWithValue("@lease", newExpiry.ToString("O"));
            cmd.Parameters.AddWithValue("@summary", (object?)summary ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@role", (object?)role ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@branch", (object?)branch ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@commit", (object?)commitSha ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@focus", (object?)focusPaths ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    public async Task UpdateLastPolledEventIdAsync(string peerId, long eventId, CancellationToken ct = default)
    {
        await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE peers SET last_polled_event_id = @eid WHERE peer_id = @id";
            cmd.Parameters.AddWithValue("@id", peerId);
            cmd.Parameters.AddWithValue("@eid", eventId);
            await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    public async Task<List<Peer>> ListAsync(string? repoRoot = null, string? branch = null,
        string? tool = null, bool includeExpired = false, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            var where = new List<string>();
            if (!includeExpired)
                where.Add("expired_at IS NULL");
            if (repoRoot is not null)
            {
                where.Add("repo_root = @repo");
                cmd.Parameters.AddWithValue("@repo", repoRoot);
            }
            if (branch is not null)
            {
                where.Add("branch = @branch");
                cmd.Parameters.AddWithValue("@branch", branch);
            }
            if (tool is not null)
            {
                where.Add("tool = @tool");
                cmd.Parameters.AddWithValue("@tool", tool);
            }

            var whereClause = where.Count > 0 ? "WHERE " + string.Join(" AND ", where) : "";
            cmd.CommandText = $"SELECT * FROM peers {whereClause} ORDER BY created_at";

            var peers = new List<Peer>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                peers.Add(ReadPeer(reader));
            return peers;
        }, ct);
    }

    public async Task<Peer?> GetAsync(string peerId, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM peers WHERE peer_id = @id";
            cmd.Parameters.AddWithValue("@id", peerId);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            return await reader.ReadAsync(ct) ? ReadPeer(reader) : null;
        }, ct);
    }

    public async Task<int> ExpireStaleAsync(CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                UPDATE peers SET expired_at = datetime('now'), updated_at = datetime('now')
                WHERE expired_at IS NULL AND lease_expires_at < datetime('now')
                """;
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    public async Task<long?> GetMinActiveCursorAsync(CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT MIN(last_polled_event_id) FROM peers WHERE expired_at IS NULL AND last_polled_event_id IS NOT NULL";
            var result = await cmd.ExecuteScalarAsync(ct);
            return result is DBNull or null ? null : (long?)Convert.ToInt64(result);
        }, ct);
    }

    private static Peer ReadPeer(SqliteDataReader r) => new()
    {
        PeerId = r.GetString(r.GetOrdinal("peer_id")),
        Tool = r.GetString(r.GetOrdinal("tool")),
        MachineId = r.GetString(r.GetOrdinal("machine_id")),
        RepoRoot = r.IsDBNull(r.GetOrdinal("repo_root")) ? null : r.GetString(r.GetOrdinal("repo_root")),
        WorktreeRoot = r.IsDBNull(r.GetOrdinal("worktree_root")) ? null : r.GetString(r.GetOrdinal("worktree_root")),
        Branch = r.IsDBNull(r.GetOrdinal("branch")) ? null : r.GetString(r.GetOrdinal("branch")),
        CommitSha = r.IsDBNull(r.GetOrdinal("commit_sha")) ? null : r.GetString(r.GetOrdinal("commit_sha")),
        Role = r.IsDBNull(r.GetOrdinal("role")) ? null : r.GetString(r.GetOrdinal("role")),
        Summary = r.IsDBNull(r.GetOrdinal("summary")) ? null : r.GetString(r.GetOrdinal("summary")),
        FocusPaths = r.IsDBNull(r.GetOrdinal("focus_paths")) ? null : r.GetString(r.GetOrdinal("focus_paths")),
        Capabilities = r.IsDBNull(r.GetOrdinal("capabilities")) ? null : r.GetString(r.GetOrdinal("capabilities")),
        ParentPeerId = r.IsDBNull(r.GetOrdinal("parent_peer_id")) ? null : r.GetString(r.GetOrdinal("parent_peer_id")),
        LastPolledEventId = r.IsDBNull(r.GetOrdinal("last_polled_event_id")) ? null : r.GetInt64(r.GetOrdinal("last_polled_event_id")),
        LeaseExpiresAt = DateTime.Parse(r.GetString(r.GetOrdinal("lease_expires_at"))),
        ExpiredAt = r.IsDBNull(r.GetOrdinal("expired_at")) ? null : DateTime.Parse(r.GetString(r.GetOrdinal("expired_at"))),
        CreatedAt = DateTime.Parse(r.GetString(r.GetOrdinal("created_at"))),
        UpdatedAt = DateTime.Parse(r.GetString(r.GetOrdinal("updated_at"))),
    };
}
