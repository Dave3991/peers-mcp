using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence;
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

    public async Task ReactivateAsync(Peer peer, CancellationToken ct = default)
    {
        await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                UPDATE peers SET
                    tool = @tool,
                    machine_id = @machine,
                    repo_root = @repo,
                    worktree_root = @worktree,
                    branch = @branch,
                    commit_sha = @commit,
                    role = @role,
                    summary = @summary,
                    focus_paths = @focus,
                    capabilities = @caps,
                    parent_peer_id = @parent,
                    lease_expires_at = @lease,
                    expired_at = NULL,
                    updated_at = datetime('now')
                WHERE peer_id = @id
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
        }, ct);
    }

    public async Task<int> RenewLeaseAsync(string peerId, DateTime newExpiry, string? summary, string? role,
        string? branch, string? commitSha, string? focusPaths, string? capabilities, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
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
                    capabilities = COALESCE(@caps, capabilities),
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
            cmd.Parameters.AddWithValue("@caps", (object?)capabilities ?? DBNull.Value);
            return await cmd.ExecuteNonQueryAsync(ct);
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
            {
                where.Add("expired_at IS NULL");
                where.Add("datetime(lease_expires_at) >= datetime('now')");
            }
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

    /// <summary>
    /// Resolve a peer by exact ID or unique prefix. Returns null if no match or ambiguous.
    /// </summary>
    public async Task<Peer?> GetByPrefixAsync(string prefixOrId, CancellationToken ct = default)
    {
        // Try exact match first (fast path)
        var exact = await GetAsync(prefixOrId, ct);
        if (exact is not null)
            return exact;

        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM peers WHERE peer_id LIKE @prefix AND expired_at IS NULL";
            cmd.Parameters.AddWithValue("@prefix", prefixOrId + "%");
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            if (!await reader.ReadAsync(ct))
                return null;

            var first = ReadPeer(reader);

            // Ambiguous if more than one match
            if (await reader.ReadAsync(ct))
                return null;

            return first;
        }, ct);
    }

    /// <summary>
    /// Find a live peer that represents the same logical session as an expired peer.
    /// Peer IDs are minted fresh per session, so when a reviewer's session restarts it
    /// re-registers under a new ID while a scratchpad's requested_reviewers still points
    /// at the old, now-expired ID. Matching on the stable identity (machine + tool + repo
    /// + worktree + branch) lets us re-route notifications to the reviewer's current
    /// session. Returns the most recently updated match, or null if none is live.
    /// </summary>
    public async Task<Peer?> FindActiveReplacementAsync(Peer expired, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            // `IS` is SQLite's null-safe comparison, so NULL repo/worktree/branch match correctly.
            cmd.CommandText = """
                SELECT * FROM peers
                WHERE peer_id != @id
                  AND machine_id = @machine
                  AND tool = @tool
                  AND repo_root IS @repo
                  AND worktree_root IS @worktree
                  AND branch IS @branch
                  AND expired_at IS NULL
                  AND datetime(lease_expires_at) >= datetime('now')
                ORDER BY datetime(updated_at) DESC
                LIMIT 1
                """;
            cmd.Parameters.AddWithValue("@id", expired.PeerId);
            cmd.Parameters.AddWithValue("@machine", expired.MachineId);
            cmd.Parameters.AddWithValue("@tool", expired.Tool);
            cmd.Parameters.AddWithValue("@repo", (object?)expired.RepoRoot ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@worktree", (object?)expired.WorktreeRoot ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@branch", (object?)expired.Branch ?? DBNull.Value);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            return await reader.ReadAsync(ct) ? ReadPeer(reader) : null;
        }, ct);
    }

    public async Task ExpireByIdAsync(string peerId, CancellationToken ct = default)
    {
        await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                UPDATE peers SET expired_at = datetime('now'), updated_at = datetime('now')
                WHERE peer_id = @id AND expired_at IS NULL
                """;
            cmd.Parameters.AddWithValue("@id", peerId);
            await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    public async Task<int> ExpireStaleAsync(CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                UPDATE peers SET expired_at = datetime('now'), updated_at = datetime('now')
                WHERE expired_at IS NULL AND datetime(lease_expires_at) < datetime('now')
                """;
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    public async Task<int> PurgeExpiredBeforeAsync(DateTime cutoff, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var txn = conn.BeginTransaction();

            using var inboxCmd = conn.CreateCommand();
            inboxCmd.CommandText = """
                DELETE FROM peer_inbox WHERE peer_id IN
                  (SELECT peer_id FROM peers WHERE expired_at IS NOT NULL AND datetime(expired_at) < datetime(@cutoff))
                """;
            inboxCmd.Parameters.AddWithValue("@cutoff", cutoff.ToString("O"));
            await inboxCmd.ExecuteNonQueryAsync(ct);

            using var peerCmd = conn.CreateCommand();
            peerCmd.CommandText = """
                DELETE FROM peers WHERE expired_at IS NOT NULL AND datetime(expired_at) < datetime(@cutoff)
                """;
            peerCmd.Parameters.AddWithValue("@cutoff", cutoff.ToString("O"));
            var deleted = await peerCmd.ExecuteNonQueryAsync(ct);

            txn.Commit();
            return deleted;
        }, ct);
    }

    public async Task<long?> GetMinActiveCursorAsync(CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT MIN(COALESCE(last_polled_event_id, 0)) FROM peers WHERE expired_at IS NULL";
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
        LeaseExpiresAt = r.GetUtc("lease_expires_at"),
        ExpiredAt = r.GetUtcOrNull("expired_at"),
        CreatedAt = r.GetUtc("created_at"),
        UpdatedAt = r.GetUtc("updated_at"),
    };
}
