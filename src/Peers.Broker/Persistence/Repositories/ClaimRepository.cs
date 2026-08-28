using Microsoft.Data.Sqlite;

namespace Peers.Broker.Persistence.Repositories;

/// <summary>
/// Path claims: which live peer is working on which repo-relative path, until when.
///
/// "Live" is the peer lease, joined here rather than tracked separately: a session that crashes never
/// releases anything, and a claim that outlives its holder would block the repo for its whole TTL. The
/// lease already expires in two minutes without a heartbeat, so joining it makes abandonment
/// self-healing and a release handshake optional rather than load-bearing.
/// </summary>
public sealed class ClaimRepository(SqliteConnectionFactory db)
{
    private const string ActiveJoin = """
        FROM claims c
        JOIN peers p ON p.peer_id = c.peer_id
        WHERE datetime(c.expires_at) > datetime('now')
          AND p.expired_at IS NULL
          AND datetime(p.lease_expires_at) >= datetime('now')
        """;

    /// <summary>Every claim currently held in <paramref name="repoRoot"/>, holder metadata included.</summary>
    public async Task<List<ClaimRow>> ListActiveAsync(string repoRoot, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                SELECT c.repo_root, c.path, c.peer_id, c.expires_at, p.role, p.summary
                {ActiveJoin}
                  AND c.repo_root = @repo
                ORDER BY c.path
                """;
            cmd.Parameters.AddWithValue("@repo", repoRoot);

            var rows = new List<ClaimRow>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(Read(reader));
            return rows;
        }, ct);
    }

    /// <summary>
    /// Claims or extends one path. Upsert on the owner index, so a peer re-claiming its own path renews
    /// the TTL instead of failing — the renewal path a long phase needs.
    /// </summary>
    public async Task<string> UpsertAsync(string peerId, string repoRoot, string path, DateTime expiresAt, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO claims (claim_id, peer_id, repo_root, path, expires_at, created_at)
                VALUES (@id, @peer, @repo, @path, @expires, datetime('now'))
                ON CONFLICT(repo_root, path, peer_id) DO UPDATE SET expires_at = excluded.expires_at
                RETURNING expires_at
                """;
            cmd.Parameters.AddWithValue("@id", Guid.NewGuid().ToString("N")[..16]);
            cmd.Parameters.AddWithValue("@peer", peerId);
            cmd.Parameters.AddWithValue("@repo", repoRoot);
            cmd.Parameters.AddWithValue("@path", path);
            cmd.Parameters.AddWithValue("@expires", expiresAt.ToString("O"));

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return reader.GetString(0);
        }, ct);
    }

    /// <summary>Releases the peer's claims: the named paths, or all of them when <paramref name="paths"/> is null.</summary>
    public async Task<int> DeleteAsync(string peerId, IReadOnlyList<string>? paths, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            if (paths is null)
            {
                cmd.CommandText = "DELETE FROM claims WHERE peer_id = @peer";
            }
            else
            {
                var names = paths.Select((_, index) => $"@p{index}").ToArray();
                cmd.CommandText = $"DELETE FROM claims WHERE peer_id = @peer AND path IN ({string.Join(", ", names)})";
                for (var index = 0; index < paths.Count; index++)
                    cmd.Parameters.AddWithValue(names[index], paths[index]);
            }
            cmd.Parameters.AddWithValue("@peer", peerId);
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    /// <summary>
    /// Drops rows whose TTL lapsed a day ago. Expiry is already invisible to every reader through
    /// <see cref="ActiveJoin"/>; this only stops the table growing for the 7 days a peer row is retained.
    /// </summary>
    public async Task<int> PruneExpiredAsync(CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM claims WHERE datetime(expires_at) < datetime('now', '-1 day')";
            return await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
    }

    private static ClaimRow Read(SqliteDataReader reader) => new()
    {
        RepoRoot = reader.GetString(0),
        Path = reader.GetString(1),
        PeerId = reader.GetString(2),
        ExpiresAt = DbTime.ParseUtc(reader.GetString(3)),
        HolderRole = reader.IsDBNull(4) ? null : reader.GetString(4),
        HolderSummary = reader.IsDBNull(5) ? null : reader.GetString(5),
    };
}

public sealed record ClaimRow
{
    public required string RepoRoot { get; init; }
    public required string Path { get; init; }
    public required string PeerId { get; init; }
    public required DateTime ExpiresAt { get; init; }
    public string? HolderRole { get; init; }
    public string? HolderSummary { get; init; }
}
