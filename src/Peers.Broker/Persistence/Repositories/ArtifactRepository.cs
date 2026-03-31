using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Peers.Shared.Models;

namespace Peers.Broker.Persistence.Repositories;

public sealed class ArtifactRepository(SqliteConnectionFactory db)
{
    public async Task<(Artifact artifact, int version)> PublishAsync(Artifact artifact, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var verCmd = conn.CreateCommand();
            verCmd.CommandText = "SELECT COALESCE(MAX(version), 0) FROM artifacts WHERE namespace = @ns AND name = @name";
            verCmd.Parameters.AddWithValue("@ns", artifact.Namespace);
            verCmd.Parameters.AddWithValue("@name", artifact.Name);
            var nextVersion = Convert.ToInt32(await verCmd.ExecuteScalarAsync(ct)) + 1;

            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(artifact.Content))).ToLowerInvariant();
            var bytes = Encoding.UTF8.GetByteCount(artifact.Content);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO artifacts (artifact_id, namespace, name, version, type, repo_root, branch, commit_sha,
                    content_type, content, content_hash, content_bytes, tags, published_by_peer_id, created_at)
                VALUES (@id, @ns, @name, @ver, @type, @repo, @branch, @commit,
                    @ctype, @content, @hash, @bytes, @tags, @peer, datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", artifact.ArtifactId);
            cmd.Parameters.AddWithValue("@ns", artifact.Namespace);
            cmd.Parameters.AddWithValue("@name", artifact.Name);
            cmd.Parameters.AddWithValue("@ver", nextVersion);
            cmd.Parameters.AddWithValue("@type", artifact.Type);
            cmd.Parameters.AddWithValue("@repo", (object?)artifact.RepoRoot ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@branch", (object?)artifact.Branch ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@commit", (object?)artifact.CommitSha ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@ctype", (object?)artifact.ContentType ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@content", artifact.Content);
            cmd.Parameters.AddWithValue("@hash", hash);
            cmd.Parameters.AddWithValue("@bytes", bytes);
            cmd.Parameters.AddWithValue("@tags", (object?)artifact.Tags ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@peer", artifact.PublishedByPeerId);
            await cmd.ExecuteNonQueryAsync(ct);

            return (artifact with { Version = nextVersion, ContentHash = hash, ContentBytes = bytes }, nextVersion);
        }, ct);
    }

    public async Task<Artifact?> GetByIdAsync(string artifactId, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM artifacts WHERE artifact_id = @id";
            cmd.Parameters.AddWithValue("@id", artifactId);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            return await r.ReadAsync(ct) ? ReadArtifact(r) : null;
        }, ct);
    }

    public async Task<Artifact?> GetLatestAsync(string ns, string name, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM artifacts WHERE namespace = @ns AND name = @name ORDER BY version DESC LIMIT 1";
            cmd.Parameters.AddWithValue("@ns", ns);
            cmd.Parameters.AddWithValue("@name", name);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            return await r.ReadAsync(ct) ? ReadArtifact(r) : null;
        }, ct);
    }

    public async Task<List<ArtifactMetadata>> ListAsync(string? type = null, string? repoRoot = null,
        string? ns = null, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            var where = new List<string>();
            if (type is not null) { where.Add("type = @type"); cmd.Parameters.AddWithValue("@type", type); }
            if (repoRoot is not null) { where.Add("repo_root = @repo"); cmd.Parameters.AddWithValue("@repo", repoRoot); }
            if (ns is not null) { where.Add("namespace = @ns"); cmd.Parameters.AddWithValue("@ns", ns); }

            var whereClause = where.Count > 0 ? "WHERE " + string.Join(" AND ", where) : "";
            cmd.CommandText = $"""
                SELECT artifact_id, namespace, name, version, type, repo_root, tags,
                    published_by_peer_id, content_bytes, created_at
                FROM artifacts {whereClause} ORDER BY created_at DESC
                """;

            var list = new List<ArtifactMetadata>();
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                list.Add(new ArtifactMetadata
                {
                    ArtifactId = r.GetString(r.GetOrdinal("artifact_id")),
                    Namespace = r.GetString(r.GetOrdinal("namespace")),
                    Name = r.GetString(r.GetOrdinal("name")),
                    Version = r.GetInt32(r.GetOrdinal("version")),
                    Type = r.GetString(r.GetOrdinal("type")),
                    RepoRoot = r.IsDBNull(r.GetOrdinal("repo_root")) ? null : r.GetString(r.GetOrdinal("repo_root")),
                    Tags = r.IsDBNull(r.GetOrdinal("tags")) ? null : r.GetString(r.GetOrdinal("tags")),
                    PublishedByPeerId = r.GetString(r.GetOrdinal("published_by_peer_id")),
                    ContentBytes = r.GetInt32(r.GetOrdinal("content_bytes")),
                    CreatedAt = DateTime.Parse(r.GetString(r.GetOrdinal("created_at"))),
                });
            }
            return list;
        }, ct);
    }

    private static Artifact ReadArtifact(SqliteDataReader r) => new()
    {
        ArtifactId = r.GetString(r.GetOrdinal("artifact_id")),
        Namespace = r.GetString(r.GetOrdinal("namespace")),
        Name = r.GetString(r.GetOrdinal("name")),
        Version = r.GetInt32(r.GetOrdinal("version")),
        Type = r.GetString(r.GetOrdinal("type")),
        RepoRoot = r.IsDBNull(r.GetOrdinal("repo_root")) ? null : r.GetString(r.GetOrdinal("repo_root")),
        Branch = r.IsDBNull(r.GetOrdinal("branch")) ? null : r.GetString(r.GetOrdinal("branch")),
        CommitSha = r.IsDBNull(r.GetOrdinal("commit_sha")) ? null : r.GetString(r.GetOrdinal("commit_sha")),
        ContentType = r.IsDBNull(r.GetOrdinal("content_type")) ? null : r.GetString(r.GetOrdinal("content_type")),
        Content = r.GetString(r.GetOrdinal("content")),
        ContentHash = r.GetString(r.GetOrdinal("content_hash")),
        ContentBytes = r.GetInt32(r.GetOrdinal("content_bytes")),
        Tags = r.IsDBNull(r.GetOrdinal("tags")) ? null : r.GetString(r.GetOrdinal("tags")),
        PublishedByPeerId = r.GetString(r.GetOrdinal("published_by_peer_id")),
        CreatedAt = DateTime.Parse(r.GetString(r.GetOrdinal("created_at"))),
    };
}
