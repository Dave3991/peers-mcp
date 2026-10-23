using System.Text.Json;
using Microsoft.Data.Sqlite;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Broker.Persistence.Repositories;

/// <summary>
/// Channels are named message boards scoped optionally to a repository; messages have stable sequence
/// numbers and carry an explicit mention list. Quiet messages are delivered as inbox items to
/// mentioned peers, invisible to everyone else's reads and notifications.
/// </summary>
public sealed class ChannelRepository(SqliteConnectionFactory db)
{
    public SqliteConnectionFactory Database => db;

    public async Task<ChannelRow> InsertAsync(ChannelRow channel, CancellationToken ct = default)
    {
        await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO channels (channel_id, name, repo_root, created_by_peer_id, created_at)
                VALUES (@id, @name, @repo, @peer, datetime('now'))
                """;
            cmd.Parameters.AddWithValue("@id", channel.ChannelId);
            cmd.Parameters.AddWithValue("@name", channel.Name);
            cmd.Parameters.AddWithValue("@repo", (object?)channel.RepoRoot ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@peer", channel.CreatedByPeerId);
            await cmd.ExecuteNonQueryAsync(ct);
            return 0;
        }, ct);
        return channel;
    }

    /// <summary>Channel scope is (repo_root, name); a null repo_root is its own scope, so both sides coalesce.</summary>
    public async Task<ChannelRow?> GetByNameAsync(string name, string? repoRoot, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT channel_id, name, repo_root, created_by_peer_id, created_at
                FROM channels
                WHERE name = @name AND COALESCE(repo_root, '') = COALESCE(@repo, '')
                """;
            cmd.Parameters.AddWithValue("@name", name);
            cmd.Parameters.AddWithValue("@repo", (object?)repoRoot ?? DBNull.Value);
            return await ReadOneAsync(cmd, ct);
        }, ct);
    }

    public async Task<ChannelRow?> GetAsync(string channelId, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT channel_id, name, repo_root, created_by_peer_id, created_at
                FROM channels WHERE channel_id = @id
                """;
            cmd.Parameters.AddWithValue("@id", channelId);
            return await ReadOneAsync(cmd, ct);
        }, ct);
    }

    public async Task<List<ChannelSummary>> ListAsync(string? repoRoot, int limit, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT c.channel_id, c.name, c.repo_root, c.created_by_peer_id, c.created_at,
                       (SELECT COUNT(*) FROM channel_messages m WHERE m.channel_id = c.channel_id) AS message_count
                FROM channels c
                WHERE (@repo IS NULL OR COALESCE(c.repo_root, '') = @repo)
                ORDER BY c.created_at DESC
                LIMIT @limit
                """;
            cmd.Parameters.AddWithValue("@repo", (object?)repoRoot ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@limit", limit);
            var result = new List<ChannelSummary>();
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                result.Add(new ChannelSummary
                {
                    ChannelId = r.GetString(r.GetOrdinal("channel_id")),
                    Name = r.GetString(r.GetOrdinal("name")),
                    RepoRoot = r.IsDBNull(r.GetOrdinal("repo_root")) ? null : r.GetString(r.GetOrdinal("repo_root")),
                    CreatedByPeerId = r.GetString(r.GetOrdinal("created_by_peer_id")),
                    CreatedAt = r.GetString(r.GetOrdinal("created_at")),
                    MessageCount = r.GetInt64(r.GetOrdinal("message_count")),
                });
            }
            return result;
        }, ct);
    }

    /// <summary>Sequence assignment and append share the caller's transaction, or create their own.</summary>
    public async Task<MessageRow> AppendMessageAsync(MessageRow message, CancellationToken ct = default,
        SqliteConnection? existingConn = null)
    {
        async Task<MessageRow> AppendAsync(SqliteConnection conn)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                UPDATE channels SET last_seq = last_seq + 1 WHERE channel_id = @channel;
                INSERT INTO channel_messages (message_id, channel_id, seq, author_peer_id, content, mentions, quiet, idempotency_key, created_at)
                VALUES (
                    @id, @channel,
                    (SELECT last_seq FROM channels WHERE channel_id = @channel),
                    @author, @content, @mentions, @quiet, @key, datetime('now'))
                RETURNING seq, created_at
                """;
            cmd.Parameters.AddWithValue("@id", message.MessageId);
            cmd.Parameters.AddWithValue("@channel", message.ChannelId);
            cmd.Parameters.AddWithValue("@author", message.AuthorPeerId);
            cmd.Parameters.AddWithValue("@content", message.Content);
            cmd.Parameters.AddWithValue("@mentions", JsonSerializer.Serialize(message.Mentions));
            cmd.Parameters.AddWithValue("@quiet", message.Quiet ? 1 : 0);
            cmd.Parameters.AddWithValue("@key", (object?)message.IdempotencyKey ?? DBNull.Value);

            await using var r = await cmd.ExecuteReaderAsync(ct);
            await r.ReadAsync(ct);
            return message with { Seq = r.GetInt64(0), CreatedAt = r.GetString(1) };
        }

        if (existingConn is not null)
            return await AppendAsync(existingConn);
        return await db.WriteAsync(async conn =>
        {
            using var txn = conn.BeginTransaction();
            var row = await AppendAsync(conn);
            txn.Commit();
            return row;
        }, ct);
    }

    /// <summary>The message a previous send with this key already wrote, or null.</summary>
    public async Task<MessageRow?> GetByIdempotencyKeyAsync(string channelId, string key, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT message_id, channel_id, seq, author_peer_id, content, mentions, quiet, idempotency_key, created_at
                FROM channel_messages
                WHERE channel_id = @channel AND idempotency_key = @key
                """;
            cmd.Parameters.AddWithValue("@channel", channelId);
            cmd.Parameters.AddWithValue("@key", key);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return null;

            return new MessageRow
            {
                MessageId = reader.GetString(0),
                ChannelId = reader.GetString(1),
                Seq = reader.GetInt64(2),
                AuthorPeerId = reader.GetString(3),
                Content = reader.GetString(4),
                Mentions = JsonSerializer.Deserialize<string[]>(reader.GetString(5)) ?? [],
                Quiet = reader.GetInt64(6) != 0,
                IdempotencyKey = reader.IsDBNull(7) ? null : reader.GetString(7),
                CreatedAt = reader.GetString(8),
            };
        }, ct);
    }

    public async Task<List<RemovedMessageRow>> GetMessagesForRemovalAsync(
        string channelId, string messageIdsJson, SqliteConnection conn, CancellationToken ct)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT message_id, author_peer_id, quiet, mentions FROM channel_messages
            WHERE channel_id = @channel AND message_id IN (SELECT value FROM json_each(@ids))
            """;
        cmd.Parameters.AddWithValue("@channel", channelId);
        cmd.Parameters.AddWithValue("@ids", messageIdsJson);
        var rows = new List<RemovedMessageRow>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add(new RemovedMessageRow(reader.GetString(0), reader.GetString(1),
                reader.GetInt64(2) != 0, JsonSerializer.Deserialize<string[]>(reader.GetString(3)) ?? []));
        return rows;
    }

    public async Task<List<string>> GetMentionedPeerIdsAsync(
        string channelId, SqliteConnection conn, CancellationToken ct)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT DISTINCT j.value FROM channel_messages m, json_each(m.mentions) j
            WHERE m.channel_id = @channel
            """;
        cmd.Parameters.AddWithValue("@channel", channelId);
        var ids = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            ids.Add(reader.GetString(0));
        return ids;
    }

    /// <summary>Delete original events first; their inbox receipts cascade in the same transaction.</summary>
    public async Task<int> DeleteMessagesAsync(
        string channelId, string? messageIdsJson, SqliteConnection conn, CancellationToken ct)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DELETE FROM events
            WHERE type = @type AND json_extract(payload, '$.channel_id') = @channel
              AND (@ids IS NULL OR json_extract(payload, '$.message_id') IN (
                  SELECT message_id FROM channel_messages
                  WHERE channel_id = @channel AND message_id IN (SELECT value FROM json_each(@ids))));
            """;
        cmd.Parameters.AddWithValue("@type", EventTypes.ChannelMessageAdded);
        cmd.Parameters.AddWithValue("@channel", channelId);
        cmd.Parameters.AddWithValue("@ids", (object?)messageIdsJson ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct);
        cmd.CommandText = """
            DELETE FROM channel_messages WHERE channel_id = @channel
              AND (@ids IS NULL OR message_id IN (SELECT value FROM json_each(@ids)));
            """;
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Quiet messages are visible only to their participants: the author and the mentioned peers.
    /// A null viewer sees none of them — reading a whole channel never leaks whispers addressed to
    /// somebody else.
    /// </summary>
    /// <summary>
    /// Every peer <paramref name="peerId"/> actually corresponds with: the ones it has mentioned, and
    /// the ones that have mentioned it. This is the audience for its presence changes.
    ///
    /// Mentions, not mere co-authorship in a channel. A fresh peer auto-announces itself into every
    /// channel scoped to its repo (see PeerService.RegisterAsync), so "has posted in the same channel"
    /// would make every new session a member of every channel in that repo — which is repo scoping
    /// wearing a disguise, and would keep waking a driver/reviewer pair for unrelated pairs' churn.
    /// Addressing someone is the thing that makes them your counterpart; join notices mention nobody.
    /// </summary>
    public async Task<List<string>> GetCorrespondentPeerIdsAsync(string peerId, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT DISTINCT p FROM (
                    -- peers this one has addressed
                    SELECT json_each.value AS p
                    FROM channel_messages m, json_each(m.mentions)
                    WHERE m.author_peer_id = @peer
                    UNION
                    -- peers that have addressed it
                    SELECT m.author_peer_id AS p
                    FROM channel_messages m
                    WHERE EXISTS (SELECT 1 FROM json_each(m.mentions) WHERE json_each.value = @peer)
                )
                WHERE p IS NOT NULL AND p <> @peer
                """;
            cmd.Parameters.AddWithValue("@peer", peerId);

            var ids = new List<string>();
            using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
                ids.Add(r.GetString(0));
            return ids;
        }, ct);
    }

    public async Task<List<MessageRow>> ReadMessagesAsync(
        string channelId, string? viewer, long sinceSeq, int limit, CancellationToken ct = default)
    {
        return await db.ReadAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT message_id, channel_id, seq, author_peer_id, content, mentions, quiet, created_at
                FROM channel_messages
                WHERE channel_id = @channel AND seq > @since
                  AND (quiet = 0
                       OR (@viewer IS NOT NULL AND author_peer_id = @viewer)
                       OR (@viewer IS NOT NULL AND EXISTS (
                           SELECT 1 FROM json_each(mentions) WHERE json_each.value = @viewer)))
                ORDER BY seq
                LIMIT @limit
                """;
            cmd.Parameters.AddWithValue("@channel", channelId);
            cmd.Parameters.AddWithValue("@since", sinceSeq);
            cmd.Parameters.AddWithValue("@viewer", (object?)viewer ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@limit", limit);

            var result = new List<MessageRow>();
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                result.Add(new MessageRow
                {
                    MessageId = r.GetString(r.GetOrdinal("message_id")),
                    ChannelId = r.GetString(r.GetOrdinal("channel_id")),
                    Seq = r.GetInt64(r.GetOrdinal("seq")),
                    AuthorPeerId = r.GetString(r.GetOrdinal("author_peer_id")),
                    Content = r.GetString(r.GetOrdinal("content")),
                    Mentions = JsonSerializer.Deserialize<string[]>(r.GetString(r.GetOrdinal("mentions"))) ?? [],
                    Quiet = r.GetInt64(r.GetOrdinal("quiet")) != 0,
                    CreatedAt = r.GetString(r.GetOrdinal("created_at")),
                });
            }
            return result;
        }, ct);
    }

    private static async Task<ChannelRow?> ReadOneAsync(SqliteCommand cmd, CancellationToken ct)
    {
        await using var r = await cmd.ExecuteReaderAsync(ct);
        if (!await r.ReadAsync(ct)) return null;
        return new ChannelRow
        {
            ChannelId = r.GetString(r.GetOrdinal("channel_id")),
            Name = r.GetString(r.GetOrdinal("name")),
            RepoRoot = r.IsDBNull(r.GetOrdinal("repo_root")) ? null : r.GetString(r.GetOrdinal("repo_root")),
            CreatedByPeerId = r.GetString(r.GetOrdinal("created_by_peer_id")),
            CreatedAt = r.GetString(r.GetOrdinal("created_at")),
        };
    }
}

public sealed record ChannelRow
{
    public required string ChannelId { get; init; }
    public required string Name { get; init; }
    public string? RepoRoot { get; init; }
    public required string CreatedByPeerId { get; init; }
    public required string CreatedAt { get; init; }
}

public sealed record MessageRow
{
    public required string MessageId { get; init; }
    public required string ChannelId { get; init; }
    public long Seq { get; init; }
    public required string AuthorPeerId { get; init; }
    public required string Content { get; init; }
    public string[] Mentions { get; init; } = [];
    public bool Quiet { get; init; }
    public string? IdempotencyKey { get; init; }
    public required string CreatedAt { get; init; }
}

public sealed record RemovedMessageRow(string MessageId, string AuthorPeerId, bool Quiet, string[] Mentions);
