using System.Text.Json;
using Microsoft.Data.Sqlite;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Broker.Persistence.Repositories;

/// <summary>
/// Channels are named message boards scoped optionally to a repository; messages are append-only and
/// carry an explicit mention list. Quiet messages are participant-scoped: delivered as inbox items to
/// mentioned peers, invisible to everyone else's reads and notifications.
/// </summary>
public sealed class ChannelRepository(SqliteConnectionFactory db)
{
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

    /// <summary>
    /// Appends one message inside a single transaction: per-channel sequence assignment, the row, and
    /// nothing else. Inbox delivery and the event row happen through <see cref="Services.EventService"/>
    /// afterwards — they are already atomic together, and the message must exist before any reader can
    /// be told about it.
    /// </summary>
    public async Task<MessageRow> AppendMessageAsync(MessageRow message, CancellationToken ct = default)
    {
        return await db.WriteAsync(async conn =>
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO channel_messages (message_id, channel_id, seq, author_peer_id, content, mentions, quiet, created_at)
                VALUES (
                    @id, @channel,
                    (SELECT COALESCE(MAX(seq), 0) + 1 FROM channel_messages WHERE channel_id = @channel),
                    @author, @content, @mentions, @quiet, datetime('now'))
                RETURNING seq, created_at
                """;
            cmd.Parameters.AddWithValue("@id", message.MessageId);
            cmd.Parameters.AddWithValue("@channel", message.ChannelId);
            cmd.Parameters.AddWithValue("@author", message.AuthorPeerId);
            cmd.Parameters.AddWithValue("@content", message.Content);
            cmd.Parameters.AddWithValue("@mentions", JsonSerializer.Serialize(message.Mentions));
            cmd.Parameters.AddWithValue("@quiet", message.Quiet ? 1 : 0);

            await using var r = await cmd.ExecuteReaderAsync(ct);
            await r.ReadAsync(ct);
            return message with { Seq = r.GetInt64(0), CreatedAt = r.GetString(1) };
        }, ct);
    }

    /// <summary>
    /// Quiet messages are visible only to their participants: the author and the mentioned peers.
    /// A null viewer sees none of them — reading a whole channel never leaks whispers addressed to
    /// somebody else.
    /// </summary>
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
    public required string CreatedAt { get; init; }
}
