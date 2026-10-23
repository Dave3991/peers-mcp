using Peers.Broker.Persistence;
using Peers.Broker.Persistence.Repositories;
using Peers.Broker.Tests.Helpers;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

public sealed class SchemaV6Tests : IDisposable
{
    private readonly TestDb _db = new();

    [Theory]
    [InlineData(false, 43)]
    [InlineData(true, 71)]
    public async Task Migrate_V5_PreservesHistoryAndSequenceHighWatermark(bool partiallyMigrated, long nextSeq)
    {
        using (var conn = _db.Factory.CreateConnection())
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO channels (channel_id, name, created_by_peer_id) VALUES ('c', 'channel', 'p');
                INSERT INTO channel_messages (message_id, channel_id, seq, author_peer_id, content)
                VALUES ('m', 'c', 42, 'p', 'existing');
                PRAGMA user_version = 5;
                """;
            cmd.ExecuteNonQuery();
            cmd.CommandText = partiallyMigrated
                ? "UPDATE channels SET last_seq = 70;"
                : "ALTER TABLE channels DROP COLUMN last_seq;";
            cmd.ExecuteNonQuery();
            Schema.Migrate(conn);
            // Re-enter the migration after an interrupted version stamp: do not lower last_seq.
            cmd.CommandText = "PRAGMA user_version = 5;";
            cmd.ExecuteNonQuery();
            Schema.Migrate(conn);
        }

        var repository = new ChannelRepository(_db.Factory);
        var existing = Assert.Single(await repository.ReadMessagesAsync("c", null, 0, 100));
        Assert.Equal("existing", existing.Content);
        Assert.Equal(42, existing.Seq);
        var next = await repository.AppendMessageAsync(new MessageRow
        {
            MessageId = "next", ChannelId = "c", AuthorPeerId = "p", Content = "new", CreatedAt = "",
        });
        Assert.Equal(nextSeq, next.Seq);
    }

    public void Dispose() => _db.Dispose();
}
