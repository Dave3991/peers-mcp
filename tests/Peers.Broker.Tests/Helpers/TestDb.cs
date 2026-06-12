using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence;

namespace Peers.Broker.Tests.Helpers;

public sealed class TestDb : IDisposable
{
    public SqliteConnectionFactory Factory { get; }

    public TestDb()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"peers_test_{Guid.NewGuid():N}.db");
        Factory = new SqliteConnectionFactory(dbPath);

        using var conn = Factory.CreateConnection();
        conn.Open();
        Schema.Migrate(conn);
    }

    public void Dispose()
    {
        Factory.Dispose();
    }
}
