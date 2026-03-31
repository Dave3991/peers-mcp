using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence;

namespace Peers.Broker.Tests.Helpers;

public sealed class TestDb : IDisposable
{
    private readonly SqliteConnection _keepAlive;

    public SqliteConnectionFactory Factory { get; }

    public TestDb()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"peers_test_{Guid.NewGuid():N}.db");
        Factory = new SqliteConnectionFactory(dbPath);

        using var conn = Factory.CreateConnection();
        conn.Open();
        Schema.Migrate(conn);

        // Keep a connection open so in-memory shared cache stays alive
        _keepAlive = Factory.CreateConnection();
        _keepAlive.Open();
    }

    public void Dispose()
    {
        _keepAlive.Dispose();
        Factory.Dispose();
    }
}
