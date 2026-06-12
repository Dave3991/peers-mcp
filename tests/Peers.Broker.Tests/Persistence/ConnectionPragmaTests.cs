using Peers.Broker.Tests.Helpers;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

public sealed class ConnectionPragmaTests : IDisposable
{
    private readonly TestDb _db = new();

    [Fact]
    public async Task FactoryConnections_HaveExpectedPerConnectionPragmas()
    {
        var (fk, sync, busy) = await _db.Factory.ReadAsync(conn =>
        {
            long ReadPragma(string p)
            {
                using var c = conn.CreateCommand();
                c.CommandText = $"PRAGMA {p}";
                return (long)c.ExecuteScalar()!;
            }
            return Task.FromResult((ReadPragma("foreign_keys"), ReadPragma("synchronous"), ReadPragma("busy_timeout")));
        }, CancellationToken.None);

        Assert.Equal(1, fk);      // foreign_keys ON
        Assert.Equal(1, sync);    // synchronous NORMAL == 1
        Assert.Equal(5000, busy); // busy_timeout milliseconds
    }

    public void Dispose() => _db.Dispose();
}
