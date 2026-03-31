using Microsoft.Data.Sqlite;

namespace Peers.Broker.Persistence;

public sealed class SqliteConnectionFactory : IDisposable
{
    private readonly string _connectionString;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public SqliteConnectionFactory(string dbPath)
    {
        var dir = Path.GetDirectoryName(dbPath);
        if (dir is not null) Directory.CreateDirectory(dir);

        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared,
        }.ToString();

        using var conn = CreateConnection();
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA synchronous=NORMAL;";
        cmd.ExecuteNonQuery();
    }

    public SqliteConnection CreateConnection()
    {
        return new SqliteConnection(_connectionString);
    }

    public SemaphoreSlim WriteLock => _writeLock;

    public async Task<T> WriteAsync<T>(Func<SqliteConnection, Task<T>> action, CancellationToken ct = default)
    {
        await _writeLock.WaitAsync(ct);
        try
        {
            await using var conn = CreateConnection();
            await conn.OpenAsync(ct);
            return await action(conn);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task WriteAsync(Func<SqliteConnection, Task> action, CancellationToken ct = default)
    {
        await _writeLock.WaitAsync(ct);
        try
        {
            await using var conn = CreateConnection();
            await conn.OpenAsync(ct);
            await action(conn);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task<T> ReadAsync<T>(Func<SqliteConnection, Task<T>> action, CancellationToken ct = default)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct);
        return await action(conn);
    }

    public void Dispose()
    {
        _writeLock.Dispose();
    }
}
