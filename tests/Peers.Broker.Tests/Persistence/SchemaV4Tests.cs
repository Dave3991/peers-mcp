using Microsoft.Data.Sqlite;
using Peers.Broker.Persistence;
using Peers.Broker.Tests.Helpers;
using Xunit;

namespace Peers.Broker.Tests.Persistence;

/// <summary>
/// TestDb migrates a fresh database straight to the current schema, so re-running Migrate only
/// exercises the already-current guard. These build a real older database and migrate it forward.
/// </summary>
public sealed class SchemaV4Tests : IDisposable
{
    private readonly TestDb _db = new();

    private static readonly string[] ProgressColumns =
        ["progress_current", "progress_total", "progress_updated_at"];

    private static HashSet<string> Columns(SqliteConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA table_info(peers);";
        using var r = cmd.ExecuteReader();
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (r.Read())
            names.Add(r.GetString(r.GetOrdinal("name")));
        return names;
    }

    private static int Version(SqliteConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA user_version;";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    private static void Exec(SqliteConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>Rewinds a current database to look like one that stopped partway through V4.</summary>
    private static void RewindTo(SqliteConnection conn, int version, params string[] dropColumns)
    {
        foreach (var col in dropColumns)
            Exec(conn, $"ALTER TABLE peers DROP COLUMN {col};");
        Exec(conn, $"PRAGMA user_version = {version};");
    }

    [Fact]
    public void Migrate_V3ToV4_AddsProgressColumns()
    {
        using var conn = _db.Factory.CreateConnection();
        conn.Open();
        RewindTo(conn, 3, ProgressColumns);
        Assert.DoesNotContain("progress_current", Columns(conn));

        Schema.Migrate(conn);

        var columns = Columns(conn);
        foreach (var col in ProgressColumns)
            Assert.Contains(col, columns);
        Assert.Equal(Schema.CurrentVersion, Version(conn));
    }

    // Three bare ALTERs interrupted midway leave the table half-migrated at user_version 3. Without
    // the per-column guard the next start dies on "duplicate column name" and never recovers.
    [Fact]
    public void Migrate_ResumesFromPartiallyAddedColumns()
    {
        using var conn = _db.Factory.CreateConnection();
        conn.Open();
        RewindTo(conn, 3, "progress_total", "progress_updated_at");

        Schema.Migrate(conn);

        var columns = Columns(conn);
        foreach (var col in ProgressColumns)
            Assert.Contains(col, columns);
        Assert.Equal(Schema.CurrentVersion, Version(conn));
    }

    // An older binary opening a newer database must not stamp its own version over the real one.
    [Fact]
    public void Migrate_DoesNotDowngrade_WhenDatabaseIsNewer()
    {
        using var conn = _db.Factory.CreateConnection();
        conn.Open();
        Exec(conn, "PRAGMA user_version = 99;");

        Schema.Migrate(conn);

        Assert.Equal(99, Version(conn));
    }

    // Not an idempotency test: the forward-only guard returns before the migration body, so this pins
    // only that a current database is left alone. Migrate_ResumesFromPartiallyAddedColumns is the one
    // that actually re-enters ApplyV4.
    [Fact]
    public void Migrate_OnACurrentDatabase_IsANoOp()
    {
        using var conn = _db.Factory.CreateConnection();
        conn.Open();

        Schema.Migrate(conn);
        Schema.Migrate(conn);

        Assert.Equal(Schema.CurrentVersion, Version(conn));
    }

    public void Dispose() => _db.Dispose();
}
