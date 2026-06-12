using System.Globalization;
using Microsoft.Data.Sqlite;

namespace Peers.Broker.Persistence;

public static class DbTime
{
    // Parses both stored formats — SQLite datetime('now') ("2026-06-12 09:02:29")
    // and .NET round-trip "O" ("2026-06-12T09:02:29.0000000Z") — to a DateTime with Kind=Utc.
    public static DateTime ParseUtc(string s) =>
        DateTime.Parse(s, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

    public static DateTime GetUtc(this SqliteDataReader r, string column) =>
        ParseUtc(r.GetString(r.GetOrdinal(column)));

    public static DateTime? GetUtcOrNull(this SqliteDataReader r, string column)
    {
        var ord = r.GetOrdinal(column);
        return r.IsDBNull(ord) ? null : ParseUtc(r.GetString(ord));
    }
}
