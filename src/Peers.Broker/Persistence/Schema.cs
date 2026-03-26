using Microsoft.Data.Sqlite;

namespace Peers.Broker.Persistence;

public static class Schema
{
    private const int CurrentVersion = 1;

    public static void Migrate(SqliteConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA user_version;";
        var version = Convert.ToInt32(cmd.ExecuteScalar());

        if (version < 1) ApplyV1(conn);

        using var setVersion = conn.CreateCommand();
        setVersion.CommandText = $"PRAGMA user_version = {CurrentVersion};";
        setVersion.ExecuteNonQuery();
    }

    private static void ApplyV1(SqliteConnection conn)
    {
        var sql = """
            CREATE TABLE IF NOT EXISTS peers (
                peer_id             TEXT PRIMARY KEY,
                tool                TEXT NOT NULL DEFAULT 'claude-code',
                machine_id          TEXT NOT NULL,
                repo_root           TEXT,
                worktree_root       TEXT,
                branch              TEXT,
                commit_sha          TEXT,
                role                TEXT,
                summary             TEXT,
                focus_paths         TEXT,
                capabilities        TEXT,
                parent_peer_id      TEXT,
                last_polled_event_id INTEGER,
                lease_expires_at    TEXT NOT NULL,
                expired_at          TEXT,
                created_at          TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_peers_status ON peers(expired_at);
            CREATE INDEX IF NOT EXISTS idx_peers_repo ON peers(repo_root);
            CREATE INDEX IF NOT EXISTS idx_peers_lease ON peers(lease_expires_at);

            CREATE TABLE IF NOT EXISTS scratchpads (
                scratchpad_id       TEXT PRIMARY KEY,
                type                TEXT NOT NULL,
                title               TEXT NOT NULL,
                repo_root           TEXT,
                status              TEXT NOT NULL DEFAULT 'open',
                requested_reviewers TEXT,
                artifact_refs       TEXT,
                created_by_peer_id  TEXT NOT NULL,
                created_at          TEXT NOT NULL DEFAULT (datetime('now')),
                closed_at           TEXT,
                resolution          TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_scratchpads_type ON scratchpads(type);
            CREATE INDEX IF NOT EXISTS idx_scratchpads_status ON scratchpads(status);

            CREATE TABLE IF NOT EXISTS scratchpad_entries (
                entry_id            TEXT PRIMARY KEY,
                scratchpad_id       TEXT NOT NULL REFERENCES scratchpads(scratchpad_id),
                author_peer_id      TEXT NOT NULL,
                content             TEXT NOT NULL,
                idempotency_key     TEXT,
                content_bytes       INTEGER NOT NULL DEFAULT 0,
                created_at          TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_entries_pad ON scratchpad_entries(scratchpad_id, created_at);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_entries_idempotency ON scratchpad_entries(scratchpad_id, idempotency_key)
                WHERE idempotency_key IS NOT NULL;

            CREATE TABLE IF NOT EXISTS artifacts (
                artifact_id         TEXT PRIMARY KEY,
                namespace           TEXT NOT NULL,
                name                TEXT NOT NULL,
                version             INTEGER NOT NULL DEFAULT 1,
                type                TEXT NOT NULL,
                repo_root           TEXT,
                branch              TEXT,
                commit_sha          TEXT,
                content_type        TEXT,
                content             TEXT NOT NULL,
                content_hash        TEXT NOT NULL,
                content_bytes       INTEGER NOT NULL DEFAULT 0,
                tags                TEXT,
                published_by_peer_id TEXT NOT NULL,
                created_at          TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(namespace, name, version)
            );

            CREATE INDEX IF NOT EXISTS idx_artifacts_name ON artifacts(namespace, name);
            CREATE INDEX IF NOT EXISTS idx_artifacts_type ON artifacts(type);

            CREATE TABLE IF NOT EXISTS events (
                event_id            INTEGER PRIMARY KEY AUTOINCREMENT,
                type                TEXT NOT NULL,
                payload             TEXT NOT NULL,
                created_at          TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
            """;

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
