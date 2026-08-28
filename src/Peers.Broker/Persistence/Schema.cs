using Microsoft.Data.Sqlite;

namespace Peers.Broker.Persistence;

public static class Schema
{
    public const int CurrentVersion = 5;

    public static void Migrate(SqliteConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA user_version;";
        var version = Convert.ToInt32(cmd.ExecuteScalar());

        if (version < 1) ApplyV1(conn);
        if (version < 2) ApplyV2(conn);
        if (version < 3) ApplyV3(conn);
        if (version < 4) ApplyV4(conn);
        if (version < 5) ApplyV5(conn);

        // Forward only. An older binary opening a newer database must not stamp its own version over
        // the real one: the columns would stay, the version would lie, and the next new-binary start
        // would re-run a migration whose work is already there.
        if (version >= CurrentVersion)
            return;

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

    private static void ApplyV2(SqliteConnection conn)
    {
        var sql = """
            CREATE TABLE IF NOT EXISTS peer_inbox (
                inbox_id            TEXT PRIMARY KEY,
                peer_id             TEXT NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
                event_id            INTEGER NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
                acked_at            TEXT,
                created_at          TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_inbox_peer ON peer_inbox(peer_id, acked_at);
            CREATE INDEX IF NOT EXISTS idx_inbox_event ON peer_inbox(event_id);
            """;

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Channels are the one communication primitive: named boards scoped optionally to a repo, with
    /// append-only mention-addressed messages. Quiet messages ride the same table — visibility is
    /// filtered at read time, so there is exactly one copy of every message ever stored. The scratchpad
    /// tables this replaces are dropped: their data was session-scoped coordination, not durable state.
    /// </summary>
    private static void ApplyV3(SqliteConnection conn)
    {
        var sql = """
            CREATE TABLE IF NOT EXISTS channels (
                channel_id          TEXT PRIMARY KEY,
                name                TEXT NOT NULL,
                repo_root           TEXT,
                created_by_peer_id  TEXT NOT NULL,
                created_at          TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_scope ON channels(COALESCE(repo_root, ''), name);

            CREATE TABLE IF NOT EXISTS channel_messages (
                message_id      TEXT PRIMARY KEY,
                channel_id      TEXT NOT NULL REFERENCES channels(channel_id) ON DELETE CASCADE,
                seq             INTEGER NOT NULL,
                author_peer_id  TEXT NOT NULL,
                content         TEXT NOT NULL,
                mentions        TEXT NOT NULL DEFAULT '[]',
                quiet           INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_messages_seq ON channel_messages(channel_id, seq);
            CREATE INDEX IF NOT EXISTS idx_channel_messages_author ON channel_messages(author_peer_id);

            DROP TABLE IF EXISTS scratchpad_entries;
            DROP TABLE IF EXISTS scratchpads;
            """;

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Long-running work reports progress on the peer row itself, through the presence heartbeat:
    /// last value wins, one current task per peer, no lifecycle. progress_updated_at is separate from
    /// updated_at because the 30s all-null lease renewal bumps updated_at for every live peer — it
    /// cannot distinguish "at 7/12 and moving" from "stalled at 7/12". The units being counted live in
    /// summary; a second note column would be one more string with exactly the same lifetime.
    ///
    /// Transactional and column-by-column idempotent, unlike V1-V3: SQLite has no
    /// ADD COLUMN IF NOT EXISTS, so three bare ALTERs interrupted midway leave a half-migrated table
    /// at user_version 3, and every later start then dies on "duplicate column name".
    /// </summary>
    private static void ApplyV4(SqliteConnection conn)
    {
        var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var info = conn.CreateCommand())
        {
            info.CommandText = "PRAGMA table_info(peers);";
            using var reader = info.ExecuteReader();
            while (reader.Read())
                existing.Add(reader.GetString(reader.GetOrdinal("name")));
        }

        var columns = new (string Name, string Type)[]
        {
            ("progress_current", "INTEGER"),
            ("progress_total", "INTEGER"),      // NULL = indeterminate: counting up with no known end
            ("progress_updated_at", "TEXT"),
        };

        using var txn = conn.BeginTransaction();
        foreach (var (name, type) in columns)
        {
            if (existing.Contains(name))
                continue;

            using var cmd = conn.CreateCommand();
            cmd.Transaction = txn;
            cmd.CommandText = $"ALTER TABLE peers ADD COLUMN {name} {type};";
            cmd.ExecuteNonQuery();
        }
        txn.Commit();
    }

    /// <summary>
    /// Two coordination gaps close here.
    ///
    /// claims: which peer is working on which path. presence.focus_paths was advisory metadata nobody
    /// could act on — two sessions in one repo learned they were editing the same file by reading prose
    /// in a channel, if at all. A claim is (repo_root, path, peer) with a TTL; overlap is a fact the
    /// broker reports at claim time. Holder liveness comes from the peer lease, so a dead session's
    /// claims stop blocking anyone the moment its lease lapses — no release handshake required.
    ///
    /// channel_messages.idempotency_key: a retried send is one message, not two. SendMessageRequest has
    /// carried the field since the scratchpad era, but nothing read it and the only unique index lived
    /// on scratchpad_entries, which V3 dropped — so a provider that retries a tool call (seen doing it
    /// seven times on one call id) double-posted review rounds. Partial index: NULL keys stay unbounded.
    ///
    /// Column-by-column idempotent and transactional, for the reason V4 spells out.
    /// </summary>
    private static void ApplyV5(SqliteConnection conn)
    {
        var messageColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var info = conn.CreateCommand())
        {
            info.CommandText = "PRAGMA table_info(channel_messages);";
            using var reader = info.ExecuteReader();
            while (reader.Read())
                messageColumns.Add(reader.GetString(reader.GetOrdinal("name")));
        }

        using var txn = conn.BeginTransaction();

        if (!messageColumns.Contains("idempotency_key"))
        {
            using var alter = conn.CreateCommand();
            alter.Transaction = txn;
            alter.CommandText = "ALTER TABLE channel_messages ADD COLUMN idempotency_key TEXT;";
            alter.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = txn;
            cmd.CommandText = """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_messages_idempotency
                    ON channel_messages(channel_id, idempotency_key)
                    WHERE idempotency_key IS NOT NULL;

                CREATE TABLE IF NOT EXISTS claims (
                    claim_id    TEXT PRIMARY KEY,
                    peer_id     TEXT NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
                    repo_root   TEXT NOT NULL,
                    path        TEXT NOT NULL,
                    expires_at  TEXT NOT NULL,
                    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_claims_owner ON claims(repo_root, path, peer_id);
                CREATE INDEX IF NOT EXISTS idx_claims_scope ON claims(repo_root);
                """;
            cmd.ExecuteNonQuery();
        }

        txn.Commit();
    }
}
