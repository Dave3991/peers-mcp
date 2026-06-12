# Peers MCP Server — Improvement Plan

**Audience:** Claude Sonnet code agents implementing improvements one task at a time.
**Status:** Analysis complete (2026-06-12), against commit `74c17d8` + uncommitted working tree
(`FindActiveReplacementAsync` reviewer re-routing in `PeerRepository.cs` / `ScratchpadService.cs`,
new `ScratchpadServiceTests.cs`).

---

## 1. How to work in this repo (read first)

```bash
dotnet build                                               # build everything
dotnet test                                                # run tests (tests/Peers.Broker.Tests)
dotnet run --project src/Peers.Broker --no-launch-profile  # run broker on localhost:7856
dotnet run --project src/Peers.Cli -- status               # smoke-check broker + peers
```

Ground rules for every task below:

- **Do not rename or remove existing MCP tools** (`peer_register`, `peer_inbox`, `scratchpad_*`,
  `artifact_*`, `context_*`, `peer_poll`, `peer_test_push`). Live sessions depend on these names.
- **Schema changes go through `Schema.cs` as a new version** (`ApplyV3`, bump `CurrentVersion`),
  never by editing `ApplyV1`/`ApplyV2`. Migrations must be idempotent (`IF NOT EXISTS` etc.) and
  safe to run on an existing `~/.peers/peers.db`.
- **Broker error style:** services return `FluentResults.Result` with typed errors from
  `Peers.Broker.Errors` (`NotFoundError`, `ConflictError`, `BadRequestError`) mapped to HTTP via
  `.ToHttp()`. MCP tools catch `BrokerApiException` and return `{ "error": ..., "status_code": ... }`
  JSON strings. Follow this; do not throw raw exceptions across layers.
- **All writes go through `SqliteConnectionFactory.WriteAsync`** (single in-process write
  semaphore). Multi-statement invariants must use one transaction inside one `WriteAsync` (see
  `EventService.EmitAsync` for the pattern).
- **Timestamps:** SQLite stores both `datetime('now')` strings (`YYYY-MM-DD HH:MM:SS`, UTC) and
  C#-written ISO-8601 (`"O"` format). Comparisons in SQL must wrap both sides in `datetime(...)`
  (this caused a real bug fixed in commit `ccd3d8f`). Task T3 addresses this properly.
- One test project exists: `tests/Peers.Broker.Tests` (xunit, real temp-file SQLite via
  `Helpers/TestDb.cs`). Add tests there for broker work; create `tests/Peers.Mcp.Tests` only when a
  task explicitly says so.
- After each task: `dotnet build && dotnet test` must pass.

### Architecture refresher

- `src/Peers.Broker` — ASP.NET Core minimal API on `127.0.0.1:7856`, SQLite (WAL) at
  `~/.peers/peers.db`. Tables (schema v2): `peers`, `scratchpads`, `scratchpad_entries`,
  `artifacts`, `events` (append-only, monotonic `event_id`), `peer_inbox` (durable per-peer
  delivery with `acked_at`).
- `src/Peers.Mcp` — stdio MCP server, one per AI session. `BrokerClient` wraps the HTTP API.
  Per-session background loops: `HeartbeatService` (30 s lease renewal; lease is 2 min) and
  `ChannelNotifier` (polls `/poll/{peerId}` every 5 s, pushes directed events to the client via
  `notifications/message` or Claude's `notifications/claude/channel`).
- Delivery model: every directed event is written to `events` + one `peer_inbox` row per recipient
  in a single transaction. `peer_inbox` MCP tool drains unacked rows with deferred ack
  (at-least-once). Push is best-effort on top.
- `src/Peers.Cli` — manual test harness. `src/Peers.Shared` — contracts/models +
  `BrokerRuntime` (discovery file, launch lock, auto-start).

---

## 2. Findings summary

| ID | Priority | Area | One-liner |
|----|----------|------|-----------|
| T1 | P0 | Broker | Unbounded growth: expired peers, acked inbox rows, and inbox-pinned events are never purged |
| T2 | P0 | MCP | `peer_inbox` silently discards drained in-memory channel messages; dual delivery paths half-merged |
| T3 | P0 | Broker | Inconsistent datetime storage/parsing (`Unspecified` kind leaks to clients; mixed formats in DB) |
| T4 | P1 | MCP | `peer_register` doesn't expose `worktree_root` or `capabilities`, breaking identity resume and discovery |
| T5 | P1 | MCP | Heartbeat loop never recovers when the broker forgets the peer (404 forever = zombie session) |
| T6 | P1 | MCP | `peer_inbox` / `peer_poll` responses are not size-capped — can blow the 30 KB tool-result budget by 100× |
| T7 | P1 | Broker | Pragmas (`synchronous`, `busy_timeout`) applied to only the first connection; `Cache=Shared` questionable |
| T8 | P2 | Security | No auth token; any local process can impersonate any peer, ack/steal inboxes, deregister others |
| T9 | P2 | MCP | `ChannelNotifier` logs raw `Console.Error` lines for every event; replace with `ILogger` + levels |
| T10 | P2 | UX | `scratchpad_read` truncation drops *all* entries instead of fitting as many as possible; `artifact_get` advertises section fetch that doesn't exist |
| T11 | P2 | UX | `peer_list` cannot show recently-expired peers or self; no pagination on any list tool |
| T12 | P3 | Feature | Direct peer messaging tool (`peer_message`) — today DMs require creating a scratchpad |
| T13 | P3 | Feature | `peer_wait` long-poll tool: block until a directed event arrives (kills the 5 s poll lag in review loops) |
| T14 | P3 | Bridge | `context_export` YAML is hand-built and injection-prone; `context_import` parser is line-based and lossy |
| T15 | P4 | Tests | Coverage is 13 tests across 2 services; no tests for events/inbox/artifacts/prune/resume |
| T16 | P4 | Ops | Misc: health `version` vs discovery `schema_version` mismatch, registration race, double-close events |

Recommended execution order: **T1 → T3 → T2 → T7 → T6 → T5 → T4 → T9 → T10 → T11 → T8 → T15 → T13 → T12 → T14 → T16**.
T1–T7 are independent of each other except where noted. T8 touches every HTTP call — do it after
the P0/P1 churn settles. T15 (tests) should also be grown incrementally inside each task.

---

## 3. Tasks

### T1 (P0) — Retention: purge expired peers, acked inbox rows, and unstick event pruning

**Files:** `src/Peers.Broker/Persistence/Repositories/PeerRepository.cs`,
`InboxRepository.cs`, `EventRepository.cs`, `src/Peers.Broker/Services/EventService.cs`,
`PeerService.cs`, `src/Peers.Broker/Hosting/CleanupBackgroundService.cs`, `Schema.cs` (index only,
if needed).

**Problem.** Nothing ever deletes rows from `peers`. Every session start inserts a new
`peer_id` (resume only matches a *single* expired identity candidate — see
`PeerService.TryFindResumablePeerAsync`, which returns `null` when 2+ expired matches exist, so
duplicates accumulate and then *prevent future resumes*). Consequences chain:

1. `peers` grows forever; `peer_list(includeExpired: true)` and resume matching degrade.
2. `peer_inbox` rows for dead peers are never acked, and both `EventRepository.PruneBeforeAsync`
   and `PruneOlderThanAsync` exclude events referenced by **unacked** inbox rows
   (`event_id NOT IN (SELECT event_id FROM peer_inbox WHERE acked_at IS NULL)`), so those events
   are pinned **forever** — the 72 h TTL in `EventService` is a fiction for directed events.
3. Acked inbox rows are only removed if FK cascade fires when their event is deleted; verify that
   `PRAGMA foreign_keys` is actually ON for the connections we open (e_sqlite3 bundles usually
   default it on, but make it explicit — see T7).

**Change.**
- Add `PeerRepository.PurgeExpiredBeforeAsync(DateTime cutoff)`: `DELETE FROM peers WHERE
  expired_at IS NOT NULL AND datetime(expired_at) < datetime(@cutoff)`. Retention window: make it
  a const in `PeerService` or config, default **24 h** (long enough that "recent collaborators"
  stay visible per the original design, short enough to bound growth).
- Before deleting a peer, delete (or rely on explicit cascade of) its `peer_inbox` rows in the
  same transaction. Do not rely on implicit FK config — write the `DELETE FROM peer_inbox WHERE
  peer_id IN (...)` explicitly so behavior is unambiguous.
- Add `InboxRepository.PurgeAckedBeforeAsync(TimeSpan age)` (e.g. acked > 24 h ago) so acked rows
  don't wait for event TTL.
- Add a cap-based safety valve: when a single peer has > N unacked inbox rows (suggest N = 500),
  ack-and-drop the oldest beyond N (they're still in `events` until TTL). This prevents one
  never-draining peer from pinning the event log.
- Wire all of the above into `CleanupBackgroundService.ExecuteAsync` (runs every 30 s; the purges
  can run on a slower cadence, e.g. every 10th cycle, to keep write-lock pressure low).
- Keep the rule "never prune scratchpads/artifacts from event cleanup" — scratchpad/artifact
  retention is **out of scope** here (it's user data; only add a CLI command if trivial).

**Acceptance criteria.**
- New unit tests in `Peers.Broker.Tests` prove: (a) expired peer older than retention is deleted
  along with its inbox rows; (b) a recently-expired peer survives; (c) an event pinned only by a
  purged peer's unacked inbox row becomes prunable; (d) unacked overflow beyond the cap is
  dropped oldest-first.
- A peer with multiple stale expired duplicate identities can resume again after purge (test the
  `TryFindResumablePeerAsync` interaction).

---

### T2 (P0) — Fix the dual delivery path: stop discarding drained channel messages

**Files:** `src/Peers.Mcp/Tools/PollTools.cs` (`Inbox`), `src/Peers.Mcp/SessionState.cs`,
`src/Peers.Mcp/Lifecycle/ChannelNotifier.cs`.

**Problem.** `ChannelNotifier` enqueues a formatted `PendingChannelMessage` into
`SessionState` for **every** deliverable event ("Always enqueue for inbox"). The `peer_inbox` tool
then calls `state.DrainChannelMessages(limit)` — and **throws the drained messages away**: only
`dropped_message_count` and `remaining_count` are serialized into the response
(`PollTools.cs`, `Inbox`, around line 68). The durable broker inbox covers the same events, so no
data is lost *today*, but the in-memory queue is pure waste: it duplicates event content in
memory (up to 100 × ~100 KB), produces confusing `dropped_message_count` noise in responses, and
is a trap for future readers who assume the buffer feeds the inbox.

**Change.** Pick ONE of these (first is recommended):
- **Remove the in-memory inbox role entirely.** `ChannelNotifier` keeps formatting/pushing but no
  longer enqueues into `SessionState`; delete `EnqueueChannelMessage` / `DrainChannelMessages` /
  `PendingInboxAckIds`-adjacent buffer fields; remove `dropped_message_count` / `remaining_count`
  from the `peer_inbox` response shape (or keep keys with constant 0 for one release if you're
  worried about client prompts referencing them — they're MCP tool results read by LLMs, so
  removing is fine). Update the tool `Description` accordingly.
- *(Alternative)* Treat the in-memory queue as a fast-path mirror: include drained message
  contents in the response and ack the matching durable rows immediately. This is more code and
  re-introduces dedup questions — only choose if push-less reading of channel-formatted text is
  actually wanted.

Also update `PEERS_MAX_BUFFERED_MESSAGES` documentation in README if the buffer goes away.

**Acceptance criteria.**
- `peer_inbox` returns every directed event exactly once via durable items (existing behavior),
  with no vestigial counters, and memory no longer grows with undrained pushes.
- `ChannelNotifier` still pushes (test push still works via `peer_test_push`).
- README "Durable message delivery" + tool descriptions match the new reality.

---

### T3 (P0) — Normalize datetime handling end-to-end

**Files:** all repositories (`PeerRepository.cs`, `EventRepository.cs`, `InboxRepository.cs`,
`ScratchpadRepository.cs`, `ArtifactRepository.cs`), `Schema.cs` (no migration needed — format is
compatible), service layer where `DateTime.UtcNow` is written.

**Problem.** Two formats coexist in the DB: SQLite `datetime('now')` (`2026-06-12 08:00:00`) and
.NET round-trip `"O"` (`2026-06-12T08:00:00.0000000Z`, only `lease_expires_at`). Reads use bare
`DateTime.Parse(...)` → `Kind=Unspecified`, which .NET treats as **local** when converting or
comparing, and which serializes to JSON without a `Z` suffix — so MCP clients receive ambiguous
timestamps and `CreatedAt`/`UpdatedAt` arithmetic in C# (e.g. `OrderByDescending(peer =>
peer.UpdatedAt)` in resume matching) silently mixes kinds. The SQL-side comparisons were already
patched once (`ccd3d8f`) by wrapping everything in `datetime(...)`; the C# side was not.

**Change.**
- Single helper (suggest `Peers.Broker.Persistence.Db.ParseUtc(string)` or extension on
  `SqliteDataReader`): `DateTime.Parse(s, CultureInfo.InvariantCulture,
  DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal)`. Use it for **every**
  datetime column read in every repository.
- Standardize all *writes* from C# to ISO-8601 UTC with `Z` (`DateTime.UtcNow.ToString("O")`), and
  keep `datetime('now')` defaults as-is (they're UTC). Both forms order/compare correctly under
  SQLite `datetime()`, which the SQL already uses.
- Audit: `grep -rn "DateTime.Parse" src/` must end up with zero bare parses.

**Acceptance criteria.**
- All `Peer`/`PeerEvent`/`Scratchpad`/`Artifact`/`InboxItem` timestamps have `Kind=Utc` (add a
  unit test reading a freshly inserted row).
- JSON responses serialize timestamps with `Z`.
- Existing lease-expiry tests still pass against a pre-existing DB containing the old mixed
  formats (write a test inserting a `datetime('now')`-style string and parsing it).

---

### T4 (P1) — Expose `worktree_root` and `capabilities` in `peer_register` / `peer_heartbeat`

**Files:** `src/Peers.Mcp/Tools/PeerTools.cs`.

**Problem.** `RegisterPeerRequest` has `WorktreeRoot` and `Capabilities` and **identity-based
session resume compares `WorktreeRoot`** (`PeerService.MatchesIdentity`) and the new
`FindActiveReplacementAsync` matches on it too — but the MCP tool never lets the client send it,
so it is always `NULL` from real sessions. Two Claude sessions in different worktrees of the same
repo+branch currently look identical to implicit resume (the "active peer matches → don't
resume" guard saves correctness, but a restarted worktree session can resume the *other*
worktree's identity). `Capabilities` is simply dead from MCP.

**Change.**
- Add optional `worktree_root` (string) and `capabilities` (string[]) parameters to
  `peer_register`, serialize capabilities the same way `focus_paths` is.
- Add `capabilities` to `peer_heartbeat` updates (requires threading through
  `HeartbeatRequest` → `PeerRepository.RenewLeaseAsync` `COALESCE` update — follow the existing
  pattern for `focus_paths`).
- Update tool descriptions: instruct callers to pass the git worktree root when working in a
  linked worktree.
- Update README tool table + CLAUDE.md workflow contract if wording changes.

**Acceptance criteria.**
- Registering from two worktrees of one repo produces distinct resumable identities (broker-level
  test through `PeerService`).
- `peer_list` output (already serializes the full peer) shows capabilities.

---

### T5 (P1) — Self-healing heartbeat: re-register when the broker loses the peer

**Files:** `src/Peers.Mcp/Lifecycle/HeartbeatService.cs`, `src/Peers.Mcp/Tools/PeerTools.cs`
(extract registration request building), `src/Peers.Mcp/SessionState.cs`.

**Problem.** If the broker DB is wiped/recreated (or the peer is purged per T1, deregistered by
another session, or expired during a long laptop sleep), `HeartbeatAsync` returns 404 *forever*;
`HeartbeatService` just logs a warning every 30 s. The session believes it's registered
(`state.PeerId` set); all its appends/publishes keep "working" but no peer row backs it —
`peer_poll`/`peer_inbox` return 404 ("Call peer_register first"), and other peers can't discover
it. The agent has no signal to recover.

**Change.**
- Store the last successful `RegisterPeerRequest` on `SessionState` when `peer_register` runs.
- In `HeartbeatService`, on a 404 (`BrokerApiException` with `StatusCode == NotFound`):
  re-register using the stored request **with `ResumePeerId = current peer id`** (broker keeps the
  ID if the row still exists on the same machine; otherwise it mints a new one). Update
  `state.PeerId`/`state.LastCursor` from the response. Log one `Information` line. Guard against
  re-register storms (e.g. at most once per minute).
- `ChannelNotifier` reads `_state.PeerId` per iteration already, so it follows the new ID
  automatically; verify the cursor remains valid (it does — cursor is global, not per-peer).

**Acceptance criteria.**
- Manual/integration test: register, delete the peer row directly via repository, wait one
  heartbeat tick (make the interval injectable for tests — constructor parameter with default
  30 s), assert the peer re-appears and `state.PeerId` is consistent.
- No behavior change on transient network errors (non-404): keep logging and retrying as today.

---

### T6 (P1) — Cap `peer_inbox` and `peer_poll` result sizes

**Files:** `src/Peers.Mcp/Tools/PollTools.cs`, `src/Peers.Mcp/Limits.cs` (reuse
`MaxToolResultChars`).

**Problem.** `scratchpad_read` and `artifact_get` enforce the 30 KB tool-result cap, but the two
highest-volume tools do not. `peer_inbox` items embed the **full event payload**, and
`scratchpad.entry_added` payloads contain the **entire entry content** (up to 100 KB each). With
the default `limit ?? 100`, one inbox drain can return multiple MB straight into the model
context. Same for `peer_poll` (limit 100, full payloads).

**Change.**
- In `Inbox` and `Poll`, serialize incrementally (or post-process): include items until adding the
  next one would exceed `Limits.MaxToolResultChars`; stop there, set
  `truncated: true`, `returned_count`, `remaining_hint` ("call again to continue"). **Critical for
  inbox at-least-once semantics:** only stage `PendingInboxAckIds` for the items actually
  *returned*, so unreturned items get redelivered on the next call.
- Additionally truncate each item's event `content` field (entry bodies) to a per-item preview
  (suggest 2 000 chars) with `content_truncated: true` and the `scratchpad_id`/`entry_id` so the
  agent can `scratchpad_read` the full text. Full payload stays in the broker; this only shapes
  the tool result.
- Update both tool descriptions to document the truncation contract.

**Acceptance criteria.**
- A test (this one justifies creating `tests/Peers.Mcp.Tests` with plain unit tests around the
  shaping logic — extract the shaping into a testable static helper, e.g.
  `InboxResultShaper`) proving: 10 items of 50 KB each → response under the cap, acks staged only
  for returned items, second call returns the remainder.

---

### T7 (P1) — SQLite connection hygiene: per-connection pragmas, drop shared cache

**Files:** `src/Peers.Broker/Persistence/SqliteConnectionFactory.cs`,
`tests/Peers.Broker.Tests/Helpers/TestDb.cs` (comment references shared cache).

**Problem.** The constructor opens one throwaway connection and runs
`PRAGMA journal_mode=WAL; busy_timeout=5000; synchronous=NORMAL;`. `journal_mode=WAL` is
persistent (stored in the DB file) — fine. `synchronous` and `busy_timeout` are **per-connection**
and silently revert to defaults for every connection opened afterwards (i.e., all real traffic).
Also `Cache = SqliteCacheMode.Shared` is a legacy mode that is discouraged in combination with
WAL and can introduce surprising `SQLITE_LOCKED` table-level locking between connections; with
WAL + the in-process write semaphore it buys nothing. The `TestDb` comment ("Keep a connection
open so in-memory shared cache stays alive") is stale — tests use temp *files*, not in-memory DBs.

**Change.**
- Apply `PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;` on every
  connection open. Cleanest: add a private `OpenAsync(ct)` helper in the factory that creates,
  opens, and pragma-initializes the connection; use it from `WriteAsync`/`ReadAsync`/
  `CreateConnection` callers. (Explicit `foreign_keys=ON` makes the `peer_inbox` cascade behavior
  deterministic instead of relying on e_sqlite3 compile flags — T1 depends on knowing this.)
- Remove `Cache = SqliteCacheMode.Shared` from the connection string.
- Fix/remove the stale keep-alive comment & connection in `TestDb` (keep the keep-alive only if
  removing it breaks tests — it shouldn't for file-backed DBs).

**Acceptance criteria.**
- `dotnet test` green.
- A test asserting `PRAGMA foreign_keys` returns 1 and `PRAGMA synchronous` returns 1 (NORMAL) on
  a connection produced by the factory.

---

### T8 (P2) — Local auth token + ownership checks

**Files:** `src/Peers.Broker/Program.cs` (discovery file write), `src/Peers.Shared/BrokerRuntime.cs`
(`BrokerInfo`, discovery read), `src/Peers.Mcp/BrokerClient.cs`, new middleware in
`src/Peers.Broker/Errors/` or `Hosting/`, `src/Peers.Cli/Program.cs`.

**Problem.** The original plan specified a per-user token in the discovery file; it was never
implemented. Today **any local process** (any OS user on a shared machine) can: register fake
peers, read every scratchpad/artifact, ack/steal another peer's inbox
(`POST /peers/{peerId}/inbox/ack` checks only that the peer exists), append entries as any
`author_peer_id`, and `DELETE /peers/{peerId}` to deregister someone else. Threat model is local
multi-user / malicious local software; payloads are code reviews and diffs, i.e. real IP.

**Change (keep it deliberately simple).**
1. Broker generates a random 32-byte token at startup, writes it into the discovery file
   (`{ port, pid, token, started_at, schema_version }` — matches the original plan shape) and
   `chmod 600`s the file (and the `~/.peers` dir to 700) on Unix; on Windows leave ACLs default.
2. Add middleware: every request except `GET /health` must carry
   `Authorization: Bearer <token>`; otherwise 401. Constant-time comparison.
3. `BrokerClient` reads the token via `BrokerRuntime.ReadDiscovery()` and sends the header. CLI
   too. `BrokerInfo` record gains `Token` (nullable for back-compat when reading an old file —
   treat missing token as "broker too old, fail with clear message").
4. Ownership checks stay out of scope except the two cheap ones: `DELETE /peers/{peerId}` and
   `inbox/ack` — these are *already* per-peer routes; with a single shared token, true ownership
   needs per-peer secrets, which is overkill for v1. Document this residual trust explicitly in
   README ("all sessions of one user trust each other").

**Acceptance criteria.**
- Request without/with-wrong token → 401; with token → works (integration-style test using
  `WebApplicationFactory` — this is a good seed for the integration test project in T15).
- Old discovery file (no token) → MCP client fails fast with an actionable error, not a hang.
- Health stays unauthenticated (launch-wait logic depends on it).

---

### T9 (P2) — Replace `Console.Error` chatter in `ChannelNotifier` with `ILogger`

**Files:** `src/Peers.Mcp/Lifecycle/ChannelNotifier.cs`, `src/Peers.Mcp/Tools/PeerTools.cs`
(constructor wiring), `src/Peers.Mcp/Program.cs`.

**Problem.** `ChannelNotifier` writes an unconditional stderr line for *every* polled event,
including skipped ones ("Skipping event ... shouldDeliver=false") — that's a line per event per
session every 5 s in busy setups, in a component that otherwise has structured logging available
(logging is already configured to stderr in `Program.cs`). `PeerTools.PeerRegister` also writes
raw stderr on failure.

**Change.** Inject `ILogger<ChannelNotifier>` (factory already available at the
`PeerRegister` call site). Map: lifecycle + delivery success → `Debug`; skips → `Trace`; push
failures → `Warning`; poll loop errors → `Warning`. Keep messages structured
(`{EventId}`, `{EventType}`). Delete the `Console.Error` calls.

**Acceptance criteria.** No `Console.Error.WriteLineAsync` left in `src/Peers.Mcp` (grep);
default log level (`Information`) produces near-silent steady-state output.

---

### T10 (P2) — Smarter truncation: partial `scratchpad_read`, ranged `artifact_get`

**Files:** `src/Peers.Mcp/Tools/ScratchpadTools.cs`, `ArtifactTools.cs`.

**Problem.**
- `scratchpad_read`: when the serialized response exceeds the cap, it returns **zero** entries
  (metadata + "use limit/offset"), even if 90% of the entries would fit. The agent then has to
  guess a workable `limit`.
- `artifact_get`: truncated responses say "Use artifact_get with the artifact_id to fetch specific
  sections" — **no such capability exists**; the tool would return the same truncated prefix
  again. The instruction actively misleads agents into a retry loop.

**Change.**
- `scratchpad_read`: include entries greedily (newest-first or in requested order — keep current
  order) until the budget is reached; respond with the included entries plus
  `{ truncated: true, included_count, entry_count, next_offset }`.
- `artifact_get`: add optional `offset` (chars) and `max_chars` parameters; default `max_chars` to
  fit the cap. Truncated response returns `{ content, offset, next_offset, total_chars,
  truncated: true }`. Fix the message to reference `offset`/`next_offset`. (Char-based, not
  byte-based — content is a .NET string end to end.)

**Acceptance criteria.** Unit tests on the shaping helpers (same pattern as T6); descriptions
updated to match; an artifact larger than the cap is fully retrievable in N sequential calls.

---

### T11 (P2) — Discovery quality: expired visibility, self-marking, pagination

**Files:** `src/Peers.Mcp/Tools/PeerTools.cs`, `src/Peers.Broker/Routes/PeerRoutes.cs`,
`src/Peers.Broker/Services/PeerService.cs`, `ScratchpadTools.cs`/`ScratchpadRoutes.cs` (list),
`ArtifactRoutes.cs` (list) as needed.

**Problem.** The design says expired peers should remain visible for a retention window so
sessions understand recent collaborators — but `peer_list` hard-filters them
(`includeExpired: false`, no flag exposed). A reviewer whose lease lapsed during a long task
simply vanishes. There's also no way to tell which entry is *you*, and none of the three list
tools paginate (a long-running setup returns every scratchpad ever).

**Change.**
- `peer_list`: add `include_expired` (default false). When true, include peers with
  `expired_at != null` (bounded by T1's purge window) and add `expired_at`/`is_expired` to the
  serialized output. Thread the flag through route → service → existing repo parameter (already
  supports it).
- Mark self: `peer_list` response items gain `"self": true` where `peer_id == state.PeerId`
  (MCP-side decoration; broker stays ignorant).
- `scratchpad_list`: add `limit` (default 50, newest first) + `offset`; same for `artifact_list`
  (repo `ListAsync` queries need `ORDER BY created_at DESC LIMIT @limit OFFSET @offset`).

**Acceptance criteria.** Tool descriptions updated; broker tests for the new filters; default
behavior unchanged for existing callers (same shapes, plus new optional fields).

---

### T12 (P3) — `peer_message`: lightweight directed messages

**Files:** new `src/Peers.Mcp/Tools/MessageTools.cs`, `src/Peers.Broker/Routes/` (new route),
`EventService`/`EventTypes`, `src/Peers.Mcp/Lifecycle/ChannelNotifier.cs` (format new event type),
`Peers.Shared/Models/PeerEvent.cs` (`EventTypes` const), `RequestLimits`.

**Problem.** The only way to say one thing to one peer is to create a whole scratchpad. README
already markets "directed messages", and the durable inbox + recipient-targeted event machinery
is fully built — a DM is just an event with one recipient and no scratchpad row.

**Change.**
- New event type `peer.message` with payload `{ from_peer_id, to_peer_ids, content,
  recipient_peer_ids }` (reuse the recipient resolution from `ScratchpadService.
  TryResolveReviewerPeerIdsAsync` — extract it into a shared `RecipientResolver` service so DMs
  get the same prefix-match + repo-target + expired-replacement semantics; this also pays down
  duplication in `ScratchpadService`).
- Broker route `POST /messages` with body `{ from_peer_id, targets: [...], content }`; validate
  content ≤ 100 KB (reuse `MaxEntryContentBytes`); emit via `EventService.EmitAsync` (event +
  inbox rows transactionally, exactly like scratchpad events). No new table.
- MCP tool `peer_message(targets, content, quiet?)` returning the same
  `{ recipients, delivery_summary }` shape as `scratchpad_create`.
- `ChannelNotifier.FormatEvent`: render `peer.message` pushes ("Peers DM from {peer}: {preview}").

**Acceptance criteria.** DM lands in the recipient's `peer_inbox` and (non-quiet) push; offline
recipient receives it after re-registering+resume; broker test covers resolution + event/inbox
atomicity.

---

### T13 (P3) — `peer_wait`: long-poll for new directed events

**Files:** `src/Peers.Broker/Routes/PollRoutes.cs` (or new route), `InboxRepository`, new MCP tool
in `PollTools.cs`.

**Problem.** Review loops ("appended my review request — now what?") force the agent to either
busy-call `peer_inbox` or end its turn. The 5 s `ChannelNotifier` push covers interactive Claude,
but pull-mode (default) agents have nothing blocking to wait on. A long-poll makes
"request review → wait for reply → continue" a single tool call.

**Change.**
- Broker: `GET /peers/{peerId}/inbox/wait?timeout_seconds=N` (cap N at 120; default 60). Loop
  server-side: check `GetUnackedAsync`; if empty, `Task.Delay(1s, ct)` and re-check until timeout.
  (Polling inside the broker is fine — it's a local SQLite read; don't build a pub-sub bus for
  this.) Return the same `InboxResponse` shape, possibly empty on timeout with
  `{ timed_out: true }`.
- MCP tool `peer_wait(timeout_seconds?)`: calls the route, then reuses the exact `peer_inbox`
  drain/ack/shaping path (share the implementation with the `Inbox` tool — same deferred-ack
  staging, same T6 size shaping). Set `BrokerClient`'s `HttpClient` timeout safely above the max
  wait (check the current `Timeout` — default 100 s — and bump per-request via
  `HttpRequestMessage` + `CancellationTokenSource` rather than globally).
- Tool description must tell agents when to use it ("after requesting a review, call peer_wait
  instead of repeatedly calling peer_inbox") and that it can time out empty.

**Acceptance criteria.** Integration-style test: enqueue an inbox row from another "peer" 2 s
after the wait starts → wait returns early with the item; no item → returns at timeout with
`timed_out: true`. Deferred-ack semantics identical to `peer_inbox`.

---

### T14 (P3) — Harden the context bridge (YAML correctness)

**Files:** `src/Peers.Mcp/Tools/ContextTools.cs`.

**Problem.** `Export` builds YAML by string interpolation — a title or summary containing `"`,
`\n`, or YAML-significant tokens corrupts the packet (and `Import`'s hand-rolled line parser then
mis-parses or silently drops data). `Import` returns raw line lists for most sections rather than
typed objects, so round-tripping loses structure. This is the designated Codex handoff format —
it should be deterministic.

**Change (keep the bridge thin, per the original plan).**
- Escape every interpolated scalar: write a small `YamlEscape(string)` that double-quotes and
  backslash-escapes (`"`, `\`, `\n`, `\t`) — or switch the machine-readable block to **JSON in a
  ```json fence** (JSON is valid YAML; `System.Text.Json` gives correctness for free and `Import`
  becomes `JsonDocument.Parse`). Recommended: switch to JSON, keep the `<!-- peers-context:v1 -->`
  marker, accept *both* on import (try JSON parse of the fenced block first, fall back to the
  current line parser for old packets).
- `Import`: return typed structures for `peers`/`scratchpads`/`artifacts` (arrays of objects, not
  string lists).
- Export should optionally include recent entries of open scratchpads
  (`include_scratchpad_entries: false` default; last 3 entries, 500-char previews) so a handoff
  actually carries the review state, not just IDs.

**Acceptance criteria.** Round-trip test: export with a title containing `"` and newlines →
import → identical values. Old-format packet still imports.

---

### T15 (P4) — Test coverage: events, inbox, artifacts, resume, prune

**Files:** `tests/Peers.Broker.Tests/**`.

Current coverage is 13 tests over `PeerService` + `ScratchpadService`. Highest-value gaps, in
order:

1. `EventService`: `EmitAsync` with recipients writes event + inbox atomically (kill the txn
   mid-way is hard; at least assert both rows exist and event ids match); `PruneAsync` respects
   min-active-cursor, TTL, and the unacked-inbox pin (overlaps T1 tests).
2. `InboxRepository`: drain order (`ORDER BY i.event_id`), ack scoping (`AND peer_id=@peer` —
   assert peer A cannot ack peer B's row), `AckAllAsync`.
3. `ArtifactService`/`ArtifactRepository`: version auto-increment per (namespace, name); content
   dedup (`Deduplicated = true` on identical content); `GetLatestAsync` returns highest version.
4. `PeerService.TryFindResumablePeerAsync`: explicit resume wrong-machine rejection; implicit
   resume with 0/1/2 expired candidates; active-match blocks resume.
5. `ScratchpadService` recipient resolution: prefix match, ambiguous prefix → unresolved, repo
   fallback + same-branch preference, expired→replacement re-route (the uncommitted feature —
   `ScratchpadServiceTests.cs` may already cover some; extend rather than duplicate).
6. Seed an integration test project (`tests/Peers.IntegrationTests`) with `WebApplicationFactory`
   covering one end-to-end flow: register two peers → create review pad targeting peer B → B
   drains inbox → B appends → A drains inbox → close. (T8's auth test lives here too.)

**Acceptance criteria.** Each listed behavior has at least one test; suite stays under ~30 s.

---

### T16 (P4) — Small fixes batch

One commit, low risk, no design decisions:

1. **Version literals:** `/health` returns `version = 2` hard-coded in `PollRoutes.cs`; the
   discovery file writes `schema_version = 1` in `Broker/Program.cs`; `Schema.CurrentVersion` is
   2. Make both read `Schema.CurrentVersion`.
2. **Double-close:** `ScratchpadService.CloseAsync` doesn't check `pad.Status == "closed"` (unlike
   `ResolveAsync`), so closing twice emits a duplicate `scratchpad.closed` event + inbox spam.
   Add the same `ConflictError` guard.
3. **Registration race:** `RegisterAsync` does find-resumable → insert/reactivate as two separate
   writes; two simultaneous registrations of the same identity can both resume the same expired
   row or double-insert. Wrap find+insert in one `WriteAsync` transaction (follow the
   `EmitAsync` pattern).
4. **Prune-before-first-poll:** `GetMinActiveCursorAsync` ignores peers with
   `last_polled_event_id IS NULL`, so a registered-but-never-polled peer doesn't hold the prune
   line and can lose broadcast events. Treat active peers with NULL cursor as cursor 0 (i.e., if
   any active peer has NULL, skip cursor-based pruning that cycle; TTL pruning still runs).
5. **`PeerRegister` error path** returns `{ error }` via generic `catch (Exception)` — narrow to
   `BrokerApiException` + `HttpRequestException` and let truly unexpected exceptions surface to
   the MCP framework (consistency with every other tool).
6. **`ArtifactTools.Get` mutual-exclusion message** when both id and ns+name passed is fine — but
   `GetArtifactByNameAsync` URL-encodes? Verify `BrokerClient.BuildQuery` escapes values
   (`Uri.EscapeDataString`) — artifact names/namespaces with `&`/`#` must round-trip; add a test.
7. **README/config table:** remove or update `PEERS_MAX_BUFFERED_MESSAGES` after T2; document new
   env vars/params added by T6/T10/T11.

---

## 4. Explicit non-goals (do not build these)

Confirmed from the original design (`peers-mcp-plan.md`) and still correct:

- No WebSocket/SSE push transport — long-poll (T13) is the ceiling for v1.
- No cloud sync, no cross-machine networking, no multi-user collaboration.
- No scratchpad/artifact deletion APIs or content retention policies (user data; events and peers
  only, per T1).
- No per-peer cryptographic identity; the single shared token (T8) plus "one user's sessions
  trust each other" is the accepted model.
- Don't replace the hand-rolled migration system or introduce EF Core / Dapper — the repo's raw
  ADO pattern is deliberate and consistent.

## 5. Verification after any task

```bash
dotnet build && dotnet test
# end-to-end smoke (broker + CLI):
dotnet run --project src/Peers.Broker --no-launch-profile &   # or: peers-cli broker-start
dotnet run --project src/Peers.Cli -- status                  # expect healthy + peer list
curl -s http://127.0.0.1:7856/health                          # {"status":"ok",...}
```

For MCP-side changes, the realistic test is two live Claude Code sessions (see README), but the
minimum bar is: broker tests green + CLI smoke + tool descriptions updated to match new behavior.
