# Peers MCP Server

## Custom Multi-Repo Coordination for Claude Code and Codex

### Goal

Replace `claude-peers-mcp` with a purpose-built coordination system for multiple AI coding sessions working across frontend and backend repositories.

The core use cases are:

- shared context across repos and worktrees
- asynchronous review threads between AI sessions
- artifact exchange such as API contracts, diffs, and generated types
- structured Claude Code <-> Codex context transfer
- support for multiple concurrent Claude and Codex sessions on the same machine or repo

### Recommendation

Prefer a `C#/.NET` implementation for the broker and persistence layer.

Reasoning:

- the hard part is coordination state, not MCP itself
- `.NET` is better suited for long-lived services, background workers, SQLite concurrency, leases, migrations, and typed contracts
- the MCP layer can stay thin and replaceable
- the official MCP C# SDK now supports stdio transport and tool registration, so a thin .NET MCP adapter is viable

Recommended stack:

- Broker daemon: `ASP.NET Core Minimal API`
- Persistence: `SQLite` in WAL mode
- Background tasks: `IHostedService` / `BackgroundService`
- MCP adapter: separate stdio process per AI session
- CLI/test harness: `Peers.Cli`

SDK note:

- treat the C# MCP SDK as usable but still operationally cautious until the project is on a stable `1.x` line
- pin a known-good package version in the solution
- avoid unnecessary framework magic in the first iteration
- if the SDK blocks progress, keep the broker in `.NET` and move only the MCP adapter to TypeScript

## Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│               Broker Daemon (localhost / loopback)          │
│  ASP.NET Core + SQLite + background cleanup + event log     │
│                                                              │
│  ┌──────────────┐  ┌────────────────┐  ┌──────────────────┐ │
│  │ Peer Registry│  │ Scratchpad     │  │ Artifact Store   │ │
│  │ Session Leases│ │ Review Threads │  │ Versioned Assets │ │
│  └──────────────┘  └────────────────┘  └──────────────────┘ │
│                                                              │
│                  Append-only Events / Polling                │
└───────────────┬───────────────────────┬──────────────────────┘
                │                       │
         MCP Server A             MCP Server B
           (stdio)                  (stdio)
                │                       │
         Claude / Codex           Claude / Codex
         session in FE            session in BE
```

Notes:

- one MCP server instance runs per AI session
- the broker is local-only and shared by all sessions
- Codex can be a first-class MCP client if supported, otherwise it participates through a manual context bridge
- the system must treat each running AI session as a distinct peer

## Core Design Principles

- session-centric, not tool-centric
- local-first, no cloud dependency
- append-only event model for coordination
- explicit leases and heartbeats for liveness
- deterministic context export/import format
- minimal v1 surface area
- operationally testable without live AI sessions

## Key Operational Constraints

- MCP tools are passive; the AI must choose to call them
- the broker may be launched concurrently by multiple MCP processes
- event growth must be bounded
- SQLite write contention must be handled deliberately
- large review payloads must not exceed practical stdio limits
- the context bridge is a compatibility layer, not the long-term center of the system

## Session Model

The primary entity is a live session, not "one AI per repo".

Each session registers independently, even if multiple sessions are attached to the same repository.

Suggested peer fields:

- `peer_id`
- `tool` (`claude-code`, `codex`, or another client)
- `machine_id`
- `repo_root`
- `worktree_root`
- `branch`
- `commit_sha`
- `role` (`implementing`, `reviewing`, `debugging`, `spec`)
- `summary`
- `focus_paths`
- `capabilities`
- `parent_peer_id` (optional, for helper/spawned sessions)
- `last_polled_event_id` (nullable)
- `lease_expires_at`
- `expired_at` (nullable)
- `created_at`
- `updated_at`

This allows:

- multiple Claude sessions on one repo
- multiple Codex sessions on one repo
- FE and BE sessions participating in the same review workflow
- scoped discovery by machine, repo, worktree, branch, or tool

Lease semantics:

- default lease duration: `2 minutes`
- heartbeat interval target: `30 seconds`
- stale peers transition to `expired` instead of disappearing immediately
- expired peers remain visible for a short retention window so other sessions can understand recent collaborators
- scratchpads and artifacts owned by expired peers remain intact and are never auto-closed

## Transport

### Broker

- local HTTP API bound to loopback only
- optional Unix socket later if desired
- discovery file under `~/.peers/`
- per-user token stored in the discovery file
- broker auto-launch on first MCP client connection
- discovery file shape: `{ port, pid, token, started_at, schema_version }`
- startup sequence: read discovery file -> health check -> if dead, acquire launch lock -> spawn broker -> wait for healthy -> register peer
- use a filesystem lock under `~/.peers/` so two MCP servers do not launch duplicate brokers
- run schema migration during broker startup before accepting traffic
- retry on `SQLITE_BUSY` with short bounded backoff
- use WAL mode and a small set of predictable write transactions

### MCP Servers

- stdio-based MCP adapter per AI session
- thin wrapper over broker HTTP API
- auto-register on startup
- send heartbeat every 30 seconds
- poll for updates on an interval
- maintain local session identity and last seen event cursor
- poll at natural breakpoints, not on every token of work
- support a manual `peer_poll` call and an optional lightweight background reminder loop

## Agent Behavior Contract

The system only works if participating AIs are instructed to use it.

Ship a shared prompt snippet for `CLAUDE.md` and `CODEX.md` with rules such as:

- you are participating in a multi-session workflow
- call `peer_poll` at startup, after completing a meaningful task chunk, before opening a new implementation thread, and before finishing
- if a review scratchpad targets you or your repo, prioritize reading and responding
- publish meaningful outputs as artifacts instead of pasting large blobs repeatedly into scratchpads
- update your summary or heartbeat when your role or focus changes

Without this prompt contract, `peer_poll` and review workflows will be underused.

## Coordination Model

Avoid a fragmented design with separate notifications, direct-message queues, and subscription-specific plumbing in v1.

Use a single append-only `events` table and a cursor-based `peer_poll(cursor)` flow.

Benefits:

- simpler implementation
- easier cross-session synchronization
- good fit for both MCP clients and manual Codex bridge imports
- supports incremental reads without custom per-feature queue logic

Operational rules:

- every event gets a monotonic integer ID
- each peer stores its last acknowledged event cursor
- prune events behind the minimum active cursor, with a fallback time-based retention window
- if no active cursors are available, retain events for a bounded TTL such as `24-72 hours`
- event writes should be idempotent where practical

## MCP Tool Surface

### Peer Management

- `peer_register`
  - register or resume a session
  - include repo, branch, role, summary, focus paths, and capabilities
- `peer_heartbeat`
  - renew lease and optionally update lightweight status fields
- `peer_list`
  - discover sessions by scope: machine, repo, worktree, branch, tool

### Scratchpads

Scratchpads are the core async collaboration primitive.

- `scratchpad_create`
  - create a pad with `type: review | discussion | spec | context`
- `scratchpad_append`
  - append a timestamped entry authored by a specific peer
- `scratchpad_read`
  - read entries, optionally since a given cursor or timestamp
  - support pagination for large threads
- `scratchpad_list`
  - list pads by type, repo, status, or tag
- `scratchpad_close`
  - close a pad with a resolution summary

Suggested scratchpad fields:

- `scratchpad_id`
- `type`
- `title`
- `repo_root`
- `status` (`open`, `resolved`, `closed`)
- `requested_reviewers`
- `artifact_refs`
- `created_by_peer_id`
- `created_at`
- `closed_at`
- `resolution`

Suggested entry fields:

- `entry_id`
- `scratchpad_id`
- `author_peer_id`
- `content`
- `idempotency_key`
- `content_bytes`
- `created_at`

Scratchpad size guidance:

- keep individual entries reasonably bounded, for example under `100 KB`
- prefer artifact references over embedding full generated outputs in entries
- page `scratchpad_read` results for long review threads

### Artifact Store

Artifacts are versioned named outputs referenced from reviews or context packets.

- `artifact_publish`
  - publish a typed artifact such as `openapi-spec`, `typescript-types`, `diff`, `summary`, `test-report`
- `artifact_get`
  - fetch by ID or by latest version within a namespace
- `artifact_list`
  - list by type, repo, tag, author, or name

Suggested artifact fields:

- `artifact_id`
- `namespace`
- `name`
- `version`
- `type`
- `repo_root`
- `branch`
- `commit_sha`
- `content_type`
- `content`
- `content_hash`
- `content_bytes`
- `tags`
- `published_by_peer_id`
- `created_at`

Use uniqueness stronger than `UNIQUE(name, version)`.

Recommended unique key:

- `UNIQUE(namespace, name, version)`

Also store `content_hash` for deduplication and integrity.

Artifact size guidance:

- large diffs, generated types, and OpenAPI specs are valid artifacts
- tool responses should return metadata and references first, not eagerly dump very large payloads
- `artifact_get` should support metadata-only fetches and full-content fetches

### Context Bridge

This is the compatibility path for Claude Code <-> Codex handoff when live MCP interoperability is not available.

- `context_export`
  - export session state as structured markdown
- `context_import`
  - parse structured markdown into typed sections

Important: the markdown format must be strict and versioned.

Use a deterministic block such as:

````md
<!-- peers-context:v1 -->
```yaml
task:
  title: ...
session:
  tool: codex
  repo_root: ...
  branch: ...
artifacts:
  - ...
scratchpads:
  - ...
changes:
  files:
    - ...
notes:
  - ...
```
````

Optional human-readable prose can follow, but the machine-readable block is the source of truth.

Planning note:

- do not over-invest in the bridge format
- if Codex gains stable native MCP integration, the bridge becomes a fallback path rather than the primary integration model

### Polling

- `peer_poll`
  - fetch all changes since a cursor
  - returns peer updates, scratchpad events, artifact publishes, and pad closures
  - returns a new cursor for the caller to persist
  - may support lightweight filtering by repo, branch, or peer scope

## Proposed v1 Tool Set

Keep v1 intentionally small:

- `peer_register`
- `peer_heartbeat`
- `peer_list`
- `scratchpad_create`
- `scratchpad_append`
- `scratchpad_read`
- `scratchpad_list`
- `scratchpad_close`
- `artifact_publish`
- `artifact_get`
- `artifact_list`
- `context_export`
- `context_import`
- `peer_poll`

Deferred for later:

- direct peer-to-peer messages
- artifact subscriptions
- WebSocket push

## Review Workflow

### End-to-End

1. Two or more sessions start and call `peer_register`
2. Sessions renew liveness with `peer_heartbeat`
3. Session A discovers review candidates with `peer_list`
4. Session A publishes an artifact such as generated types or a diff via `artifact_publish`
5. Session A opens a review thread with `scratchpad_create`
6. Session A adds context and questions via `scratchpad_append`
7. Session B calls `peer_poll` and sees new events
8. Session B reads the thread with `scratchpad_read`
9. Session B appends review comments via `scratchpad_append`
10. Session A polls, reads feedback, updates code, and appends status
11. Session A closes the thread via `scratchpad_close`

### Codex Bridge Workflow

Export from Claude Code to Codex:

1. call `context_export`
2. paste the structured markdown into Codex
3. Codex uses the packet as working context

Import from Codex to Claude Code:

1. user pastes Codex output into `context_import`
2. broker parses the packet into task, files, notes, artifacts, and related references
3. session can append the imported result into a scratchpad or publish it as an artifact

## Data Model

Prefer a smaller schema for v1.

### Tables

- `peers`
- `scratchpads`
- `scratchpad_entries`
- `artifacts`
- `events`

### `peers`

Stores live sessions and lease metadata.

Also stores last-seen cursor information needed for retention and recovery.

### `scratchpads`

Stores thread metadata and lifecycle state.

### `scratchpad_entries`

Stores append-only entries for discussion and review.

### `artifacts`

Stores versioned outputs with type and provenance.

### `events`

Append-only event log for incremental polling.

Suggested event types:

- `peer.registered`
- `peer.updated`
- `peer.expired`
- `scratchpad.created`
- `scratchpad.entry_added`
- `scratchpad.closed`
- `artifact.published`

Retention:

- prune events behind the minimum cursor across active peers
- keep a fallback TTL so abandoned cursors do not block cleanup forever
- never prune artifacts or scratchpad entries through event cleanup alone

## File Structure

```text
peers/
├── Peers.sln
├── src/
│   ├── Peers.Broker/
│   │   ├── Program.cs
│   │   ├── Hosting/
│   │   │   ├── BrokerStartupService.cs
│   │   │   └── CleanupBackgroundService.cs
│   │   ├── Persistence/
│   │   │   ├── SqliteConnectionFactory.cs
│   │   │   ├── Schema.cs
│   │   │   └── Repositories/
│   │   ├── Routes/
│   │   │   ├── PeerRoutes.cs
│   │   │   ├── ScratchpadRoutes.cs
│   │   │   ├── ArtifactRoutes.cs
│   │   │   └── PollRoutes.cs
│   │   └── Services/
│   │       ├── PeerService.cs
│   │       ├── ScratchpadService.cs
│   │       ├── ArtifactService.cs
│   │       └── EventService.cs
│   ├── Peers.Mcp/
│   │   ├── Program.cs
│   │   ├── BrokerClient.cs
│   │   ├── Lifecycle/
│   │   │   ├── RegistrationService.cs
│   │   │   ├── HeartbeatService.cs
│   │   │   └── BrokerDiscovery.cs
│   │   └── Tools/
│   │       ├── PeerTools.cs
│   │       ├── ScratchpadTools.cs
│   │       ├── ArtifactTools.cs
│   │       ├── ContextTools.cs
│   │       └── PollTools.cs
│   ├── Peers.Cli/
│   │   ├── Program.cs
│   │   └── Commands/
│   └── Peers.Shared/
│       ├── Contracts/
│       ├── Models/
│       ├── Ids/
│       └── ContextBridge/
│           ├── ContextPacket.cs
│           ├── ContextExporter.cs
│           └── ContextImporter.cs
└── tests/
    ├── Peers.Broker.Tests/
    ├── Peers.Mcp.Tests/
    └── Peers.IntegrationTests/
```

## Implementation Phases

### Phase 1: Foundation

1. Create solution and projects
2. Implement SQLite schema and migrations
3. Prove a minimal `.NET` stdio MCP server works with tool registration and one test tool
4. Build broker startup, discovery file handling, and launch locking
5. Implement MCP broker client
6. Add `peer_register`, `peer_heartbeat`, `peer_list`
7. Add broker auto-launch, lease cleanup, and `SQLITE_BUSY` retry handling
8. Create `Peers.Cli` for local simulation and test-driving broker routes

### Phase 2: Scratchpads and Polling

1. Implement scratchpad routes and MCP tools
2. Introduce append-only `events`
3. Implement `peer_poll(cursor)`
4. Add event retention and cursor tracking
5. Validate multi-session review flow across FE and BE repos
6. Add shared `CLAUDE.md` / `CODEX.md` workflow guidance

### Phase 3: Artifacts

1. Implement artifact routes and tools
2. Support versioned publish/get/list
3. Add metadata-only fetches and payload-size guards
4. Link artifact references into scratchpads and context packets

### Phase 4: Context Bridge

1. Define versioned context packet format
2. Implement `context_export`
3. Implement `context_import`
4. Keep the bridge thin and scoped as a compatibility shim
5. Test Claude Code <-> Codex manual handoff

### Phase 5: Polish

1. Improve README and setup
2. Harden auth, startup locking, and migration handling
3. Add paging, retention tuning, and observability
4. Add optional WebSocket push later if polling becomes limiting

## Verification

### Unit Tests

- broker route behavior
- lease expiry logic
- broker discovery and launch-lock behavior
- `SQLITE_BUSY` retry behavior
- scratchpad lifecycle
- artifact versioning and lookup
- context packet export/import parsing

### Integration Tests

- start broker
- register multiple MCP sessions
- run a full review workflow end-to-end
- publish an artifact and observe it via `peer_poll`
- simulate lease expiration and cleanup
- exercise large payload handling and paged scratchpad reads
- exercise broker restart and recovery from stale discovery metadata

### Manual Tests

- run two Claude Code sessions against different repos
- run multiple sessions against the same repo and confirm scoped discovery
- create a scratchpad in one session and review it in another
- export context to Codex, then import the result back
- use `Peers.Cli` to simulate peers, events, and reviews without live AI sessions

## Setup Direction

Target user-facing setup should remain simple.

Example shape:

```bash
claude mcp add --scope user --transport stdio peers -- dotnet run --project ~/peers/src/Peers.Mcp
```

Equivalent Codex integration can be added later if native MCP hookup becomes available.

## Required Companion Files

The plan assumes the repository ships prompt guidance alongside code.

Required docs:

- `CLAUDE.md` snippet for polling and review behavior
- `CODEX.md` snippet for the same workflow where applicable
- README section for multi-session setup and expectations
- CLI usage examples for local testing

## Non-Goals for v1

- cloud sync
- remote multi-user collaboration over network
- autonomous cross-agent orchestration
- complex authorization beyond local user isolation
- full chat product semantics

## Summary

Build a local-first coordination system with:

- a `.NET` broker as the durable core
- thin MCP session adapters
- explicit multi-session support from day one
- scratchpads as the main async review primitive
- artifacts as versioned shared outputs
- one append-only event stream for incremental polling
- a strict but intentionally lightweight markdown bridge for Codex interoperability
- a required prompt contract so AIs actually poll and review
- a required CLI so the system can be developed without live paired sessions

That gives a smaller and more robust v1 than a feature-heavy design centered on separate notifications, subscriptions, and ad hoc markdown.
