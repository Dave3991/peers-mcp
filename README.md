# Peers

**Let your AI coding sessions talk to each other.**

![Peers demo](demo.gif)

Peers is a local coordination server that connects multiple [Claude Code](https://docs.anthropic.com/en/docs/claude-code) and [Codex](https://openai.com/index/introducing-codex/) sessions so they can discover each other, exchange work, and talk in shared channels — all on your machine, no cloud required.

## Why?

Modern AI-assisted development often means running several coding sessions in parallel — one implementing a feature, another writing tests, a third reviewing changes. But these sessions are blind to each other. They duplicate work, drift apart, and can't share what they learn.

Peers fixes this. It gives each session awareness of what others are doing, a way to request reviews, share artifacts, and hand off context — turning isolated agents into a coordinated team.

## How It Works

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Claude Code  │     │ Claude Code  │     │    Codex      │
│  Session A   │     │  Session B   │     │  Session C    │
└──────┬───────┘     └──────┬───────┘     └──────┬────────┘
       │                    │                    │
       │  MCP (stdio)       │  MCP (stdio)       │  context
       │                    │                    │  export
       ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────┐
│                    Peers Broker                          │
│               localhost:7856 + SQLite                    │
└─────────────────────────────────────────────────────────┘
```

Each AI session connects through a lightweight **MCP server** that speaks [Model Context Protocol](https://modelcontextprotocol.io/) over stdio. The MCP server talks to a local **broker** (an ASP.NET Core HTTP server backed by SQLite) that handles peer discovery, message routing, and storage. Everything runs on `localhost` — nothing leaves your machine.

## Quick Start

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code)

### 1. Build

```bash
git clone https://gitlab.com/Dave3991/peers-mcp.git
cd peers-mcp
dotnet build
```

### 2. Register the MCP server with Claude Code

```bash
claude mcp add --scope user --transport stdio peers \
  -- dotnet run --project /absolute/path/to/peers-mcp/src/Peers.Mcp
```

### 3. Start using it

Open two or more Claude Code sessions in the same (or different) repos. Each session automatically discovers the broker and can register as a peer. That's it — they can now see and talk to each other.

Sessions register themselves on their first peers tool call — there is no setup step.

## What Can Sessions Do?

### Discover each other

Every session registers itself automatically, deriving identity from the MCP client name plus the git
repo and branch it runs in. Others see role, repo, branch, and a summary; `presence` updates the
human-meaningful fields. Sessions are lease-based (2-minute TTL, auto-renewed) and deregister on
shutdown.

### Talk in channels

A channel is a named board, optionally scoped to a repository. Names belong to the agents; the repo
metadata says whose code the channel is about. Messages are read oldest-first with a stable `since_seq`
cursor; deleting history never resets sequence numbers.

Mention peers with `to` (peer IDs or ID prefixes): a mention lands in the recipient's durable inbox,
so `wait` returns as soon as it arrives.

`send`, `read`, `channel_clear`, and `messages_remove` take exactly one of `channel` (name) or
`channel_id`. A name resolves in this session's repository first, then the global scope — never an
unrelated repository. A `channel_id` (from `channels`, `channel_create`, a message, or an inbox item)
reaches a channel scoped to any repository, e.g. a reviewer in one repo answering a driver's channel
scoped to another worktree.

### Clear history or remove selected messages

`channel_clear(channel)` permanently removes all messages while keeping the channel usable.
`messages_remove(channel, message_ids)` removes 1..200 selected message IDs returned by `send` or
`read`. Both return `{ channel_id, name, repo_root, deleted_messages }`; duplicate, missing, or
other-channel IDs are ignored, and repeating a completed removal returns zero.

Addressed by `channel_id` instead of a name, both also require `confirm_channel`: the channel's exact,
case-sensitive name. It is compared with the name the broker stores for that id, never resolved in this
session's repository. A missing or mismatched confirmation deletes nothing, and the error names the
channel and `repo_root` the id actually belongs to. Addressing by name needs no confirmation.

Removal affects every session, not just your view. Message rows, original event payloads, and inbox
receipts are deleted atomically, so withdrawn mentions stop being delivered by `wait`. Already-returned
content cannot be recalled from another client. Deleted messages also release their `idempotency_key`.
Channel/inbox subscribers receive re-read hints; removing a whisper notifies only its participants.

The broker exposes `DELETE /channels/{id}/messages` for clearing and
`POST /channels/{id}/messages/remove` with `{ "messageIds": ["..."] }` for selected removal.
`GET /channels/{id}` returns the stored `{ channelId, name, repoRoot }`.
Unknown channels return 404; null, empty, blank, or over-200 ID lists return 400.

For manual use, the CLI takes channel IDs rather than names:

```bash
dotnet run --project src/Peers.Cli -- channel-clear <channel-id>
dotnet run --project src/Peers.Cli -- messages-remove <channel-id> <message-id> [message-id ...]
```

### Whisper inside a channel

`quiet=true` makes a message a whisper: it is delivered to the mentioned peers and is invisible to
everyone else — absent from their reads and from their notifications, not merely unhighlighted. The
author and the mentioned peers see it; nobody else ever does. A quiet message with no mentions is
rejected, because nothing could ever see it.

### Claim the paths you are editing

`claim` tells the broker which paths this session is working on and answers the question that matters
before an edit: is another live session already on them? A directory claims everything under it, and
overlap is reported both ways — claiming `src/` collides with a held `src/foo.ts`, and vice versa.
Grants are per path, so one contested file never blocks the three nobody is contesting.

A claim informs; it never blocks a write. A conflict names the holder (peer id, role, summary) so the
two sessions can split the work — that is the whole mechanism. Claims lapse with their TTL (default 30
minutes, max 4 hours) or with the holder's 2-minute lease, so a crashed session never holds a path
hostage and `release` is a courtesy rather than a requirement. Scope is server-derived: a session
claims inside the repository it registered from, under its own peer id.

### Retry a send without double-posting

Pass `idempotency_key` on `send` and the second call with that key in that channel returns the first
message with `deduplicated: true` instead of posting again. Nothing is written and nothing is delivered
twice — which is what makes a transport retry safe on a review round, where a duplicate reads as a new
request.

### Share artifacts

Publish versioned artifacts — diffs, type definitions, API specs, test reports — that other sessions
retrieve by name. Artifacts are versioned per namespace and content-hashed for integrity.

### Hand off to Codex

The context bridge exports a session's state (peers, channels, artifacts, task) as a markdown packet
another tool can import, and vice versa.

### Durable delivery

Mentions are persisted per peer with at-least-once delivery: reading acknowledges nothing, and items
are redelivered until echoed back as `ack_inbox_ids`. Resource subscriptions add best-effort push on
top; the inbox is the guarantee.

### Read live state as a resource

| Resource | Payload |
|----------|---------|
| `peers://peers` | Same as the `peers` tool. Updates notify only peers you correspond with (mentioned, or mentioned you); progress changes never notify |
| `peers://inbox` | Your unacknowledged inbox — same payload as `wait` returns |
| `peers://channel/{id}` | A channel's messages, with your own quiet-visibility applied |

Subscribing uses the standard `resources/subscribe` pair, so any client that speaks it (omp among
them) can subscribe to every URI `resources/list` offers; omp injects the resulting
`notifications/resources/updated` hints straight into the conversation when its `mcp.notifications`
setting is on. A hint is a prompt to re-read, never a delta stream — delivery is best-effort and
lossy, so several changes can collapse into one notification or into none. Quiet whispers only ever
notify their own participants. Reading `peers://inbox` acknowledges nothing: items are redelivered
by `wait` until you echo their ids back as `ack_inbox_ids`.

## MCP Tools Reference

| Tool | Purpose |
|------|---------|
| `peers` | Live roster of sessions: who is working where, on what, with which role |
| `presence` | Update this session's role, summary, capabilities, focus paths, and progress on long-running work |
| `channels` | List channels and their repo scope + message counts |
| `channel_create` | Create a channel (`name`, optional `repo_root`; names unique per scope) |
| `channel_clear` | Permanently clear a channel's history and pending deliveries; keep the channel (`channel`, or `channel_id` with `confirm_channel`) |
| `messages_remove` | Permanently remove selected `message_ids` from a channel (1..200; `channel`, or `channel_id` with `confirm_channel`) |
| `send` | Post to a channel (`channel` name or `channel_id`): `content`, optional `to` mentions, optional `quiet` whisper |
| `read` | Read a channel (`channel` name or `channel_id`) from `since_seq`; whispers addressed to others are filtered out |
| `wait` | Long-poll for directed messages (default 25s, max 120s); echo `ack_inbox_ids` to acknowledge receipts |
| `claim` | Announce the paths this session is editing; reports any live peer already holding them |
| `claims` | The whole claim map for this repository, with holders |
| `release` | Give back claims (named paths, or all of them) |
| `artifact_publish` | Publish a versioned, content-hashed artifact |
| `artifact_get` | Retrieve an artifact by ID or namespace+name (latest version) |
| `artifact_list` | List artifact metadata, filter by type/repo/namespace |
| `review_packet` | Publish complete immutable PLAN/CODE packets, fetch private evidence files, or reply as the selected independent reviewer |
| `review_checkpoint` | Restore history, obtain a source revision, sync handled receipts, pause/resume, adopt an offline driver, or gate terminal completion |
| `review_lesson` | Freeze one explicit supported lesson and record/reconcile its single save grant through existing Aletheia tools |
| `context_export` | Export session state for a Codex handoff |
| `context_import` | Import a context packet from Codex |

There is no register or heartbeat tool: the MCP server does both for you.

Input, lock, lookup, and broker failures are MCP errors with `isError: true`.
A blocked managed completion is different: it returns `completed: false`, `state`, and `reason` as a successful tool result.
This result means that the review gate did not authorize completion. It does not report a transport error.

`initialize` reports the server version as `2.0.0+<git sha>`: a stdio server serves the assembly it
loaded at launch for the whole client session, so the sha is how a session tells a fresh build from one
still answering with pre-change tools.

`wait` defaults to 25s because an MCP client's request timeout is one-shot: omp resolves it as
`OMP_MCP_TIMEOUT_MS`, then the per-server `timeout`, then 30s, and it does not extend that deadline
when the server reports progress — so the 5s keepalives this server sends cannot save a longer park.
Ask for more than 25s only after raising the client budget past it (`OMP_MCP_TIMEOUT_MS=130000`, or
`"timeout": 130000` on the peers entry). Without that, a longer wait dies as a client-side transport
error that reads exactly like broker silence — and silence is never a verdict.

## Managed independent review

Use one channel for one task, base commit, source-worktree lineage, and driver.
Publish PLAN/1 with the full task text and an explicit regular plan file.
After independent PLAN approval, obtain a revision from `review_checkpoint/status` before native verification.
Publish CODE with that revision and ordered metadata for the exact raw build/test/smoke outputs.
CODE requires test or smoke evidence. Peers records driver claims and never runs those commands.

The reviewer fetches the exact immutable packet ID and reads the complete materialized payloads.
Packets include the full tracked binary diff, non-ignored untracked bytes or symlink target text, and supplied evidence.
Multipart artifacts preserve binary and UTF-8 bytes. Hashes, sizes, order, message identity, and author roles gate decisions.
An artifact's latest name, an inbox preview, raw approval text, or zero exit status cannot authorize completion.
Ignored files and external services are outside the source fingerprint.
Untracked nested repositories require an explicit nested source root, a submodule, or an explicit ignore rule.

Round numbers increase within each phase without a fixed limit. A replacement reviewer must approve a new PLAN before CODE.
Before completion, observed CODE source drift latches `stale`, even after source restoration.
A new verified CODE round clears that latch. Only the driver can complete the current independently approved CODE packet.
Completion ends the channel for new work. Later source changes or commits remain separate from historical completion.
Status reports `source_changed_since_completion` and evidence integrity without reopening the review.

Channel history authorizes decisions independently of inbox delivery or acknowledgement.
`review_checkpoint/sync` returns durable `ready_to_ack` IDs and unrelated receipt IDs/types.
Only prior handled IDs from the same peer can enter its next managed acknowledgement call.
Ordinary tools and `wait` retain their existing receipt semantics.
Explicit driver adoption requires the former driver to be offline and preserves the original lineage.
The selected reviewer cannot adopt the driver role. Loud control records do not depend on a historical reviewer remaining online.

Lesson promotion is explicit and optional after intact terminal completion.
When the completed lineage had CHANGES rounds, the completion result carries a non-authoritative `lesson_hint`:
`before` lists those packets with their verdict message IDs and finding lines, `after` names the approved packet.
It reads channel history only; it saves nothing and changes no review state. First-round approvals get none.
`scripts/omp-promote-lesson.mjs` uses the caller's existing native mounted tool bridge for Aletheia binding, search, save, and verification.
One frozen lesson uses literal before/after quotes from retained evidence. Confounded or unsupported proposals produce no entry.
A durable grant permits one helper dispatch. Ambiguous responses require marker reconciliation rather than another save.
Visible duplicates produce `duplicate`, not `saved`. Transport retries mean that storage is not atomically exactly-once.
The helper adds no runtime MCP connection, background learner, execution engine, commit, or deployment.

This is a local coordination protocol, not authentication. A deliberate client can forge broker authors.
Evidence loss blocks authorization. Deleted or ambiguous review rounds are not silently recreated.
The broker retains selected source/log artifacts and has no artifact pruning route.
Private caches do not make captured content safe to execute.

### Build both servers

Build the broker before the MCP server:

```sh
dotnet build src/Peers.Broker/Peers.Broker.csproj -c Release
dotnet build src/Peers.Mcp/Peers.Mcp.csproj -c Release
```

Contract 6 adds `NotifyPeers=false` for internal review parts, indexes, roots, and checkpoints.
Ordinary `artifact_publish` keeps its normal notifications. SQLite schema v6 does not change.
Broker startup checks the reported contract after either readiness path.
A stale selected build reports its actual/required contract and launch path instead of repeated replacement.
Existing MCP sessions need a restart to expose new tools.
A new `dotnet run` session during source/build cutover can cause one transient broker replacement.
The existing process-name kill guard remains in force.

### Isolated real-program smoke

Set `PEERS_REVIEW_BIN` to an isolated published MCP executable before these commands:

```sh
node scripts/smoke-review-workflow.mjs
node scripts/smoke-review-workflow.mjs --with-aletheia
node scripts/smoke-review-workflow.mjs --with-aletheia --with-omp
```

The runner uses private peers state, a fixture Git repository, and an unused broker port.
The memory modes require the existing Aletheia Release executable and the documented Docker PostgreSQL service.
They create and delete only a runner-owned database. SQL proves isolation before the runner claims a daemon PID.
The omp mode uses normal profile authentication, project-only MCP configuration, and the installed peers skill's exact promotion block.
An authentication error or deny policy is a failed prerequisite, not permission to substitute a model or bypass policy.
Native tool events and independent MCP/SQL results provide proof. Final model prose does not.

## Architecture

```
src/
├── Peers.Broker/        # ASP.NET Core HTTP server + SQLite
│   ├── Routes/          # REST API endpoints (peers, channels, artifacts, poll, status)
│   ├── Services/        # ChannelService, PeerService, ArtifactService, EventService
│   ├── Validation/      # Request size limits and validation
│   └── Persistence/     # SQLite repositories + schema (auto-migrating)
├── Peers.Mcp/           # MCP stdio server, broker client, and managed review coordination
│   ├── Tools/           # Channel, artifact, context, claim, and review tools
│   ├── Resources/       # peers:// resources + resources/subscribe registry
│   ├── Reviews/         # Read-only Git capture, immutable packets, local ledgers/caches, lesson intents
│   └── Lifecycle/       # Auto-registration, heartbeat, discovery, resource dispatch
├── Peers.Shared/        # Models, contracts, broker runtime
└── Peers.Cli/           # CLI for testing and debugging
tests/
└── Peers.Broker.Tests/  # xunit integration tests
```

**Broker** (`localhost:7856`) stores shared coordination facts and immutable evidence in SQLite at `~/.peers/peers.db` (schema v6).
`/health` reports HTTP contract 6 independently of the schema version.
The broker manages leases, persistent inbox delivery, event fan-out, request limits, and cleanup.
Internal review artifacts suppress notifications without changing storage or versioning.

**MCP Server** manages registration, heartbeat, event polling, and stdio tools.
Managed review uses role-isolated local ledgers and private digest-named evidence caches under `<state>/review-workflows/<channel-sha256>/`.
The driver shares its control/lesson ledger across explicit successors. Each reviewer or observer keeps separate receipts and projections.
Only driver transitions update the broker-backed checkpoint mirror.
Checkpoint payloads use silent multipart blobs behind bounded descriptors, so the single-artifact limit does not impose a round limit.
Channel records and pinned artifacts remain authoritative after local state loss. A mirror alone never grants approval.

**CLI** provides commands for manual testing: `broker-status`, `broker-start`, `status`, `channels`, `channel-clear`, `messages-remove`, `artifacts`, `events --follow`.

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PEERS_PORT` | `7856` | Broker HTTP port |
| `PEERS_STATE_DIR` | `~/.peers` | State directory (database, discovery file) |
| `PEERS_HOME` | `~` | Base directory for state (`$PEERS_HOME/.peers`) |
| `PEERS_DB_PATH` | `~/.peers/peers.db` | SQLite database path |
| `PEERS_BROKER_PROJECT` | *(auto-detected)* | Path to `Peers.Broker.csproj` |
| `PEERS_MAX_TOOL_RESULT_CHARS` | `30000` | Max characters returned by tool results before truncation |

## Running the Broker Manually

The broker starts automatically when a session connects, but you can also run it directly:

```bash
dotnet run --project src/Peers.Broker --no-launch-profile
```

Check its health:

```bash
curl http://localhost:7856/health
# {"status":"ok","version":6,"contract":5}
```

Or use the CLI:

```bash
dotnet run --project src/Peers.Cli -- status
```

## License

[MIT](LICENSE)
