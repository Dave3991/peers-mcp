# Peers

**Let your AI coding sessions talk to each other.**

![Peers demo](demo.gif)

Peers is a local coordination server that connects multiple [Claude Code](https://docs.anthropic.com/en/docs/claude-code) and [Codex](https://openai.com/index/introducing-codex/) sessions so they can discover each other, exchange work, and collaborate through shared scratchpads — all on your machine, no cloud required.

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

For real-time push notifications between sessions, start Claude Code with:

```bash
claude --dangerously-load-development-channels server:peers
```

Without this flag, sessions can still communicate using the pull-based `peer_inbox` tool.

## What Can Sessions Do?

### Discover each other

Every session registers with a role (`implementing`, `reviewing`, `debugging`, `spec`), its repo, branch, and a summary of what it's working on. Other sessions can list active peers and see who's doing what. Sessions are lease-based (2-minute TTL, auto-renewed) and automatically deregister on shutdown. Reconnecting sessions can resume their previous identity.

### Collaborate through scratchpads

Scratchpads are shared, append-only documents where sessions exchange thoughts asynchronously. Create a review scratchpad, tag a peer as reviewer, and they'll be notified with delivery status (online/offline/unresolved). Quiet appends let you add entries without triggering push notifications. Types include `review`, `discussion`, `spec`, and `context`.

### Share artifacts

Publish versioned artifacts — diffs, type definitions, API specs, test reports — that other sessions can retrieve by name. Artifacts are automatically versioned per namespace and content-hashed for integrity. Large results are automatically truncated with guidance on pagination.

### Hand off to Codex

The context bridge lets you export a session's full state (peers, scratchpads, artifacts, task description) as a structured markdown packet that Codex can import, and vice versa.

### Durable message delivery

Directed messages are persisted in a per-peer inbox with at-least-once delivery semantics. If a session disconnects and resumes, it picks up where it left off — no messages lost. Push notifications provide real-time delivery when available, with the inbox as a reliable fallback.

### Read live state as a resource

Two MCP resources mirror the tools, so a session can read state directly instead of calling a tool:

| Resource | Payload |
|----------|---------|
| `peers://peers` | Same as `peer_list` |
| `peers://scratchpad/{id}` | Same as `scratchpad_read` |

Both are inert until `peer_register`, and readable on every MCP revision. Subscribing needs MCP 2026-07-28 (`subscriptions/listen`); the older `resources/subscribe` is refused with an error rather than silently accepted. A `notifications/resources/updated` is a hint to re-read, never a delta stream — delivery is best-effort and lossy, so several changes can collapse into one notification or into none.

## MCP Tools Reference

Once registered, each session has access to these tools:

| Tool | Purpose |
|------|---------|
| `peer_register` | Join the network with your role, repo, and summary. Supports session resumption; accepts `worktree_root` (linked-worktree identity key) and `capabilities` tags |
| `peer_heartbeat` | Update your status and renew your session lease; `capabilities` is updatable |
| `peer_list` | See all active sessions, filter by repo/branch/tool |
| `peer_inbox` | Read messages directed to you (pull mode, durable). At-least-once: items are redelivered until you acknowledge them by echoing their `inbox_id`s as `ack_inbox_ids` |
| `peer_wait` | Long-poll for directed messages — blocks until a message arrives or the timeout expires. Same echo-to-acknowledge contract as `peer_inbox` |
| `peer_poll` | Low-level event polling with cursor |
| `peer_test_push` | Send a test push notification to verify push delivery |
| `scratchpad_create` | Start a shared review, discussion, or spec. Shows recipient delivery status |
| `scratchpad_append` | Add an entry to a scratchpad. Supports quiet mode to suppress push |
| `scratchpad_read` | Read scratchpad entries with pagination |
| `scratchpad_list` | List scratchpads, filter by type/status/repo |
| `scratchpad_resolve` | Mark a scratchpad as resolved without closing it |
| `scratchpad_close` | Close a scratchpad with an optional resolution summary |
| `artifact_publish` | Publish a versioned, content-hashed artifact |
| `artifact_get` | Retrieve an artifact by ID or by name (latest version) |
| `artifact_list` | List artifact metadata, filter by type/repo/namespace |
| `context_export` | Export session state for Codex handoff |
| `context_import` | Import a context packet from Codex |

## Architecture

```
src/
├── Peers.Broker/        # ASP.NET Core HTTP server + SQLite
│   ├── Routes/          # REST API endpoints
│   ├── Services/        # Business logic
│   ├── Validation/      # Request size limits and validation
│   └── Persistence/     # SQLite repositories + schema (auto-migrating)
├── Peers.Mcp/           # MCP stdio server (thin broker client)
│   ├── Tools/           # MCP tool implementations
│   └── Lifecycle/       # Heartbeat, discovery, push notifications
├── Peers.Shared/        # Models, contracts, broker runtime
└── Peers.Cli/           # CLI for testing and debugging
tests/
└── Peers.Broker.Tests/  # xunit integration tests
```

**Broker** (`localhost:7856`) manages all state in SQLite at `~/.peers/peers.db`. It exposes a REST API, handles peer leases (2-minute TTL, auto-renewed), persistent inbox delivery, event fan-out, request validation, and cleanup.

**MCP Server** is a thin stdio wrapper that translates MCP tool calls into broker HTTP requests. It manages the session lifecycle — heartbeat every 30 seconds, event polling every 5 seconds, push notification delivery, and deregistration on shutdown.

**CLI** provides commands for manual testing: `status`, `register`, `scratchpads`, `artifacts`, `poll`.

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
# {"status":"ok","version":1}
```

Or use the CLI:

```bash
dotnet run --project src/Peers.Cli -- status
```

## License

[MIT](LICENSE)
