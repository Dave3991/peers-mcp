# Peers — Multi-Session AI Coordination

## What this is

A local MCP server that lets multiple Claude Code (and Codex) sessions discover each other, share context, exchange artifacts, and review each other's work via shared scratchpads.

## Build & Run

```bash
dotnet build                                              # build everything
dotnet run --project src/Peers.Broker --no-launch-profile # start broker
dotnet run --project src/Peers.Cli -- status              # check status
```

## MCP Setup

```bash
claude mcp add --scope user --transport stdio peers -- dotnet run --project /path/to/peers/src/Peers.Mcp
```

For visible push-style channel messages in Claude Code, start Claude with:

```bash
claude --dangerously-load-development-channels server:peers
```

If you start Claude normally, directed peer messages are still available through the `peer_inbox` tool.

## Peer Workflow Contract

When participating in a multi-session workflow with this MCP server:

1. **On startup**: Call `peer_register` with your repo, branch, role, and summary, and worktree_root if working in a linked git worktree
2. **Check inbox regularly**: Call `peer_inbox` at startup, after completing a task chunk, before starting new work, and before finishing. Acknowledge what you have handled by passing the previous result's `inbox_id`s as `ack_inbox_ids` — anything you do not echo is delivered again. Use `peer_poll` only when you need raw event payloads or cursor debugging.
3. **Review requests**: If a review scratchpad targets your repo, prioritize reading and responding
4. **Share artifacts**: Publish meaningful outputs (types, specs, diffs) as artifacts instead of pasting large blobs into scratchpads
5. **Update status**: Call `peer_heartbeat` when your role or focus changes
6. **Context bridge**: Use `context_export` before handing off to Codex, and `context_import` when receiving from Codex
7. **Resources**: `peers://peers` (same payload as `peer_list`) and `peers://scratchpad/{id}` (same as `scratchpad_read`) are readable on any MCP revision and inert until `peer_register`. Subscribing needs MCP 2026-07-28 (`subscriptions/listen`); `resources/updated` is a hint to re-read, never a delta stream

## Architecture

- `src/Peers.Broker` — ASP.NET Core HTTP server + SQLite (localhost:7856)
- `src/Peers.Mcp` — MCP stdio server (thin wrapper over broker API)
- `src/Peers.Shared` — Models and contracts
- `src/Peers.Cli` — CLI for testing without live AI sessions
