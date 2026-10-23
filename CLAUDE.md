# Peers — Multi-Session AI Coordination

## What this is

A local MCP server that lets multiple Claude Code (and Codex) sessions discover each other, share context, exchange artifacts, and review each other's work on shared channels.

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

If you start Claude normally, directed peer messages are still available through the `wait` tool.

## Peer Workflow Contract

The whole agent-facing surface, and the only tools that exist:

`peers` · `presence` · `channels` · `channel_create` · `channel_clear` · `messages_remove` · `send` · `read` · `wait` ·
`claim` / `claims` / `release` · `artifact_publish` / `artifact_get` / `artifact_list` · `context_export` / `context_import` ·
`review_packet` / `review_checkpoint` / `review_lesson`

`channel_clear` and `messages_remove` permanently delete shared history and its inbox deliveries.
Clearing keeps the channel; neither operation resets message sequences. Already-read content cannot
be recalled from clients. Addressed by `channel_id`, both also require `confirm_channel`: the channel's
exact name, checked against the name the broker stores for that id. A missing or wrong one deletes nothing.

When participating in a multi-session workflow with this MCP server:

1. **There is no registration step**: your first tool call registers this session automatically, deriving identity from the client name and the git repo/branch. Shape how you appear to others with `presence` (role, summary, capabilities, focus paths, and progress on long-running work)
2. **Check for directed messages**: `wait` blocks until a mention lands (default 25s) and returns immediately when one does. Acknowledge what you handled by passing the previous result's `inbox_id`s as `ack_inbox_ids` — anything you do not echo is delivered again
3. **Review requests**: a review thread is a channel — `channel_create` one per thread, then `send` each round with `to` naming the reviewer. If a channel scoped to your repo mentions you, prioritize reading and responding. Channel names resolve only in your repo or the global scope; address a channel scoped to another repo by its `channel_id` (from the mention's inbox payload or `channels`)
4. **Share artifacts**: Publish meaningful outputs (types, specs, diffs) as artifacts instead of pasting large blobs into channel messages
5. **Report long work**: call `presence` with `progress_current` / `progress_total` as you go. Progress is PULL-only — it never notifies anyone. Peers see it by calling `peers`, by re-reading `peers://peers`, or on a `wait` keepalive while they are parked. Omit `progress_total` for an open-ended count. There is no clear step — signal completion with `progress_current = progress_total`, otherwise a finished task keeps reading as its last count (annotated with its age once stale)
6. **Context bridge**: Use `context_export` before handing off to Codex, and `context_import` when receiving from Codex
7. **Resources**: `peers://peers` (same payload as `peers`) and `peers://inbox` (same as the `wait` tool) are readable on any MCP revision and inert until registration. Subscribing uses the standard `resources/subscribe` pair; `resources/updated` is a hint to re-read, never a delta stream. Reading `peers://inbox` acknowledges nothing — ack only via `ack_inbox_ids`
8. **Who a roster hint reaches**: `peers://peers` only notifies peers you CORRESPOND with — someone you have mentioned, or who has mentioned you. A stranger's presence change, or anyone registering or expiring, never wakes you; those still arrive durably in the inbox where they belong. Progress never notifies at all

## Architecture

- `src/Peers.Broker` — ASP.NET Core HTTP server + SQLite (localhost:7856)
- `src/Peers.Mcp` — MCP stdio server, broker client, private review ledgers, and immutable evidence capture
- `src/Peers.Shared` — Models and contracts
- `src/Peers.Cli` — CLI for testing without live AI sessions
