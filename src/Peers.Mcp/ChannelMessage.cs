// ponytail: MCP9005 disabled file-wide. The MCP Logging feature (LoggingLevel,
// notifications/message) is deprecated as of spec revision 2026-07-28 (SEP-2577), but it is
// still the only push path for clients on 2025-11-25 and earlier — which today is all of them.
// Scoped to this file rather than <NoWarn> in the csproj so the NEXT MCP9005 elsewhere is still
// reported. Remove the pragma, and this push path, once no supported client predates 2026-07-28.
#pragma warning disable MCP9005

using ModelContextProtocol.Protocol;

namespace Peers.Mcp;

public sealed record PendingChannelMessage
{
    public required string MessageId { get; init; }
    public required long EventId { get; init; }
    public required string EventType { get; init; }
    public required DateTime CreatedAt { get; init; }
    public required string Content { get; init; }
    public required LoggingLevel Level { get; init; }
    public string? ScratchpadId { get; init; }
    public string? Title { get; init; }
    public string? FromPeerId { get; init; }
};

/// <summary>What a push attempt actually did. peer_test_push exists to answer "is push wired up?", so it
/// reports this instead of assuming the attempt landed.</summary>
public enum PushOutcome
{
    /// <summary>Delivered as <c>notifications/message</c>.</summary>
    LoggingMessage,

    /// <summary>Delivered as <c>notifications/claude/channel</c>.</summary>
    ClaudeChannel,

    /// <summary>Nothing was sent, and nothing would be: the negotiated revision has no conforming
    /// unsolicited push. Not a failure to retry — pull-mode <c>peer_inbox</c> covers delivery.</summary>
    SuppressedByProtocolRevision,

    /// <summary>A push method was available and every attempt threw.</summary>
    Failed,
}

/// <summary><paramref name="Detail"/> is client-facing prose: safe to surface verbatim from a tool result.</summary>
public readonly record struct PushResult(PushOutcome Outcome, string Detail)
{
    public bool Delivered => Outcome is PushOutcome.LoggingMessage or PushOutcome.ClaudeChannel;
}
