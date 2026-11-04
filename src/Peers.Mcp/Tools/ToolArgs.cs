using Peers.Mcp.Reviews;

namespace Peers.Mcp.Tools;

/// <summary>
/// Checks for the tools that take an <c>action</c>. Each error names the field and what the action
/// needs: an agent can fix only a call whose error says which field to change.
/// </summary>
internal static class ToolArgs
{
    /// <summary>The action, or an error that lists every allowed action.</summary>
    internal static string RequireAction(string? action, string[] allowed) =>
        ReviewJson.RequireOneOf("action", action, allowed);

    /// <summary>A field that <paramref name="action"/> requires must be present; one it takes no part in must be absent.</summary>
    internal static void RequirePayload(string action, string field, bool present, bool required) =>
        ReviewJson.Require(present == required, required
            ? $"{field} is missing; action \"{action}\" requires it."
            : $"{field} is set, but action \"{action}\" takes no {field}; omit it.");
}
