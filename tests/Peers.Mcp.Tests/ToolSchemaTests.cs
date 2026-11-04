using System.Text.Json;
using Xunit;

namespace Peers.Mcp.Tests;

// Observed 2026-10-27 and 2026-11-04: review_packet advertised exit_code as "integer or null" while the
// tool rejects null, so agents sent null for an unknown status. Every field that a published schema lists
// as required is held to a non-null type, across every tool of the full profile.
public sealed class ToolSchemaTests
{
    [Fact]
    public void Required_fields_are_never_advertised_as_nullable()
    {
        var nullable = new List<string>();
        foreach (var tool in ProductionServer.Tools(allTools: true))
            CollectNullableRequired(tool.ProtocolTool.InputSchema, tool.ProtocolTool.Name, nullable);
        Assert.Empty(nullable);
    }

    private static void CollectNullableRequired(JsonElement schema, string path, List<string> nullable)
    {
        if (schema.ValueKind != JsonValueKind.Object) return;
        if (schema.TryGetProperty("items", out var items)) CollectNullableRequired(items, path + "[]", nullable);
        if (!schema.TryGetProperty("properties", out var properties)) return;
        var required = schema.TryGetProperty("required", out var names)
            ? names.EnumerateArray().Select(name => name.GetString()).ToHashSet()
            : [];
        foreach (var property in properties.EnumerateObject())
        {
            var field = path + "." + property.Name;
            if (required.Contains(property.Name) && property.Value.TryGetProperty("type", out var type) &&
                (type.ValueKind == JsonValueKind.Array ? type.EnumerateArray().Any(t => t.GetString() == "null") : type.GetString() == "null"))
                nullable.Add(field);
            CollectNullableRequired(property.Value, field, nullable);
        }
    }
}
