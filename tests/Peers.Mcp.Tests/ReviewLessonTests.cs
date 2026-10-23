using System.Text.Json;
using ModelContextProtocol;
using Peers.Mcp.Reviews;
using Xunit;

namespace Peers.Mcp.Tests;

public sealed class ReviewLessonTests
{
    [Fact]
    public void Binding_freezes_actual_identity_without_upgrading_low_confidence_or_silently_rebinding()
    {
        var intent = new ReviewLessonIntent();
        ReviewWorkflow.FreezeBinding(intent, JsonSerializer.SerializeToElement(new { workspaceBindingId = "wb_first", identityConfidence = "low", bindingStatus = "observed", projectIdentityHash = (string?)null }));
        Assert.Equal("wb_first", intent.WorkspaceBindingId);
        Assert.Equal("low", intent.Binding!.Value.GetProperty("identityConfidence").GetString());
        Assert.Throws<McpException>(() => ReviewWorkflow.FreezeBinding(intent, JsonSerializer.SerializeToElement(new { workspaceBindingId = "wb_other", identityConfidence = "high", bindingStatus = "observed" })));
        Assert.Equal("wb_first", intent.WorkspaceBindingId);
    }

    [Theory]
    [InlineData("{\"error\":\"BIND_FAILED\"}")]
    [InlineData("{\"workspaceBindingId\":\"wb_x\",\"identityConfidence\":\"low\",\"bindingStatus\":\"quarantined\"}")]
    [InlineData("{\"workspaceBindingId\":\"wb_x\",\"bindingStatus\":\"observed\"}")]
    public void Malformed_or_quarantined_bindings_never_freeze(string json)
    {
        var intent = new ReviewLessonIntent();
        Assert.Throws<McpException>(() => ReviewWorkflow.FreezeBinding(intent, JsonDocument.Parse(json).RootElement));
        Assert.Null(intent.WorkspaceBindingId);
    }

    [Fact]
    public void Observation_matching_is_ordinal_and_requires_binding_scope_body_and_exact_tag_set()
    {
        var intent = new ReviewLessonIntent { WorkspaceBindingId = "wb_fixture", Scope = "project", Body = "Čeština 🧪\nLesson", Tags = ["peers-lesson:marker", "source:peers"] };
        JsonElement Observation(string binding, string scope, string body, string[] tags) => JsonSerializer.SerializeToElement(new { observationId = "obs_fixture", workspaceBindingId = binding, profileScope = scope, body, tags });
        Assert.True(ReviewWorkflow.ExactObservation(intent, Observation("wb_fixture", "project", intent.Body, intent.Tags.Reverse().ToArray())));
        Assert.False(ReviewWorkflow.ExactObservation(intent, Observation("wb_other", "project", intent.Body, intent.Tags)));
        Assert.False(ReviewWorkflow.ExactObservation(intent, Observation("wb_fixture", "user", intent.Body, intent.Tags)));
        Assert.False(ReviewWorkflow.ExactObservation(intent, Observation("wb_fixture", "project", intent.Body + "\n", intent.Tags)));
        Assert.False(ReviewWorkflow.ExactObservation(intent, Observation("wb_fixture", "project", intent.Body, ["PEERS-LESSON:marker", "source:peers"])));
        Assert.False(ReviewWorkflow.ExactObservation(intent, Observation("wb_fixture", "project", intent.Body, [.. intent.Tags, "extra"])));
        Assert.Throws<McpException>(() => ReviewWorkflow.ExactObservation(intent, JsonSerializer.SerializeToElement(new { error = "OBSERVATION_NOT_FOUND", observationId = "obs_fixture" })));
    }
}
