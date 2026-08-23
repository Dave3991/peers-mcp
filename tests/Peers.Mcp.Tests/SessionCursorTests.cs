using System.Net;
using System.Text;
using System.Text.Json;
using Peers.Mcp;
using Peers.Mcp.Tools;
using Xunit;

namespace Peers.Mcp.Tests;

/// <summary>
/// The session cursor has three concurrent writers — the channel notifier's poll loop, peer_poll, and
/// the heartbeat's self-heal re-register — and exactly one invariant: it never moves backward except at
/// peer_register, where a new identity legitimately resumes at its own cursor. A regression here is not
/// visible at the call site that causes it: the cursor is shared, so the NEXT poll re-delivers
/// everything in between as duplicate pushes.
/// </summary>
public class SessionCursorTests
{
    private sealed class PollHandler(long cursor, params long[] eventIds) : HttpMessageHandler
    {
        public string? LastQuery { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            LastQuery = request.RequestUri!.Query;
            var events = string.Join(",", eventIds.Select(id =>
                $$"""{"eventId":{{id}},"type":"peer.registered","payload":"{}","createdAt":"2026-01-01T00:00:00Z"}"""));

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    $$"""{"events":[{{events}}],"cursor":{{cursor}}}""", Encoding.UTF8, "application/json"),
            });
        }
    }

    [Fact]
    public void AdvanceCursor_MovesForwardOnly()
    {
        var state = new SessionState();
        state.ResetCursorForRegistration(10);

        state.AdvanceCursor(4);
        Assert.Equal(10, state.LastCursor);

        state.AdvanceCursor(10);
        Assert.Equal(10, state.LastCursor);

        state.AdvanceCursor(11);
        Assert.Equal(11, state.LastCursor);
    }

    /// <summary>
    /// peer_poll used to ASSIGN the cursor it polled to, so a diagnostic replay from an explicit low
    /// cursor rewound the whole session — including the background notifier, which then re-pushed every
    /// event in between. Replay itself has to keep working: the older events are still returned.
    /// </summary>
    [Fact]
    public async Task PeerPoll_ReplayingFromAnExplicitCursor_ReturnsTheEvents_ButDoesNotRewindTheSession()
    {
        var handler = new PollHandler(cursor: 3, eventIds: [2, 3]);
        using var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        state.ResetCursorForRegistration(10);

        var json = await PollTools.Poll(client, state, cursor: 0);

        Assert.Contains("cursor=0", handler.LastQuery);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(2, doc.RootElement.GetProperty("events").GetArrayLength());
        Assert.Equal(10, state.LastCursor);
    }

    [Fact]
    public async Task PeerPoll_WithoutAnExplicitCursor_ResumesFromTheSession_AndAdvancesIt()
    {
        var handler = new PollHandler(cursor: 14, eventIds: [12, 14]);
        using var client = new BrokerClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var state = new SessionState { PeerId = "p" };
        state.ResetCursorForRegistration(10);

        await PollTools.Poll(client, state);

        Assert.Contains("cursor=10", handler.LastQuery);
        Assert.Equal(14, state.LastCursor);
    }
}
