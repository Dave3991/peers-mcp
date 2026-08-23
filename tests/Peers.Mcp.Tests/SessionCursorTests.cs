using Peers.Mcp;
using Xunit;

namespace Peers.Mcp.Tests;

/// <summary>
/// The session cursor's writers — the channel notifier's poll loop and the heartbeat's self-heal
/// re-register — share one invariant: it never moves backward except at registration, where a new
/// identity legitimately resumes at its own cursor. A regression here is not visible at the call site
/// that causes it: the cursor is shared, so the NEXT poll re-delivers everything in between as
/// duplicate pushes.
/// </summary>
public class SessionCursorTests
{

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
}

