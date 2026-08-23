using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Peers.Broker.Hosting;
using Peers.Broker.Persistence;
using Peers.Shared.Contracts;
using Xunit;

namespace Peers.IntegrationTests;

public sealed class BrokerFlowTests : IAsyncLifetime
{
    private WebApplication _app = null!;
    private HttpClient _http = null!;
    private string _dbPath = null!;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    public async Task InitializeAsync()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"peers_it_{Guid.NewGuid():N}.db");

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        BrokerHost.AddBrokerServices(builder, _dbPath);

        _app = builder.Build();
        BrokerHost.MapBrokerRoutes(_app);

        await _app.StartAsync();
        _http = _app.GetTestClient();
    }

    public async Task DisposeAsync()
    {
        try
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
        finally
        {
            foreach (var suffix in new[] { "", "-wal", "-shm" })
            {
                try { File.Delete(_dbPath + suffix); }
                catch { /* best-effort */ }
            }
        }
    }

    // -------------------------------------------------------------------------
    // 1. Health + Status  (PollRoutes + StatusRoutes)
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Health_Returns200WithSchemaVersion()
    {
        var resp = await _http.GetAsync("/health");
        resp.EnsureSuccessStatusCode();

        var doc = await resp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        Assert.NotNull(doc);
        var version = doc!.RootElement.GetProperty("version").GetInt32();
        Assert.Equal(Schema.CurrentVersion, version);
    }

    [Fact]
    public async Task Status_Returns200()
    {
        var resp = await _http.GetAsync("/status");
        resp.EnsureSuccessStatusCode();

        var doc = await resp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        Assert.NotNull(doc);
        Assert.Equal("ok", doc!.RootElement.GetProperty("status").GetString());
    }

    // -------------------------------------------------------------------------
    // 2. Peer registration  (PeerRoutes + PeerService)
    // -------------------------------------------------------------------------
    [Fact]
    public async Task RegisterTwoPeers_ReturnDistinctIds()
    {
        var peerA = await RegisterPeerAsync("machine-a");
        var peerB = await RegisterPeerAsync("machine-b");

        Assert.False(string.IsNullOrEmpty(peerA));
        Assert.False(string.IsNullOrEmpty(peerB));
        Assert.NotEqual(peerA, peerB);
    }

    // -------------------------------------------------------------------------
    // 3-5. Scratchpad flow  (ScratchpadRoutes + ScratchpadService + RecipientResolver
    //      + InboxRepository + EventService)
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ScratchpadFlow_CreateAppendInbox()
    {
        var peerA = await RegisterPeerAsync("m-a");
        var peerB = await RegisterPeerAsync("m-b");

        // 3. Create scratchpad as A, requesting B as reviewer
        var createReq = new CreateScratchpadRequest
        {
            Type = "review",
            Title = "T15 smoke",
            CreatedByPeerId = peerA,
            RequestedReviewers = JsonSerializer.Serialize(new[] { peerB }),
        };
        var createResp = await _http.PostAsJsonAsync("/scratchpads", createReq, JsonOpts);
        Assert.Equal(HttpStatusCode.OK, createResp.StatusCode);

        var createDoc = await createResp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        Assert.NotNull(createDoc);
        var scratchpadId = createDoc!.RootElement.GetProperty("scratchpadId").GetString();
        Assert.False(string.IsNullOrEmpty(scratchpadId));

        // Verify B is in recipients
        var recipients = createDoc.RootElement.GetProperty("recipients");
        var online = recipients.GetProperty("onlinePeerIds");
        var hasB = Enumerable.Range(0, online.GetArrayLength())
            .Any(i => online[i].GetString() == peerB);
        Assert.True(hasB, "B should appear as an online recipient of the scratchpad.created event");

        // 4. B's inbox should have the scratchpad.created event
        var inboxResp = await _http.GetAsync($"/peers/{peerB}/inbox");
        Assert.Equal(HttpStatusCode.OK, inboxResp.StatusCode);

        var inboxDoc = await inboxResp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        Assert.NotNull(inboxDoc);
        var count = inboxDoc!.RootElement.GetProperty("count").GetInt32();
        Assert.True(count >= 1, $"B's inbox should have ≥1 item, got {count}");

        // 5. B appends a reply; A's inbox should receive it
        var appendReq = new AppendEntryRequest
        {
            AuthorPeerId = peerB,
            Content = "LGTM from B",
        };
        var appendResp = await _http.PostAsJsonAsync($"/scratchpads/{scratchpadId}/entries", appendReq, JsonOpts);
        Assert.Equal(HttpStatusCode.OK, appendResp.StatusCode);

        var aInboxResp = await _http.GetAsync($"/peers/{peerA}/inbox");
        Assert.Equal(HttpStatusCode.OK, aInboxResp.StatusCode);
        var aInboxDoc = await aInboxResp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        Assert.NotNull(aInboxDoc);
        var aCount = aInboxDoc!.RootElement.GetProperty("count").GetInt32();
        Assert.True(aCount >= 1, $"A's inbox should have ≥1 item after B's reply, got {aCount}");
    }

    // -------------------------------------------------------------------------
    // 6. Artifact flow  (ArtifactRoutes + ArtifactService)
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ArtifactFlow_PublishAndRetrieveByName()
    {
        var peerA = await RegisterPeerAsync("m-art");

        var publishReq = new PublishArtifactRequest
        {
            Namespace = "ns",
            Name = "smoke-artifact",
            Type = "diff",
            Content = "--- a\n+++ b",
            PublishedByPeerId = peerA,
        };
        var publishResp = await _http.PostAsJsonAsync("/artifacts", publishReq, JsonOpts);
        Assert.Equal(HttpStatusCode.OK, publishResp.StatusCode);

        var publishDoc = await publishResp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        Assert.NotNull(publishDoc);
        Assert.Equal(1, publishDoc!.RootElement.GetProperty("version").GetInt32());

        // GET by name
        var getResp = await _http.GetAsync("/artifacts/by-name?ns=ns&name=smoke-artifact");
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);

        var getDoc = await getResp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        Assert.NotNull(getDoc);
        Assert.Equal("--- a\n+++ b", getDoc!.RootElement.GetProperty("content").GetString());
    }

    // -------------------------------------------------------------------------
    // 8. Scratchpad close — idempotency / conflict  (T16 #2 over HTTP)
    // -------------------------------------------------------------------------
    [Fact]
    public async Task CloseScratchpad_SecondCloseReturns409()
    {
        var peerA = await RegisterPeerAsync("m-close");

        var createReq = new CreateScratchpadRequest
        {
            Type = "review",
            Title = "close-test",
            CreatedByPeerId = peerA,
        };
        var createResp = await _http.PostAsJsonAsync("/scratchpads", createReq, JsonOpts);
        createResp.EnsureSuccessStatusCode();

        var createDoc = await createResp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        var scratchpadId = createDoc!.RootElement.GetProperty("scratchpadId").GetString()!;

        var closeReq = new CloseScratchpadRequest { Resolution = "done" };

        // First close → 200
        var close1 = await _http.PatchAsJsonAsync($"/scratchpads/{scratchpadId}/close", closeReq, JsonOpts);
        Assert.Equal(HttpStatusCode.OK, close1.StatusCode);

        // Second close → 409 Conflict
        var close2 = await _http.PatchAsJsonAsync($"/scratchpads/{scratchpadId}/close", closeReq, JsonOpts);
        Assert.Equal(HttpStatusCode.Conflict, close2.StatusCode);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    private async Task<string> RegisterPeerAsync(string machineId)
    {
        var req = new RegisterPeerRequest
        {
            Tool = "claude-code",
            MachineId = machineId,
            RepoRoot = "/repo",
            Branch = "main",
        };
        var resp = await _http.PostAsJsonAsync("/peers", req, JsonOpts);
        resp.EnsureSuccessStatusCode();

        var doc = await resp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        return doc!.RootElement.GetProperty("peerId").GetString()!;
    }
}
