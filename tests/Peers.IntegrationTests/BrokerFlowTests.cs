using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Peers.Broker.Hosting;
using Peers.Broker.Persistence;
using Peers.Shared;
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

        // The contract version is what makes a stale daemon replace itself; a payload change that
        // forgets to bump it fails silently in production, so pin it here.
        Assert.Equal(BrokerContract.Current, doc.RootElement.GetProperty("contract").GetInt32());
    }

    // The heartbeat route validated nothing until progress arrived, so these pin the whole guard,
    // not just the progress part of it.
    [Fact]
    public async Task Heartbeat_InvalidPayloads_Return400()
    {
        var peerId = await RegisterPeerAsync("m-validate");

        var negative = await _http.PatchAsJsonAsync($"/peers/{peerId}/heartbeat",
            new HeartbeatRequest { ProgressCurrent = -1 }, JsonOpts);
        Assert.Equal(HttpStatusCode.BadRequest, negative.StatusCode);

        // Total on its own would be silently dropped by the discriminator, so it is refused loudly.
        var totalOnly = await _http.PatchAsJsonAsync($"/peers/{peerId}/heartbeat",
            new HeartbeatRequest { ProgressTotal = 12 }, JsonOpts);
        Assert.Equal(HttpStatusCode.BadRequest, totalOnly.StatusCode);

        var oversizedFocus = await _http.PatchAsJsonAsync($"/peers/{peerId}/heartbeat",
            new HeartbeatRequest { FocusPaths = new string('x', 11 * 1024) }, JsonOpts);
        Assert.Equal(HttpStatusCode.BadRequest, oversizedFocus.StatusCode);

        // The valid shape still goes through.
        var ok = await _http.PatchAsJsonAsync($"/peers/{peerId}/heartbeat",
            new HeartbeatRequest { ProgressCurrent = 7, ProgressTotal = 12 }, JsonOpts);
        ok.EnsureSuccessStatusCode();
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
    // 3-5. Channel flow (ChannelRoutes + ChannelService + RecipientResolver
    //      + InboxRepository + EventService)
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ChannelFlow_CreateSendMentionInbox()
    {
        var peerA = await RegisterPeerAsync("m-a");
        var peerB = await RegisterPeerAsync("m-b");

        var createResp = await _http.PostAsJsonAsync("/channels", new CreateChannelRequest
        {
            Name = "T15-smoke",
            RepoRoot = "/repo",
            CreatedByPeerId = peerA,
        }, JsonOpts);
        Assert.Equal(HttpStatusCode.OK, createResp.StatusCode);

        var createDoc = await createResp.Content.ReadFromJsonAsync<JsonDocument>(JsonOpts);
        Assert.NotNull(createDoc);
        var channelId = createDoc!.RootElement.GetProperty("channelId").GetString();
        Assert.False(string.IsNullOrEmpty(channelId));

        // A sends a LOUD message mentioning B: everyone sees it, B also gets an inbox row.
        var sendReq = new SendMessageRequest { AuthorPeerId = peerA, Content = "hello all", To = [peerB] };
        var sendResp = await _http.PostAsJsonAsync($"/channels/{channelId}/messages", sendReq, JsonOpts);
        Assert.Equal(HttpStatusCode.OK, sendResp.StatusCode);

        var inbox = await _http.GetFromJsonAsync<JsonDocument>($"/peers/{peerB}/inbox", JsonOpts);
        Assert.True(inbox!.RootElement.GetProperty("count").GetInt32() >= 1,
            "B's inbox should hold the mention");

        // A whisper to B is invisible in C's read — participant-scoped visibility over HTTP.
        var peerC = await RegisterPeerAsync("m-c");
        var quietReq = new SendMessageRequest { AuthorPeerId = peerA, Content = "whisper", To = [peerB], Quiet = true };
        Assert.Equal(HttpStatusCode.OK, (await _http.PostAsJsonAsync($"/channels/{channelId}/messages", quietReq, JsonOpts)).StatusCode);

        // Fresh joins post one loud row per repo-matched channel, so C's own join row is visible here too.
        var cView = await _http.GetFromJsonAsync<JsonDocument>($"/channels/{channelId}/messages?viewer={peerC}", JsonOpts);
        Assert.Equal(2, cView!.RootElement.GetProperty("messages").GetArrayLength());

        var bView = await _http.GetFromJsonAsync<JsonDocument>($"/channels/{channelId}/messages?viewer={peerB}", JsonOpts);
        Assert.Equal(3, bView!.RootElement.GetProperty("messages").GetArrayLength());
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
    // 8. Duplicate channel name in the same scope → 400
    // -------------------------------------------------------------------------
    [Fact]
    public async Task ChannelCreate_DuplicateNameInScope_ReturnsBadRequest()
    {
        var peerA = await RegisterPeerAsync("m-dup");

        var createReq = new CreateChannelRequest { Name = "dup-test", RepoRoot = "/repo", CreatedByPeerId = peerA };
        Assert.Equal(HttpStatusCode.OK, (await _http.PostAsJsonAsync("/channels", createReq, JsonOpts)).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await _http.PostAsJsonAsync("/channels", createReq, JsonOpts)).StatusCode);
    }

    [Fact]
    public async Task Removal_InvalidIdsAreRejected_AndMissingChannelsReturn404()
    {
        var peer = await RegisterPeerAsync("removal-validation");
        var created = await _http.PostAsJsonAsync("/channels",
            new CreateChannelRequest { Name = "validation", CreatedByPeerId = peer }, JsonOpts);
        var channel = (await created.Content.ReadFromJsonAsync<CreateChannelResponse>(JsonOpts))!;
        var sent = await _http.PostAsJsonAsync($"/channels/{channel.ChannelId}/messages",
            new SendMessageRequest { AuthorPeerId = peer, Content = "must survive bad requests" }, JsonOpts);
        sent.EnsureSuccessStatusCode();
        string[]?[] invalid = [null, [], [""], [" "], [null!], Enumerable.Repeat("id", 201).ToArray()];
        foreach (var ids in invalid)
        {
            var response = await _http.PostAsJsonAsync($"/channels/{channel.ChannelId}/messages/remove",
                new { MessageIds = ids }, JsonOpts);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        }
        var read = await _http.GetFromJsonAsync<ReadMessagesResponse>($"/channels/{channel.ChannelId}/messages", JsonOpts);
        Assert.Equal("must survive bad requests", Assert.Single(read!.Messages).Content);
        Assert.Equal(HttpStatusCode.NotFound, (await _http.DeleteAsync("/channels/missing/messages")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _http.PostAsJsonAsync("/channels/missing/messages/remove",
            new RemoveMessagesRequest { MessageIds = ["id"] }, JsonOpts)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _http.GetAsync("/channels/missing")).StatusCode);
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
