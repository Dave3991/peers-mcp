using System.Net.Http.Json;
using System.Text.Json;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

namespace Peers.Mcp;

public sealed class BrokerClient : IDisposable
{
    private readonly HttpClient _http;
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    public BrokerClient(string baseUrl)
    {
        _http = new HttpClient { BaseAddress = new Uri(baseUrl), Timeout = TimeSpan.FromSeconds(130) };
    }

    internal BrokerClient(HttpClient http)
    {
        _http = http;
    }

    // Peers
    public async Task<RegisterPeerResponse> RegisterAsync(RegisterPeerRequest req) =>
        (await PostAsync<RegisterPeerResponse>("/peers", req))!;

    public async Task HeartbeatAsync(string peerId, HeartbeatRequest req) =>
        await PatchAsync($"/peers/{peerId}/heartbeat", req);

    public async Task DeregisterAsync(string peerId)
    {
        try { await _http.DeleteAsync($"/peers/{peerId}"); }
        catch { /* best-effort on shutdown */ }
    }

    public async Task<List<Peer>> ListPeersAsync(string? repoRoot = null, string? branch = null, string? tool = null,
        bool includeExpired = false, CancellationToken cancellationToken = default)
    {
        var query = BuildQuery(("repo_root", repoRoot), ("branch", branch), ("tool", tool), ("include_expired", includeExpired ? "true" : null));
        return (await GetAsync<List<Peer>>($"/peers{query}", cancellationToken))!;
    }

    // Channels
    public async Task<CreateChannelResponse> CreateChannelAsync(CreateChannelRequest req) =>
        (await PostAsync<CreateChannelResponse>("/channels", req))!;

    public async Task<ListChannelsResponse> ListChannelsAsync(string? repoRoot = null, int limit = 100)
    {
        var query = BuildQuery(("repo_root", repoRoot), ("limit", limit.ToString()));
        return (await GetAsync<ListChannelsResponse>($"/channels{query}"))!;
    }

    public async Task<ChannelDto> GetChannelAsync(string channelId, CancellationToken cancellationToken = default) =>
        (await GetAsync<ChannelDto>($"/channels/{Uri.EscapeDataString(channelId)}", cancellationToken))!;

    public async Task<SendMessageResponse?> SendMessageAsync(string channelId, SendMessageRequest req) =>
        await PostAsync<SendMessageResponse>($"/channels/{Uri.EscapeDataString(channelId)}/messages", req);

    public async Task<ReadMessagesResponse?> ReadChannelMessagesAsync(
        string channelId, string? viewer, long sinceSeq = 0, int limit = 200)
    {
        var query = BuildQuery(("viewer", viewer), ("since_seq", sinceSeq.ToString()), ("limit", limit.ToString()));
        return await GetAsync<ReadMessagesResponse>($"/channels/{Uri.EscapeDataString(channelId)}/messages{query}");
    }

    public async Task<RemoveMessagesResponse> ClearChannelAsync(
        string channelId, CancellationToken cancellationToken = default)
    {
        try
        {
            using var resp = await _http.DeleteAsync(
                $"/channels/{Uri.EscapeDataString(channelId)}/messages", cancellationToken);
            if (!resp.IsSuccessStatusCode)
                throw new BrokerApiException(resp.StatusCode, await resp.Content.ReadAsStringAsync(cancellationToken));
            return (await resp.Content.ReadFromJsonAsync<RemoveMessagesResponse>(JsonOpts, cancellationToken))!;
        }
        catch (HttpRequestException ex) { throw BrokerApiException.FromTransport(ex); }
    }

    public async Task<RemoveMessagesResponse> RemoveMessagesAsync(
        string channelId, RemoveMessagesRequest req, CancellationToken cancellationToken = default) =>
        (await PostAsync<RemoveMessagesResponse>(
            $"/channels/{Uri.EscapeDataString(channelId)}/messages/remove", req, cancellationToken))!;

    // Artifacts
    public async Task<PublishArtifactResponse> PublishArtifactAsync(PublishArtifactRequest req) =>
        (await PostAsync<PublishArtifactResponse>("/artifacts", req))!;

    public async Task<Artifact?> GetArtifactByIdAsync(string id) =>
        await GetAsync<Artifact>($"/artifacts/{Uri.EscapeDataString(id)}");

    public async Task<Artifact?> GetArtifactByNameAsync(string ns, string name) =>
        await GetAsync<Artifact>($"/artifacts/by-name?ns={Uri.EscapeDataString(ns)}&name={Uri.EscapeDataString(name)}");

    public async Task<List<ArtifactMetadata>> ListArtifactsAsync(string? type = null, string? repoRoot = null, string? ns = null, int limit = 50, int offset = 0)
    {
        var query = BuildQuery(("type", type), ("repo_root", repoRoot), ("ns", ns), ("limit", limit.ToString()), ("offset", offset.ToString()));
        return (await GetAsync<List<ArtifactMetadata>>($"/artifacts{query}"))!;
    }

    // Claims
    public async Task<ClaimPathsResponse> ClaimPathsAsync(ClaimPathsRequest req, CancellationToken cancellationToken = default) =>
        (await PostAsync<ClaimPathsResponse>("/claims", req, cancellationToken))!;

    public async Task<ReleaseClaimsResponse> ReleaseClaimsAsync(ReleaseClaimsRequest req, CancellationToken cancellationToken = default) =>
        (await PostAsync<ReleaseClaimsResponse>("/claims/release", req, cancellationToken))!;

    public async Task<ListClaimsResponse> ListClaimsAsync(string peerId, CancellationToken cancellationToken = default) =>
        (await GetAsync<ListClaimsResponse>($"/claims{BuildQuery(("peer_id", peerId))}", cancellationToken))!;

    // Poll
    /// <param name="cancellationToken">The caller's cancellation. The background notifier passes its
    /// shutdown token so a broker that has stopped answering cannot hold process exit for the whole
    /// 130s HttpClient.Timeout above.</param>
    public async Task<PollResponse> PollAsync(string peerId, long cursor, int limit = 100, CancellationToken cancellationToken = default) =>
        (await GetAsync<PollResponse>($"/poll/{peerId}?cursor={cursor}&limit={limit}", cancellationToken))!;

    // Inbox
    public async Task<InboxResponse?> GetInboxAsync(string peerId, int limit = 100) =>
        await GetAsync<InboxResponse>($"/peers/{peerId}/inbox?limit={limit}");

    /// <param name="cancellationToken">The caller's cancellation (for peer_wait: the MCP request's).
    /// Linked with — not replaced by — the broker-hang deadline, because the two stops mean different
    /// things to the caller and the catch below has to tell them apart.</param>
    public async Task<InboxResponse?> WaitInboxAsync(string peerId, int timeoutSeconds, int limit = 100, CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds + 10));
        using var req = new HttpRequestMessage(HttpMethod.Get, $"/peers/{peerId}/inbox/wait?timeout_seconds={timeoutSeconds}&limit={limit}");
        try
        {
            var resp = await _http.SendAsync(req, cts.Token);
            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(cts.Token);
                throw new BrokerApiException(resp.StatusCode, body);
            }
            return await resp.Content.ReadFromJsonAsync<InboxResponse>(JsonOpts, cts.Token);
        }
        // Only OUR deadline (or HttpClient.Timeout) is a broker timeout. A caller-cancelled call is a
        // different event and must not be laundered into one: peer_wait would report timed_out for a
        // request the client abandoned.
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        { throw new BrokerApiException(System.Net.HttpStatusCode.GatewayTimeout, "Broker did not respond within the wait window."); }
        catch (HttpRequestException ex) { throw BrokerApiException.FromTransport(ex); }
    }

    public async Task AckInboxAsync(string peerId, string[]? inboxIds = null, bool all = false, CancellationToken cancellationToken = default) =>
        await PostAsync<object>($"/peers/{peerId}/inbox/ack", new { InboxIds = inboxIds, All = all }, cancellationToken);

    // Health
    public async Task<bool> IsHealthyAsync()
    {
        try
        {
            var resp = await _http.GetAsync("/health");
            return resp.IsSuccessStatusCode;
        }
        catch { return false; }
    }

    private async Task<T?> GetAsync<T>(string path, CancellationToken cancellationToken = default)
    {
        try
        {
            var resp = await _http.GetAsync(path, cancellationToken);
            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(cancellationToken);
                throw new BrokerApiException(resp.StatusCode, body);
            }
            return await resp.Content.ReadFromJsonAsync<T>(JsonOpts, cancellationToken);
        }
        catch (HttpRequestException ex) { throw BrokerApiException.FromTransport(ex); }
    }

    private async Task<T?> PostAsync<T>(string path, object body, CancellationToken cancellationToken = default)
    {
        try
        {
            var resp = await _http.PostAsJsonAsync(path, body, JsonOpts, cancellationToken);
            if (!resp.IsSuccessStatusCode)
            {
                var errorBody = await resp.Content.ReadAsStringAsync(cancellationToken);
                throw new BrokerApiException(resp.StatusCode, errorBody);
            }
            return await resp.Content.ReadFromJsonAsync<T>(JsonOpts, cancellationToken);
        }
        catch (HttpRequestException ex) { throw BrokerApiException.FromTransport(ex); }
    }

    private async Task PatchAsync(string path, object body)
    {
        try
        {
            var resp = await _http.PatchAsJsonAsync(path, body, JsonOpts);
            if (!resp.IsSuccessStatusCode)
            {
                var errorBody = await resp.Content.ReadAsStringAsync();
                throw new BrokerApiException(resp.StatusCode, errorBody);
            }
        }
        catch (HttpRequestException ex) { throw BrokerApiException.FromTransport(ex); }
    }

    internal static string BuildQuery(params (string key, string? value)[] pairs)
    {
        var parts = pairs.Where(p => p.value is not null).Select(p => $"{p.key}={Uri.EscapeDataString(p.value!)}");
        var q = string.Join("&", parts);
        return q.Length > 0 ? $"?{q}" : "";
    }

    public void Dispose() => _http.Dispose();
}
