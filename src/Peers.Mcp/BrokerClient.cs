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
        _http = new HttpClient { BaseAddress = new Uri(baseUrl) };
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

    public async Task<List<Peer>> ListPeersAsync(string? repoRoot = null, string? branch = null, string? tool = null)
    {
        var query = BuildQuery(("repo_root", repoRoot), ("branch", branch), ("tool", tool));
        return (await GetAsync<List<Peer>>($"/peers{query}"))!;
    }

    // Scratchpads
    public async Task<CreateScratchpadResponse> CreateScratchpadAsync(CreateScratchpadRequest req) =>
        (await PostAsync<CreateScratchpadResponse>("/scratchpads", req))!;

    public async Task<AppendEntryResponse?> AppendEntryAsync(string padId, AppendEntryRequest req) =>
        await PostAsync<AppendEntryResponse>($"/scratchpads/{padId}/entries", req);

    public async Task<ReadScratchpadResponse?> ReadScratchpadAsync(string padId, int limit = 200, int offset = 0) =>
        await GetAsync<ReadScratchpadResponse>($"/scratchpads/{padId}?limit={limit}&offset={offset}");

    public async Task<List<Scratchpad>> ListScratchpadsAsync(string? type = null, string? repoRoot = null, string? status = null)
    {
        var query = BuildQuery(("type", type), ("repo_root", repoRoot), ("status", status));
        return (await GetAsync<List<Scratchpad>>($"/scratchpads{query}"))!;
    }

    public async Task ResolveScratchpadAsync(string padId, ResolveScratchpadRequest req) =>
        await PatchAsync($"/scratchpads/{padId}/resolve", req);

    public async Task CloseScratchpadAsync(string padId, CloseScratchpadRequest req) =>
        await PatchAsync($"/scratchpads/{padId}/close", req);

    // Artifacts
    public async Task<PublishArtifactResponse> PublishArtifactAsync(PublishArtifactRequest req) =>
        (await PostAsync<PublishArtifactResponse>("/artifacts", req))!;

    public async Task<Artifact?> GetArtifactByIdAsync(string id) =>
        await GetAsync<Artifact>($"/artifacts/{Uri.EscapeDataString(id)}");

    public async Task<Artifact?> GetArtifactByNameAsync(string ns, string name) =>
        await GetAsync<Artifact>($"/artifacts/by-name?ns={Uri.EscapeDataString(ns)}&name={Uri.EscapeDataString(name)}");

    public async Task<List<ArtifactMetadata>> ListArtifactsAsync(string? type = null, string? repoRoot = null, string? ns = null)
    {
        var query = BuildQuery(("type", type), ("repo_root", repoRoot), ("ns", ns));
        return (await GetAsync<List<ArtifactMetadata>>($"/artifacts{query}"))!;
    }

    // Poll
    public async Task<PollResponse> PollAsync(string peerId, long cursor, int limit = 100) =>
        (await GetAsync<PollResponse>($"/poll/{peerId}?cursor={cursor}&limit={limit}"))!;

    // Inbox
    public async Task<InboxResponse?> GetInboxAsync(string peerId, int limit = 100) =>
        await GetAsync<InboxResponse>($"/peers/{peerId}/inbox?limit={limit}");

    public async Task AckInboxAsync(string peerId, string[]? inboxIds = null, bool all = false) =>
        await PostAsync<object>($"/peers/{peerId}/inbox/ack", new { InboxIds = inboxIds, All = all });

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

    private async Task<T?> GetAsync<T>(string path)
    {
        try
        {
            var resp = await _http.GetAsync(path);
            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync();
                throw new BrokerApiException(resp.StatusCode, body);
            }
            return await resp.Content.ReadFromJsonAsync<T>(JsonOpts);
        }
        catch (HttpRequestException ex) { throw BrokerApiException.FromTransport(ex); }
    }

    private async Task<T?> PostAsync<T>(string path, object body)
    {
        try
        {
            var resp = await _http.PostAsJsonAsync(path, body, JsonOpts);
            if (!resp.IsSuccessStatusCode)
            {
                var errorBody = await resp.Content.ReadAsStringAsync();
                throw new BrokerApiException(resp.StatusCode, errorBody);
            }
            return await resp.Content.ReadFromJsonAsync<T>(JsonOpts);
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

    private static string BuildQuery(params (string key, string? value)[] pairs)
    {
        var parts = pairs.Where(p => p.value is not null).Select(p => $"{p.key}={Uri.EscapeDataString(p.value!)}");
        var q = string.Join("&", parts);
        return q.Length > 0 ? $"?{q}" : "";
    }

    public void Dispose() => _http.Dispose();
}
