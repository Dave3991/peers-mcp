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

    // Peers
    public async Task<RegisterPeerResponse> RegisterAsync(RegisterPeerRequest req) =>
        (await PostAsync<RegisterPeerResponse>("/peers", req))!;

    public async Task HeartbeatAsync(string peerId, HeartbeatRequest req) =>
        await _http.PatchAsJsonAsync($"/peers/{peerId}/heartbeat", req, JsonOpts);

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

    public async Task CloseScratchpadAsync(string padId, CloseScratchpadRequest req) =>
        await _http.PatchAsJsonAsync($"/scratchpads/{padId}/close", req, JsonOpts);

    // Artifacts
    public async Task<PublishArtifactResponse> PublishArtifactAsync(PublishArtifactRequest req) =>
        (await PostAsync<PublishArtifactResponse>("/artifacts", req))!;

    public async Task<Artifact?> GetArtifactByIdAsync(string id) =>
        await GetAsync<Artifact>($"/artifacts/{id}");

    public async Task<Artifact?> GetArtifactByNameAsync(string ns, string name) =>
        await GetAsync<Artifact>($"/artifacts/by-name/{ns}/{name}");

    public async Task<List<ArtifactMetadata>> ListArtifactsAsync(string? type = null, string? repoRoot = null, string? ns = null)
    {
        var query = BuildQuery(("type", type), ("repo_root", repoRoot), ("ns", ns));
        return (await GetAsync<List<ArtifactMetadata>>($"/artifacts{query}"))!;
    }

    // Poll
    public async Task<PollResponse> PollAsync(string peerId, long cursor, int limit = 100) =>
        (await GetAsync<PollResponse>($"/poll/{peerId}?cursor={cursor}&limit={limit}"))!;

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
        var resp = await _http.GetAsync(path);
        if (!resp.IsSuccessStatusCode) return default;
        return await resp.Content.ReadFromJsonAsync<T>(JsonOpts);
    }

    private async Task<T?> PostAsync<T>(string path, object body)
    {
        var resp = await _http.PostAsJsonAsync(path, body, JsonOpts);
        resp.EnsureSuccessStatusCode();
        return await resp.Content.ReadFromJsonAsync<T>(JsonOpts);
    }

    private static string BuildQuery(params (string key, string? value)[] pairs)
    {
        var parts = pairs.Where(p => p.value is not null).Select(p => $"{p.key}={Uri.EscapeDataString(p.value!)}");
        var q = string.Join("&", parts);
        return q.Length > 0 ? $"?{q}" : "";
    }

    public void Dispose() => _http.Dispose();
}
