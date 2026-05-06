using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using Peers.Shared;
using Peers.Shared.Contracts;
using Peers.Shared.Models;

var jsonOpts = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
    WriteIndented = true,
};

if (args.Length == 0)
{
    PrintUsage();
    return 1;
}

try
{
    switch (args[0])
    {
        case "broker-status":
        {
            var brokerStatus = await BrokerRuntime.GetStatusAsync();
            PrintBrokerStatus(brokerStatus);
            return brokerStatus.IsHealthy ? 0 : 1;
        }

        case "broker-start":
        {
            var broker = await BrokerRuntime.EnsureBrokerRunningAsync();
            var brokerStatus = await BrokerRuntime.GetStatusAsync();
            PrintBrokerStatus(brokerStatus);
            Console.WriteLine($"\nBroker ready on http://127.0.0.1:{broker.Port} (pid {broker.Pid}).");
            return 0;
        }

        case "claude":
        {
            return await RunClaudeAsync(args.Skip(1).ToArray());
        }

        case "status":
        {
            var brokerStatus = await BrokerRuntime.GetStatusAsync();
            PrintBrokerStatus(brokerStatus);
            if (!brokerStatus.IsHealthy)
            {
                Console.Error.WriteLine("\nBroker is not reachable. Run 'peers-cli broker-start' to launch it.");
                return 1;
            }

            using var http = CreateHttpClient(brokerStatus.Port);
            var peers = await http.GetFromJsonAsync<List<Peer>>("/peers", jsonOpts);
            Console.WriteLine($"\nPeers ({peers?.Count ?? 0}):");
            if (peers is not null)
                foreach (var p in peers)
                    Console.WriteLine($"  {p.PeerId}  {p.Tool,-12} {p.RepoRoot ?? "?",-30} {p.Branch ?? "?",-20} {p.Summary ?? ""}");
            break;
        }

        case "register":
        {
            var brokerStatus = await RequireHealthyBrokerAsync();
            if (brokerStatus is null)
                return 1;

            using var http = CreateHttpClient(brokerStatus.Port);
            var tool = args.Length > 1 ? args[1] : "cli";
            var repo = GetArg(args, "--repo");
            var branch = GetArg(args, "--branch");
            var summary = GetArg(args, "--summary");
            var regReq = new RegisterPeerRequest
            {
                Tool = tool,
                MachineId = Environment.MachineName,
                RepoRoot = repo,
                Branch = branch,
                Summary = summary,
            };
            var regResp = await http.PostAsJsonAsync("/peers", regReq, jsonOpts);
            Console.WriteLine(await regResp.Content.ReadAsStringAsync());
            break;
        }

        case "scratchpads":
        {
            var brokerStatus = await RequireHealthyBrokerAsync();
            if (brokerStatus is null)
                return 1;

            using var http = CreateHttpClient(brokerStatus.Port);
            var pads = await http.GetFromJsonAsync<List<Scratchpad>>("/scratchpads", jsonOpts);
            Console.WriteLine(JsonSerializer.Serialize(pads, jsonOpts));
            break;
        }

        case "artifacts":
        {
            var brokerStatus = await RequireHealthyBrokerAsync();
            if (brokerStatus is null)
                return 1;

            using var http = CreateHttpClient(brokerStatus.Port);
            var arts = await http.GetFromJsonAsync<List<ArtifactMetadata>>("/artifacts", jsonOpts);
            Console.WriteLine(JsonSerializer.Serialize(arts, jsonOpts));
            break;
        }

        case "poll":
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Usage: poll <peer-id> [--cursor N]");
                return 1;
            }

            var brokerStatus = await RequireHealthyBrokerAsync();
            if (brokerStatus is null)
                return 1;

            using var http = CreateHttpClient(brokerStatus.Port);
            var cursor = long.Parse(GetArg(args, "--cursor") ?? "0");
            var pollResp = await http.GetFromJsonAsync<PollResponse>($"/poll/{args[1]}?cursor={cursor}", jsonOpts);
            Console.WriteLine(JsonSerializer.Serialize(pollResp, jsonOpts));
            break;
        }

        case "events":
        {
            if (!HasFlag(args, "--follow"))
            {
                Console.Error.WriteLine("Usage: events --follow --peer <peerId> [--interval <seconds>]");
                return 1;
            }

            var peerId = GetArg(args, "--peer");
            if (string.IsNullOrEmpty(peerId))
            {
                Console.Error.WriteLine("--peer <peerId> is required");
                return 1;
            }

            var brokerStatus = await RequireHealthyBrokerAsync();
            if (brokerStatus is null)
                return 1;

            var intervalSeconds = int.TryParse(GetArg(args, "--interval"), out var i) && i > 0 ? i : 2;
            return await FollowEventsAsync(brokerStatus.Port, peerId!, intervalSeconds, jsonOpts);
        }

        default:
            Console.Error.WriteLine($"Unknown command: {args[0]}");
            PrintUsage();
            return 1;
    }
}
catch (HttpRequestException ex)
{
    Console.Error.WriteLine($"Broker request failed: {ex.Message}");
    return 1;
}

return 0;

static HttpClient CreateHttpClient(int port) =>
    new() { BaseAddress = new Uri($"http://127.0.0.1:{port}") };

static async Task<BrokerStatus?> RequireHealthyBrokerAsync()
{
    var brokerStatus = await BrokerRuntime.GetStatusAsync();
    if (brokerStatus.IsHealthy)
        return brokerStatus;

    PrintBrokerStatus(brokerStatus);
    Console.Error.WriteLine("\nBroker is not reachable. Run 'peers-cli broker-start' to launch it.");
    return null;
}

static async Task<int> RunClaudeAsync(string[] forwardedArgs)
{
    if (OwnsFlag(forwardedArgs, "--channels") || OwnsFlag(forwardedArgs, "--dangerously-load-development-channels"))
    {
        Console.Error.WriteLine("'peers-cli claude' automatically sets the Claude channel flags. Remove '--channels' and '--dangerously-load-development-channels'.");
        return 1;
    }

    var psi = new ProcessStartInfo
    {
        FileName = "claude",
        UseShellExecute = false,
    };

    psi.ArgumentList.Add("--dangerously-load-development-channels");
    psi.ArgumentList.Add("server:peers");

    foreach (var arg in forwardedArgs)
        psi.ArgumentList.Add(arg);

    using var process = Process.Start(psi);
    if (process is null)
    {
        Console.Error.WriteLine("Failed to start 'claude'. Ensure the command is installed and on PATH.");
        return 1;
    }

    await process.WaitForExitAsync();
    return process.ExitCode;
}

static bool OwnsFlag(string[] args, string flag) =>
    args.Any(arg => string.Equals(arg, flag, StringComparison.Ordinal) ||
                    arg.StartsWith(flag + "=", StringComparison.Ordinal));

static string? GetArg(string[] args, string name)
{
    var idx = Array.IndexOf(args, name);
    return idx >= 0 && idx + 1 < args.Length ? args[idx + 1] : null;
}

static bool HasFlag(string[] args, string name) =>
    Array.IndexOf(args, name) >= 0;

static async Task<int> FollowEventsAsync(int port, string peerId, int intervalSeconds, JsonSerializerOptions jsonOpts)
{
    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    using var http = CreateHttpClient(port);
    long cursor = 0;
    Console.Error.WriteLine($"Following events for {peerId} every {intervalSeconds}s. Press Ctrl+C to stop.");

    var ct = cts.Token;
    while (!ct.IsCancellationRequested)
    {
        try
        {
            var resp = await http.GetFromJsonAsync<PollResponse>(
                $"/poll/{peerId}?cursor={cursor}&limit=50&peek=true", jsonOpts, ct);

            if (resp is not null)
            {
                foreach (var ev in resp.Events)
                    Console.WriteLine($"{ev.CreatedAt:O} {ev.Type} {SummarizePayload(ev.Payload)}");
                cursor = resp.Cursor;
            }
        }
        catch (OperationCanceledException) { break; }
        catch (HttpRequestException ex)
        {
            Console.Error.WriteLine($"Poll failed: {ex.Message}");
        }

        try
        {
            await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), ct);
        }
        catch (OperationCanceledException) { break; }
    }

    return 0;
}

static string SummarizePayload(string payload)
{
    if (string.IsNullOrEmpty(payload))
        return "";

    var parts = new List<string>();
    try
    {
        using var doc = JsonDocument.Parse(payload);
        if (doc.RootElement.ValueKind == JsonValueKind.Object)
        {
            foreach (var keyName in new[] { "peer_id", "scratchpad_id", "artifact_id", "title", "type" })
            {
                if (doc.RootElement.TryGetProperty(keyName, out var v) && v.ValueKind == JsonValueKind.String)
                    parts.Add($"{keyName}={v.GetString()}");
            }
        }
    }
    catch (JsonException)
    {
        // Fall through to raw trim
    }

    var summary = parts.Count > 0 ? string.Join(" ", parts) : payload.Replace('\n', ' ').Replace('\r', ' ');
    return summary.Length > 80 ? summary[..80] + "..." : summary;
}

static void PrintBrokerStatus(BrokerStatus status)
{
    Console.WriteLine($"Broker health: {(status.IsHealthy ? "ok" : "unreachable")}");
    Console.WriteLine($"Port: {status.Port}");
    Console.WriteLine($"State dir: {status.StateDirectory}");
    Console.WriteLine($"Discovery file: {status.DiscoveryFile}");

    if (status.Discovery is not null)
    {
        Console.WriteLine($"Discovery pid: {status.Discovery.Pid}");
        Console.WriteLine($"Discovery started_at: {status.Discovery.StartedAt}");
    }
    else if (status.DiscoveryFileExists)
    {
        Console.WriteLine("Discovery status: present but unreadable");
    }
    else
    {
        Console.WriteLine("Discovery status: missing");
    }

    if (status.HasStaleDiscovery)
        Console.WriteLine("Discovery freshness: stale");
}

static void PrintUsage()
{
    Console.WriteLine("""
        Peers CLI — test and debug the peers broker

        Usage: peers-cli <command> [options]

        Commands:
          broker-status           Show broker health and discovery-file state
          broker-start            Launch the broker if needed and wait until healthy
          claude [args...]        Start Claude Code with `server:peers` development channel enabled
          status                  Show broker status and connected peers
          register [tool]         Register a simulated peer (--repo, --branch, --summary)
          scratchpads             List all scratchpads
          artifacts               List all artifacts
          poll <peer-id>          Poll events (--cursor N)
          events --follow         Stream events for a peer (--peer <id> [--interval N])
        """);
}
