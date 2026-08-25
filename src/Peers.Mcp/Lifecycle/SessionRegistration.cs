using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Text.Json;
using ModelContextProtocol.Server;
using Peers.Mcp.Tools;
using Peers.Shared.Contracts;

namespace Peers.Mcp.Lifecycle;

/// <summary>
/// Registration is infrastructure, not an agent task: the first tool call (or resource read) on a
/// session derives its identity from where it runs — client name, git repo, branch — registers with
/// the broker, and starts the heartbeat and channel-notifier loops. Agents never call peer_register;
/// they can still shape how they appear to others through the presence tool.
///
/// Concurrent sessions in one repo are safe without any explicit resume key: the broker only ever
/// resumes EXPIRED identities, so two live sessions on the same machine+repo+branch always get
/// distinct peer ids.
/// </summary>
internal static class SessionRegistration
{
    private static readonly Lock Gate = new();
    private static Task<RegisterPeerResponse>? _inFlight;

    public static async Task EnsureRegisteredAsync(
        BrokerClient client,
        SessionState state,
        McpServer server,
        ILoggerFactory loggerFactory,
        CancellationToken ct = default)
    {
        if (state.PeerId is not null) return;

        state.Server ??= server;

        Task<RegisterPeerResponse> inFlight;
        lock (Gate)
        {
            if (state.PeerId is not null) return;
            _inFlight ??= RegisterAsync(client, state, server, loggerFactory, ct);
            inFlight = _inFlight;
        }

        try
        {
            var resp = await inFlight.ConfigureAwait(false);
            await ApplyRegistrationAsync(client, state, loggerFactory, resp);
        }
        catch
        {
            // A failed attempt must not poison the session: the next call retries.
            lock (Gate) _inFlight = null;
            throw;
        }
    }

    /// <summary>Test seam: register with explicit identity instead of the derived one.</summary>
    public static async Task<RegisterPeerResponse> RegisterWithAsync(
        BrokerClient client, SessionState state, ILoggerFactory loggerFactory, RegisterPeerRequest request, CancellationToken ct = default)
    {
        var resp = await client.RegisterAsync(request).ConfigureAwait(false);
        state.LastRegisterRequest = request;
        await ApplyRegistrationAsync(client, state, loggerFactory, resp);
        return resp;
    }

    private static async Task<RegisterPeerResponse> RegisterAsync(
        BrokerClient client, SessionState state, McpServer server, ILoggerFactory loggerFactory, CancellationToken ct)
    {
        var info = server.ClientInfo;
        var (repoRoot, branch) = DeriveGitIdentity();
        state.RepoRoot = repoRoot;

        var req = new RegisterPeerRequest
        {
            Tool = DeriveTool(info?.Name),
            MachineId = Environment.MachineName,
            RepoRoot = repoRoot,
            Branch = branch,
            Role = "implementing",
            Summary = $"{info?.Name ?? "unknown client"} in {branch ?? Path.GetFileName(repoRoot ?? "no repo")}",
        };
        var resp = await client.RegisterAsync(req).ConfigureAwait(false);
        state.LastRegisterRequest = req;
        return resp;
    }

    /// <summary>
    /// Adopts a registration: stop whatever loops the previous identity was running FIRST — a stale
    /// notifier holds the OLD resource baseline and would replay history the new session must not see —
    /// then take the new cursor, baseline, and loops.
    /// </summary>
    private static async Task ApplyRegistrationAsync(
        BrokerClient client, SessionState state, ILoggerFactory loggerFactory, RegisterPeerResponse resp)
    {
        await state.StopServicesAsync();

        state.ResetCursorForRegistration(resp.LastCursor);
        state.PeerId = resp.PeerId;
        state.ResourceBaseline = resp.LatestEventId;
        // The cached roster belongs to the OLD peer id. Keeping it would let this session's own
        // previous row — still carrying fresh progress — slip past the wait ticker's self-filter and
        // read as another peer working. The next poll refills it.
        state.PeerRoster = [];

        state.Heartbeat = new HeartbeatService(client, state, loggerFactory.CreateLogger<HeartbeatService>());
        state.Heartbeat.Start();

        state.ChannelNotifier = new ChannelNotifier(client, state, loggerFactory.CreateLogger<ChannelNotifier>());
        state.ChannelNotifier.Start();
    }

    internal static string DeriveTool(string? clientName)
    {
        var name = clientName ?? "";
        if (name.Contains("claude", StringComparison.OrdinalIgnoreCase)) return "claude-code";
        if (name.Contains("codex", StringComparison.OrdinalIgnoreCase)) return "codex";
        if (!string.IsNullOrWhiteSpace(name)) return name.Length > 40 ? name[..40] : name;
        return "unknown";
    }

    private static (string? RepoRoot, string? Branch) DeriveGitIdentity()
    {
        try
        {
            var repo = RunGit("rev-parse --show-toplevel");
            if (repo is null) return (null, null);
            var branch = RunGit("rev-parse --abbrev-ref HEAD");
            return (repo, branch);
        }
        catch
        {
            return (null, null);
        }
    }

    private static string? RunGit(string args)
    {
        using var proc = Process.Start(new ProcessStartInfo
        {
            FileName = "git",
            Arguments = args,
            WorkingDirectory = Directory.GetCurrentDirectory(),
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        });
        if (proc is null) return null;
        var line = proc.StandardOutput.ReadLine();
        proc.WaitForExit(2000);
        return proc.ExitCode == 0 && !string.IsNullOrWhiteSpace(line) ? line.Trim() : null;
    }
}
