using System.Diagnostics;
using System.Text.Json;

namespace Peers.Shared;

public sealed record BrokerInfo(int Port, int Pid, string StartedAt, int SchemaVersion);

public sealed record BrokerStatus
{
    public required int Port { get; init; }
    public required bool IsHealthy { get; init; }
    public required bool DiscoveryFileExists { get; init; }
    public required bool HasStaleDiscovery { get; init; }
    public required string StateDirectory { get; init; }
    public required string DiscoveryFile { get; init; }
    public BrokerInfo? Discovery { get; init; }
}

public static class BrokerRuntime
{
    private const int DefaultPort = 7856;
    private const int MaxPort = 65535;
    private const int BrokerReadyAttempts = 30;
    private static readonly TimeSpan BrokerReadyDelay = TimeSpan.FromMilliseconds(500);
    private const int LaunchLockAttempts = 50;
    private static readonly TimeSpan LaunchLockRetryDelay = TimeSpan.FromMilliseconds(100);

    private static readonly JsonSerializerOptions DiscoveryJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true,
    };

    public static int GetPort()
    {
        var envPort = Environment.GetEnvironmentVariable("PEERS_PORT");
        if (string.IsNullOrWhiteSpace(envPort))
            return DefaultPort;
        if (!int.TryParse(envPort, out var port) || port is < 1 or > MaxPort)
            throw new InvalidOperationException(
                $"Invalid PEERS_PORT value '{envPort}'. Expected an integer between 1 and {MaxPort}.");

        return port;
    }

    public static BrokerInfo? ReadDiscovery()
    {
        var discoveryFile = PeersPaths.GetDiscoveryFile();
        if (!File.Exists(discoveryFile))
            return null;

        try
        {
            var json = File.ReadAllText(discoveryFile);
            return JsonSerializer.Deserialize<BrokerInfo>(json, DiscoveryJsonOptions);
        }
        catch
        {
            return null;
        }
    }

    public static async Task<BrokerStatus> GetStatusAsync(CancellationToken ct = default)
    {
        var stateDirectory = PeersPaths.GetStateDirectory();
        var discoveryFile = PeersPaths.GetDiscoveryFile();
        var discovery = ReadDiscovery();
        var port = discovery?.Port ?? GetPort();
        var isHealthy = await IsHealthyAsync(port, ct);

        return new BrokerStatus
        {
            Port = port,
            IsHealthy = isHealthy,
            DiscoveryFileExists = File.Exists(discoveryFile),
            HasStaleDiscovery = discovery is not null && !isHealthy,
            Discovery = discovery,
            StateDirectory = stateDirectory,
            DiscoveryFile = discoveryFile,
        };
    }

    public static async Task<BrokerInfo> EnsureBrokerRunningAsync(CancellationToken ct = default)
    {
        Directory.CreateDirectory(PeersPaths.GetStateDirectory());

        var status = await GetStatusAsync(ct);
        if (status.IsHealthy && status.Discovery is not null)
            return status.Discovery;

        await using var lockStream = await AcquireLaunchLockAsync(ct);

        status = await GetStatusAsync(ct);
        if (!status.IsHealthy)
        {
            var brokerProject = FindBrokerProject();
            if (brokerProject is null)
                throw new InvalidOperationException("Cannot find Peers.Broker project");

            using var process = Process.Start(CreateBrokerStartInfo(brokerProject));
            if (process is null)
                throw new InvalidOperationException("Failed to start broker process");

            process.OutputDataReceived += static (_, _) => { };
            process.ErrorDataReceived += static (_, _) => { };
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            return await WaitForBrokerReadyAsync(status.Port, process, ct);
        }

        return await WaitForBrokerReadyAsync(status.Port, launchedProcess: null, ct);
    }

    public static async Task<bool> IsHealthyAsync(int port, CancellationToken ct = default)
    {
        try
        {
            using var http = new HttpClient
            {
                BaseAddress = new Uri($"http://127.0.0.1:{port}"),
                Timeout = TimeSpan.FromSeconds(2),
            };
            using var response = await http.GetAsync("/health", ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    private static async Task<BrokerInfo> WaitForBrokerReadyAsync(int port, Process? launchedProcess, CancellationToken ct)
    {
        for (var attempt = 0; attempt < BrokerReadyAttempts; attempt++)
        {
            ct.ThrowIfCancellationRequested();

            if (launchedProcess is not null && launchedProcess.HasExited)
                throw new InvalidOperationException(
                    $"Broker exited before becoming healthy (exit code {launchedProcess.ExitCode}).");

            var discovery = ReadDiscovery();
            if (discovery is not null && discovery.Port == port && await IsHealthyAsync(discovery.Port, ct))
                return discovery;

            await Task.Delay(BrokerReadyDelay, ct);
        }

        throw new InvalidOperationException(
            $"Broker failed to publish a healthy discovery record on port {port} within " +
            $"{BrokerReadyAttempts * BrokerReadyDelay.TotalMilliseconds / 1000:0.#} seconds.");
    }

    private static async Task<FileStream> AcquireLaunchLockAsync(CancellationToken ct)
    {
        var lockFile = PeersPaths.GetLockFile();
        IOException? lastError = null;

        for (var attempt = 0; attempt < LaunchLockAttempts; attempt++)
        {
            try
            {
                return new FileStream(lockFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException ex) when (attempt < LaunchLockAttempts - 1)
            {
                lastError = ex;
                await Task.Delay(LaunchLockRetryDelay, ct);
            }
            catch (IOException ex)
            {
                lastError = ex;
                break;
            }
        }

        throw new IOException(
            $"Failed to acquire broker launch lock '{lockFile}' after {LaunchLockAttempts} attempts.",
            lastError);
    }

    private static ProcessStartInfo CreateBrokerStartInfo(string brokerProject)
    {
        var brokerDir = Path.GetDirectoryName(brokerProject)
            ?? throw new InvalidOperationException("Cannot determine Peers.Broker project directory");

        var brokerExe = FindLatestBuildArtifact(
            brokerDir,
            OperatingSystem.IsWindows() ? "Peers.Broker.exe" : "Peers.Broker");
        if (brokerExe is not null)
        {
            return CreateProcessStartInfo(
                fileName: brokerExe,
                arguments: null,
                workingDirectory: brokerDir);
        }

        var brokerDll = FindLatestBuildArtifact(brokerDir, "Peers.Broker.dll");
        if (brokerDll is not null)
        {
            return CreateProcessStartInfo(
                fileName: "dotnet",
                arguments: $"\"{brokerDll}\"",
                workingDirectory: brokerDir);
        }

        return CreateProcessStartInfo(
            fileName: "dotnet",
            arguments: $"run --project \"{brokerProject}\" --no-launch-profile --no-restore",
            workingDirectory: brokerDir);
    }

    private static ProcessStartInfo CreateProcessStartInfo(string fileName, string? arguments, string workingDirectory)
    {
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = arguments ?? string.Empty,
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };

        psi.Environment["ASPNETCORE_ENVIRONMENT"] = "Production";
        psi.Environment["DOTNET_NOLOGO"] = "1";
        psi.Environment["DOTNET_SKIP_FIRST_TIME_EXPERIENCE"] = "1";
        return psi;
    }

    private static string? FindLatestBuildArtifact(string brokerDir, string fileName)
    {
        var binDir = Path.Combine(brokerDir, "bin");
        if (!Directory.Exists(binDir))
            return null;

        return Directory
            .EnumerateFiles(binDir, fileName, SearchOption.AllDirectories)
            .OrderByDescending(File.GetLastWriteTimeUtc)
            .FirstOrDefault();
    }

    private static string? FindBrokerProject()
    {
        foreach (var startDirectory in new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
        {
            var found = TryFindBrokerProject(startDirectory);
            if (found is not null)
                return found;
        }

        var envPath = Environment.GetEnvironmentVariable("PEERS_BROKER_PROJECT");
        return envPath is not null && File.Exists(envPath) ? envPath : null;
    }

    private static string? TryFindBrokerProject(string startDirectory)
    {
        var dir = Path.GetFullPath(startDirectory);

        while (true)
        {
            foreach (var candidate in new[]
            {
                Path.Combine(dir, "src", "Peers.Broker", "Peers.Broker.csproj"),
                Path.Combine(dir, "Peers.Broker", "Peers.Broker.csproj"),
            })
            {
                if (File.Exists(candidate))
                    return candidate;
            }

            var parent = Path.GetDirectoryName(dir);
            if (string.IsNullOrWhiteSpace(parent) || string.Equals(parent, dir, StringComparison.Ordinal))
                return null;

            dir = parent;
        }
    }
}
