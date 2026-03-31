namespace Peers.Shared;

public static class PeersPaths
{
    public static string GetStateDirectory()
    {
        var configuredStateDir = Environment.GetEnvironmentVariable("PEERS_STATE_DIR");
        if (!string.IsNullOrWhiteSpace(configuredStateDir))
            return EnsureWritable(configuredStateDir, "PEERS_STATE_DIR");

        var configuredHome = Environment.GetEnvironmentVariable("PEERS_HOME");
        if (!string.IsNullOrWhiteSpace(configuredHome))
            return EnsureWritable(Path.Combine(configuredHome, ".peers"), "PEERS_HOME");

        var userHome = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        if (!string.IsNullOrWhiteSpace(userHome))
        {
            var defaultDir = Path.Combine(userHome, ".peers");
            if (TryEnsureWritable(defaultDir))
                return defaultDir;
        }

        var fallbackDir = Path.Combine(Path.GetTempPath(), "peers");
        Directory.CreateDirectory(fallbackDir);
        return fallbackDir;
    }

    public static string GetDiscoveryFile() => Path.Combine(GetStateDirectory(), "broker.json");

    public static string GetLockFile() => Path.Combine(GetStateDirectory(), "broker.lock");

    public static string GetDbPath() =>
        Environment.GetEnvironmentVariable("PEERS_DB_PATH")
        ?? Path.Combine(GetStateDirectory(), "peers.db");

    private static string EnsureWritable(string path, string variableName)
    {
        if (TryEnsureWritable(path))
            return path;

        throw new InvalidOperationException(
            $"The configured {variableName} path '{path}' is not writable.");
    }

    private static bool TryEnsureWritable(string path)
    {
        try
        {
            Directory.CreateDirectory(path);

            var probePath = Path.Combine(path, $".peers-write-test-{Environment.ProcessId}-{Guid.NewGuid():N}");
            using (new FileStream(probePath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
            }
            File.Delete(probePath);

            return true;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
        catch (IOException)
        {
            return false;
        }
    }
}
