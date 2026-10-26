namespace Peers.Shared;

/// <summary>
/// macOS links the top-level <c>/tmp</c>, <c>/var</c> and <c>/etc</c> to <c>/private/tmp</c>,
/// <c>/private/var</c> and <c>/private/etc</c>. Git, <c>realpath</c> and <c>mktemp</c> report one
/// spelling and agents type the other, so a path under <c>/tmp</c> would fail every symlink check and
/// never match a repo root Git reported. Only these OS links are rewritten; a symlink anywhere else in
/// a path stays visible to the caller's checks.
/// </summary>
public static class SystemPaths
{
    private static readonly string[] LinkedRoots = ["tmp", "var", "etc"];

    /// <summary>
    /// <paramref name="path"/> with a leading <c>/tmp</c>, <c>/var</c> or <c>/etc</c> replaced by its
    /// <c>/private</c> target when that top-level entry is exactly the OS link to it; any other path
    /// unchanged. On Linux these are real directories, so nothing is rewritten.
    /// </summary>
    public static string Canonicalize(string path)
    {
        foreach (var name in LinkedRoots)
        {
            var link = "/" + name;
            if (path != link && !path.StartsWith(link + "/", StringComparison.Ordinal))
                continue;
            var target = new DirectoryInfo(link).LinkTarget;
            var canonical = "/private/" + name;
            return target is not null && Path.GetFullPath(target, "/") == canonical
                ? canonical + path[link.Length..]
                : path;
        }
        return path;
    }
}
