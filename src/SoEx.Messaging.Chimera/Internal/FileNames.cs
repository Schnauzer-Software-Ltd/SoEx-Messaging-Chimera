using System.Text;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// Turns a topic name or a subscriber name into a filename. The caller chooses the names, as with a queue name in
/// the Service Bus SDK. This class makes each name safe to put on disk. It never maps two different names onto one
/// file.
/// </summary>
/// <remarks>
/// The escaping is more careful than the characters alone need. It includes letter case, because Windows and macOS
/// default to filesystems that ignore case. On those filesystems, two names that differ only in case are one file.
/// Two publishers then write one log, and subscribers get the messages of a different topic. This occurs on
/// developer machines, and not on the Linux machine that runs the tests. Each character outside the safe set
/// becomes up to five characters in the escaped name. A long name with many of these
/// characters can come near a Windows path limit, although the name limit here is 128.
/// </remarks>
internal static class FileNames
{
    private const int MaxNameLength = 128;

    internal static string TopicFile(string root, string topic) =>
        Full(root, Sanitise(topic, nameof(topic)) + ".topic.db");

    internal static string SubscriberFile(string root, string subscriber) =>
        Full(root, Sanitise(subscriber, nameof(subscriber)) + ".subscriber.db");

    /// <summary>
    /// One spelling per file. The path also identifies the file to the guard. The guard decides if in-flight
    /// deliveries belong to a process that has stopped or to a subscription that runs now. Two components in one
    /// host can write their options with different spellings: "/var/events" and "/var/./events", or a relative root
    /// to the same directory. Without this step, the two components name one file twice and miss each other. The
    /// deliveries that one component holds then go to the other. Two spellings that meet through a symlink still
    /// get past this step. For this reason, one writer per file is a rule. This class cannot guarantee it.
    /// </summary>
    private static string Full(string root, string file) => Path.GetFullPath(Path.Combine(root, file));

    /// <summary>
    /// The key for one file. The guard uses it to tell a subscription that runs now from one that has stopped. Two
    /// spellings of one path must give the same key. If they give different keys, the guard misses, and the second
    /// component to open the file resets deliveries that the handlers of the first component hold.
    /// <see cref="Full"/> resolves dot segments and relative roots. It cannot resolve case, because this class controls
    /// only the names. The root directory belongs to the caller. Name escaping and path folding are one rule, read
    /// from opposite ends: one name must never become two files, and one file must never become two keys.
    /// </summary>
    internal static string Identity(string fullPath) => Identity(fullPath, IgnoresCase(fullPath));

    /// <summary>Applies the fold. The decision to fold is a separate step.</summary>
    internal static string Identity(string fullPath, bool ignoresCase) =>
        ignoresCase ? fullPath.ToLowerInvariant() : fullPath;

    /// <summary>
    /// Whether the filesystem that holds this file ignores case. The method asks the filesystem. It looks for the
    /// same file under a spelling that only a case-insensitive filesystem matches.
    /// </summary>
    /// <remarks>
    /// An assumption from the operating system is wrong in both directions. The worse error is to fold a path that
    /// the filesystem distinguishes. Two subscriber files then get one key, and the second one to open takes the
    /// deliveries of the first as its own. Windows and macOS default to filesystems that ignore case, and neither
    /// guarantees it. A case-insensitive mount on Linux is ordinary. Every spelling of one file gives the same
    /// answer, so the fold is stable for any spelling. The file must exist for this check. Every caller here opens
    /// the file first.
    /// </remarks>
    internal static bool IgnoresCase(string fullPath)
    {
        string swapped = string.Create(fullPath.Length, fullPath, static (span, path) =>
        {
            for (int i = 0; i < path.Length; i++)
                span[i] = char.IsUpper(path[i]) ? char.ToLowerInvariant(path[i]) : char.ToUpperInvariant(path[i]);
        });

        // A path without letters has only one spelling, so there is nothing to fold.
        return !string.Equals(swapped, fullPath, StringComparison.Ordinal) && File.Exists(swapped);
    }

    private static string Sanitise(string name, string what)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException($"A {what} name cannot be empty.", what);
        if (name.Length > MaxNameLength)
            throw new ArgumentException($"A {what} name cannot exceed {MaxNameLength} characters.", what);

        // The safe set is lower-case ASCII letters, digits, '-' and '_'. Each character outside it becomes an escape
        // that holds the original code point. An upper-case ASCII letter becomes a marker and its lower-case form. This keeps ordinary names
        // readable on disk. A code point for each capital makes them hard to read. Each escape starts with a
        // character that is itself escaped when it occurs in a name. Thus the mapping stays one-to-one: no two
        // names can give the same filename, on a filesystem that ignores case or on one that does not.
        var builder = new StringBuilder(name.Length);
        foreach (char c in name)
        {
            if (char.IsAsciiLetterLower(c) || char.IsAsciiDigit(c) || c is '-' or '_')
                builder.Append(c);
            else if (char.IsAsciiLetterUpper(c))
                builder.Append('^').Append(char.ToLowerInvariant(c));
            else
                builder.Append('~').Append(((int)c).ToString("x4"));
        }

        return builder.ToString();
    }
}
