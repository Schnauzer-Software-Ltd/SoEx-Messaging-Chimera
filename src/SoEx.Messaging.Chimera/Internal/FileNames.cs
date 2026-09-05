using System.Text;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// Turns a topic or subscriber name into a filename. Names are the caller's, exactly as a queue name is in the
/// Service Bus SDK, so this only has to make them safe to put on disk — and has to do it without ever mapping
/// two different names onto one file.
/// </summary>
/// <remarks>
/// The escaping is deliberately more careful than the characters alone need. Letter case is folded into it
/// because Windows and macOS default to filesystems that ignore case: two names differing only in case would
/// otherwise be one file, which means two publishers writing one log and subscribers handed another topic's
/// messages — on developer machines, and not on the Linux one the tests run on. Escaped names are longer than
/// the names they came from, up to five characters for each character outside the safe set, so a long name
/// full of them can approach a path limit on Windows even though the limit here is 128.
/// </remarks>
internal static class FileNames
{
    private const int MaxNameLength = 128;

    internal static string TopicFile(string root, string topic) =>
        Full(root, Sanitise(topic, nameof(topic)) + ".topic.db");

    internal static string SubscriberFile(string root, string subscriber) =>
        Full(root, Sanitise(subscriber, nameof(subscriber)) + ".subscriber.db");

    /// <summary>
    /// One spelling per file. The path is not only where the file is: it identifies the file to the guard
    /// that decides whether deliveries in flight belong to a process that has gone or to a subscription
    /// running now. Two components in one host writing their own options — "/var/events" and
    /// "/var/./events", or a relative root against the same directory — would otherwise name one file twice,
    /// miss each other, and hand what one of them is holding to the other. Two spellings that meet through a
    /// symlink still get past this, which is why one writer per file is a rule and not a guarantee.
    /// </summary>
    private static string Full(string root, string file) => Path.GetFullPath(Path.Combine(root, file));

    /// <summary>
    /// The key one file is known by, for the guard that tells a subscription running now from one that has
    /// gone. Two spellings of one path have to answer to the same key or that guard misses, and the second
    /// component to open the file resets deliveries the first one's handlers are holding. <see cref="Full"/>
    /// settles dot segments and relative roots; case is the spelling it cannot settle, because names are all
    /// this class controls and the root directory belongs to the caller. Escaping a name and folding a path
    /// are the same rule read from opposite ends: one name must never become two files, and one file must
    /// never become two keys.
    /// </summary>
    internal static string Identity(string fullPath) => Identity(fullPath, IgnoresCase(fullPath));

    /// <summary>The fold itself, separated from the question of whether to make it.</summary>
    internal static string Identity(string fullPath, bool ignoresCase) =>
        ignoresCase ? fullPath.ToLowerInvariant() : fullPath;

    /// <summary>
    /// Whether the filesystem holding this file ignores case — asked of the filesystem, by looking for the
    /// same file under a spelling only a filesystem that ignores case would answer to.
    /// </summary>
    /// <remarks>
    /// Assuming it from the operating system would be wrong in both directions, and folding a path the
    /// filesystem does distinguish is the worse one: two subscriber files under one key, and the second
    /// opened takes the first one's deliveries for its own. Windows and macOS default to filesystems that
    /// ignore case and neither guarantees it, and a case-insensitive mount on Linux is ordinary enough.
    /// Every spelling of one file gives the same answer, so the fold is stable however it was reached. The
    /// file has to exist to be asked about, which every caller here has already seen to by opening it.
    /// </remarks>
    internal static bool IgnoresCase(string fullPath)
    {
        string swapped = string.Create(fullPath.Length, fullPath, static (span, path) =>
        {
            for (int i = 0; i < path.Length; i++)
                span[i] = char.IsUpper(path[i]) ? char.ToLowerInvariant(path[i]) : char.ToUpperInvariant(path[i]);
        });

        // A path with no letters in it has no second spelling, and nothing to fold either way.
        return !string.Equals(swapped, fullPath, StringComparison.Ordinal) && File.Exists(swapped);
    }

    private static string Sanitise(string name, string what)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException($"A {what} name cannot be empty.", what);
        if (name.Length > MaxNameLength)
            throw new ArgumentException($"A {what} name cannot exceed {MaxNameLength} characters.", what);

        // The safe set is lower case only. Anything outside it becomes an escape carrying the original code
        // point, and an upper-case ASCII letter becomes a marker and its lower-case form — which keeps ordinary
        // names readable on disk where a code point per capital would not. Every escape starts with a
        // character that is itself escaped when it appears in a name, so the mapping stays one-to-one: no two
        // names can produce the same filename, whether or not the filesystem cares about case.
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
