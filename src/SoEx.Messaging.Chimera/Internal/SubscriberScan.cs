using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// Reads other subscribers' files to see where they have got to. Read-only and uncoordinated: it holds nothing
/// up, and a subscriber writing at the time is unaffected, which is exactly what the one-writer-per-file rule
/// buys. This is not a distributed read — it is a handful of local files in the same directory.
/// </summary>
internal static class SubscriberScan
{
    internal static List<SubscriberPosition> Positions(ChimeraOptions options, string topic, string logUuid,
        long highestSequence)
    {
        var positions = new List<SubscriberPosition>();
        if (!Directory.Exists(options.RootDirectory))
            return positions;

        foreach (string path in Directory.EnumerateFiles(options.RootDirectory, "*.subscriber.db"))
        {
            if (TryRead(path, options, topic, logUuid, highestSequence) is { } position)
                positions.Add(position);
        }

        positions.Sort((a, b) => string.CompareOrdinal(a.Subscriber, b.Subscriber));
        return positions;
    }

    private static SubscriberPosition? TryRead(string path, ChimeraOptions options, string topic, string logUuid,
        long highestSequence)
    {
        try
        {
            using SqliteConnection connection = Sqlite.OpenReader(path, options);

            using var cursor = Sqlite.Command(connection, null,
                "SELECT mark, log_uuid FROM cursor WHERE topic = $t;");
            cursor.Parameters.AddWithValue("$t", topic);
            using SqliteDataReader reader = cursor.ExecuteReader();
            if (!reader.Read())
                return null;   // this subscriber does not read this topic

            long mark = reader.GetInt64(0);

            // A position belonging to a different incarnation of the log says nothing about this one, and
            // counting it would put a stranger's number in the report.
            if (!string.Equals(reader.GetString(1), logUuid, StringComparison.Ordinal))
                return null;

            using var meta = Sqlite.Command(connection, null, "SELECT value FROM meta WHERE key = 'subscriber';");
            string name = meta.ExecuteScalar() as string ?? Path.GetFileName(path);

            return new SubscriberPosition(name, mark, Math.Max(0, highestSequence - mark));
        }
        catch (SqliteException)
        {
            // A file being created, or one this process cannot read. Retention must not fail over a subscriber
            // it only wanted to report on.
            return null;
        }
    }
}
