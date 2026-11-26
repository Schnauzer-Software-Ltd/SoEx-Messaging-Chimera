using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// Reads the files of other subscribers to find their positions. The scan is read-only and uncoordinated. It blocks
/// nothing, and a subscriber that writes at the same time is unaffected. The one-writer-per-file rule gives this
/// property. The scan reads a small number of local files in one directory.
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

            // A position from a different incarnation of the log gives no information about this log. A count of it
            // puts a number from a different log in the report.
            if (!string.Equals(reader.GetString(1), logUuid, StringComparison.Ordinal))
                return null;

            using var meta = Sqlite.Command(connection, null, "SELECT value FROM meta WHERE key = 'subscriber';");
            string name = meta.ExecuteScalar() as string ?? Path.GetFileName(path);

            return new SubscriberPosition(name, mark, Math.Max(0, highestSequence - mark));
        }
        catch (SqliteException)
        {
            // A file under creation, or a file that this process cannot read. Retention must not fail because of a
            // subscriber that it only reports on.
            return null;
        }
    }
}
