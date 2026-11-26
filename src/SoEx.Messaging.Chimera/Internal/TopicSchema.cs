using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// The schema of the topic file, and the identity stamped into it. Any tool that can open SQLite can read the
/// schema. This is a feature. Treat the schema as an interface, and change it deliberately.
/// </summary>
internal static class TopicSchema
{
    internal const int Version = 1;

    internal const string MetaSchemaVersion = "schema_version";
    internal const string MetaTopic = "topic";
    internal const string MetaLogUuid = "log_uuid";

    private const string Ddl = """
        CREATE TABLE IF NOT EXISTS meta (
          key   TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS log (
          seq         INTEGER PRIMARY KEY AUTOINCREMENT,
          msg_id      TEXT    NOT NULL,
          enqueued_at INTEGER NOT NULL,
          size        INTEGER NOT NULL,
          payload     BLOB    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scheduled (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          msg_id      TEXT    NOT NULL,
          enqueued_at INTEGER NOT NULL,
          due_at      INTEGER NOT NULL,
          size        INTEGER NOT NULL,
          payload     BLOB    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS ix_scheduled_due ON scheduled(due_at);
        """;

    /// <summary>Creates the schema if absent and returns the log's identity.</summary>
    internal static string Initialise(SqliteConnection connection, string topic)
    {
        using (var transaction = connection.BeginTransaction())
        {
            EnsureNotNewer(connection, transaction, topic);

            using (var ddl = Sqlite.Command(connection, transaction, Ddl))
                ddl.ExecuteNonQuery();

            Put(connection, transaction, MetaSchemaVersion, Version.ToString());
            Put(connection, transaction, MetaTopic, topic);

            string? uuid = Get(connection, transaction, MetaLogUuid);
            if (uuid is null)
            {
                uuid = Guid.NewGuid().ToString("n");
                Put(connection, transaction, MetaLogUuid, uuid);
            }

            transaction.Commit();
            return uuid;
        }
    }

    /// <summary>
    /// Refuses a file that a later version of this library wrote. This version does not know that layout, and does
    /// not open the file or append into it.
    /// </summary>
    /// <remarks>
    /// The reader always refused such a file (<see cref="TopicReader.TryOpen"/>). A refusal there alone left a gap
    /// on the side that can do damage. The publisher holds the only connection that writes. Without this check, the
    /// publisher created its tables into the existing layout and stamped the version back down to its own. A file
    /// with no recorded version does not count as newer. The subscriber file has the same leniency, for the same
    /// reason: either this library has not written the file yet, or something outside the library has. The schema
    /// below brings both into shape.
    /// </remarks>
    internal static void EnsureNotNewer(SqliteConnection connection, SqliteTransaction? transaction, string topic)
    {
        using (var exists = Sqlite.Command(connection, transaction,
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'meta';"))
        {
            if (exists.ExecuteScalar() is null)
                return;
        }

        if (Get(connection, transaction, MetaSchemaVersion) is not { } recorded
            || !int.TryParse(recorded, out int version))
        {
            return;
        }

        if (version > Version)
            throw new ChimeraException(
                $"The file for topic '{topic}' was written by a newer version of this library " +
                $"(schema {version}). Refusing to open it for writing: this version does not know what its " +
                "records mean, and appending into a layout it has guessed at would corrupt them.");
    }

    internal static string? Get(SqliteConnection connection, SqliteTransaction? transaction, string key)
    {
        using var command = Sqlite.Command(connection, transaction, "SELECT value FROM meta WHERE key = $k;");
        command.Parameters.AddWithValue("$k", key);
        return command.ExecuteScalar() as string;
    }

    private static void Put(SqliteConnection connection, SqliteTransaction? transaction, string key, string value)
    {
        using var command = Sqlite.Command(connection, transaction,
            "INSERT INTO meta (key, value) VALUES ($k, $v) ON CONFLICT(key) DO UPDATE SET value = excluded.value;");
        command.Parameters.AddWithValue("$k", key);
        command.Parameters.AddWithValue("$v", value);
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Reads the identity from a log opened read-only. Returns null if the file is not ready to read.
    /// </summary>
    /// <remarks>
    /// A publisher creates the file when it opens the connection, and commits the schema after that. Thus, for a
    /// period, the file exists and is empty. A subscriber that arrives in that period must wait, and must not fail.
    /// The topic is in creation, which is a normal state. The schema and the identity are written in one
    /// transaction. Thus a readable identity means that the whole schema is readable.
    /// </remarks>
    internal static (string Uuid, int SchemaVersion)? TryReadIdentity(SqliteConnection connection)
    {
        using (var exists = Sqlite.Command(connection, null,
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'meta';"))
        {
            if (exists.ExecuteScalar() is null)
                return null;
        }

        string? uuid = Get(connection, null, MetaLogUuid);
        if (uuid is null)
            return null;

        // Tolerant in the same direction as the guard above. A version that nobody can parse counts as absent, and
        // the method does not throw. The file can be one that something else wrote. A caller that asks if a log is
        // ready gets an answer in the terms of this library.
        string? version = Get(connection, null, MetaSchemaVersion);
        return (uuid, int.TryParse(version, out int parsed) ? parsed : 0);
    }
}
