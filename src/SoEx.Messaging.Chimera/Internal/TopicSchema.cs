using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// The topic file's schema, and the identity stamped into it. The schema is readable by anything that can open
/// SQLite, which is a feature, so treat it as an interface and change it deliberately.
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
    /// Refuses a file a later version of this library wrote, rather than opening it and appending into a
    /// layout this version does not know.
    /// </summary>
    /// <remarks>
    /// The reader has always refused one (<see cref="TopicReader.TryOpen"/>), and refusing there alone left
    /// the gap on the side that can do damage: the publisher holds the only connection that writes, and
    /// without this it created its tables into whatever was there and stamped the version back down to its
    /// own. A file with no version recorded is not treated as newer, the same leniency the subscriber file
    /// has and for the same reason: either this library has not written it yet, or something outside it has,
    /// and the schema below builds both into shape.
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
    /// Reads identity from a log opened read-only, or null if the file is not ready to be read yet.
    /// </summary>
    /// <remarks>
    /// A publisher creates the file when it opens the connection and commits the schema afterwards, so there is
    /// a window where the file exists and contains nothing. A subscriber that arrives in that window must wait,
    /// not fail: the topic is being created, which is a normal thing for it to be doing. Because the schema and
    /// the identity are written in one transaction, an identity that is readable means the whole schema is.
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

        // Tolerant in the same direction as the guard above: a version nobody can parse is treated as absent
        // rather than thrown from, because the file this reads may be one something else wrote and a caller
        // asking whether a log is ready should get an answer from the library's own vocabulary.
        string? version = Get(connection, null, MetaSchemaVersion);
        return (uuid, int.TryParse(version, out int parsed) ? parsed : 0);
    }
}
