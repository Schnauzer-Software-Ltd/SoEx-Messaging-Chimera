using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>The current state of a delivery.</summary>
internal enum EntryState
{
    InFlight = 0,

    /// <summary>Finished, by any path. Completed, deferred and dead messages all go to this state. For the
    /// position, nothing remains to do for the message.</summary>
    Complete = 1,

    /// <summary>
    /// Given back after a failure, and waits for a new delivery. The row stays only to hold the attempt count. If
    /// the row is deleted, a message that always fails gets retries forever, because the counter starts again on
    /// each delivery.
    /// </summary>
    Available = 2,
}

/// <summary>The state of a set-aside row in its small lifecycle.</summary>
internal enum SetAsideDelivery
{
    /// <summary>Stored. Nothing delivers it until a caller asks for it.</summary>
    Held = 0,

    /// <summary>Replay was requested. The next read picks it up.</summary>
    Ready = 1,

    /// <summary>In delivery now. The store resets it to <see cref="Ready"/> when the store opens, because the
    /// previous holder is gone.</summary>
    InFlight = 2,
}

/// <summary>The schema of the subscriber file. One file per subscriber. Only that subscriber writes it.</summary>
internal static class SubscriberSchema
{
    internal const int Version = 1;

    internal const string MetaSchemaVersion = "schema_version";

    private const string Ddl = """
        CREATE TABLE IF NOT EXISTS meta (
          key   TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        -- One row per topic this subscriber reads. log_uuid ties the position to the log it belongs to.
        -- Two positions, because the topic file holds two tables that are ordered differently: mark is the
        -- log sequence everything at or below which is settled, and (due_floor, id_floor) is the same idea
        -- for the scheduled table, whose order is due time and then id.
        CREATE TABLE IF NOT EXISTS cursor (
          topic     TEXT PRIMARY KEY,
          log_uuid  TEXT NOT NULL,
          mark      INTEGER NOT NULL,
          due_floor INTEGER NOT NULL DEFAULT 0,
          id_floor  INTEGER NOT NULL DEFAULT 0
        );

        -- The exception set: only what is not yet absorbed into a position. Bounded by the in-flight cap,
        -- because anything set aside is terminal here and lets the position move past it, and a settled row
        -- is deleted as soon as the position it belongs to reaches it.
        CREATE TABLE IF NOT EXISTS entry (
          topic    TEXT    NOT NULL,
          source   INTEGER NOT NULL,   -- EntrySource: log and scheduled ids are different number spaces
          id       INTEGER NOT NULL,
          state    INTEGER NOT NULL,   -- EntryState
          attempts INTEGER NOT NULL DEFAULT 0,
          updated  INTEGER NOT NULL,
          PRIMARY KEY (topic, source, id)
        );

        -- Deferred and dead messages. Self-contained: the payload is copied, so log retention cannot strand
        -- one and replaying it does not depend on the original record still existing.
        CREATE TABLE IF NOT EXISTS set_aside (
          aside_id    INTEGER PRIMARY KEY AUTOINCREMENT,
          kind        INTEGER NOT NULL,   -- SetAsideKind
          topic       TEXT    NOT NULL,
          origin      TEXT    NOT NULL,   -- provenance only, e.g. "log:1234"; nothing dereferences it
          msg_id      TEXT    NOT NULL,
          enqueued_at INTEGER NOT NULL,   -- original publish time, so end-to-end latency survives deferral
          aside_at    INTEGER NOT NULL,   -- when it was set aside; age-out runs from here
          delivery    INTEGER NOT NULL DEFAULT 0,  -- SetAsideDelivery
          attempts    INTEGER NOT NULL,
          reason      TEXT    NULL,
          payload     BLOB    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS ix_aside ON set_aside(topic, kind, delivery);
        """;

    internal static void Initialise(SqliteConnection connection, string subscriber)
    {
        using var transaction = connection.BeginTransaction();

        EnsureNotNewer(connection, transaction, subscriber);

        using (var ddl = Sqlite.Command(connection, transaction, Ddl))
            ddl.ExecuteNonQuery();

        using (var meta = Sqlite.Command(connection, transaction,
            $"INSERT INTO meta (key, value) VALUES ('{MetaSchemaVersion}', $v), ('subscriber', $s) " +
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value;"))
        {
            meta.Parameters.AddWithValue("$v", Version.ToString());
            meta.Parameters.AddWithValue("$s", subscriber);
            meta.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    /// <summary>
    /// Refuses a file that a later version of this library wrote. This version does not open the file, and does not
    /// guess at a layout that it does not know.
    /// </summary>
    /// <remarks>
    /// The topic file has the same rule. The reason here is stronger: this file holds positions. A read under the
    /// wrong layout does not fail. It gives an answer, and a wrong answer here redelivers everything or skips it. A
    /// file with no recorded version does not count as newer. Either this library has not written it yet, or
    /// something outside the library has. In both cases the schema below rebuilds the file into shape.
    /// </remarks>
    internal static void EnsureNotNewer(SqliteConnection connection, SqliteTransaction? transaction,
        string subscriber)
    {
        using (var exists = Sqlite.Command(connection, transaction,
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'meta';"))
        {
            if (exists.ExecuteScalar() is null)
                return;
        }

        int version;
        using (var read = Sqlite.Command(connection, transaction,
            $"SELECT value FROM meta WHERE key = '{MetaSchemaVersion}';"))
        {
            if (read.ExecuteScalar() is not string value || !int.TryParse(value, out version))
                return;
        }

        if (version > Version)
            throw new ChimeraException(
                $"The store for subscriber '{subscriber}' was written by a newer version of this library " +
                $"(schema {version}). Refusing to open it: this version does not know what its positions mean, " +
                "and reading them under the wrong layout would redeliver or skip messages without saying so.");
    }
}
