using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>What a delivery is doing right now.</summary>
internal enum EntryState
{
    InFlight = 0,

    /// <summary>Finished with, however it finished. Completed, deferred and dead all land here, because as far
    /// as the position is concerned there is nothing left to do about the message.</summary>
    Complete = 1,

    /// <summary>
    /// Handed back after a failure and waiting to be delivered again. The row stays only to carry the attempt
    /// count: delete it and a message that fails forever would be retried forever, with the counter starting
    /// again on every delivery.
    /// </summary>
    Available = 2,
}

/// <summary>Where a set-aside row is in its own small lifecycle.</summary>
internal enum SetAsideDelivery
{
    /// <summary>Sitting there. Nothing will deliver it until someone asks.</summary>
    Held = 0,

    /// <summary>Replay was asked for; the next read picks it up.</summary>
    Ready = 1,

    /// <summary>Being delivered now. Reset to <see cref="Ready"/> when the store is opened, since whoever held
    /// it is gone.</summary>
    InFlight = 2,
}

/// <summary>The subscriber file's schema. One file per subscriber, written only by that subscriber.</summary>
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
    /// Refuses a file a later version of this library wrote, rather than opening it and guessing at a layout
    /// this version does not know.
    /// </summary>
    /// <remarks>
    /// The same rule the topic file has had, and for a stronger reason: this file holds positions. Reading one
    /// under the wrong layout does not fail, it answers — and a wrong answer here redelivers everything or
    /// skips it. A file with no version recorded is not treated as newer: either this library has not written
    /// it yet, or something outside it has, and both are cases the schema below rebuilds into shape.
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
