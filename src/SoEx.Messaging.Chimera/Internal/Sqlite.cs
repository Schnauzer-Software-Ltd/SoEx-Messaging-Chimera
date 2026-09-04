using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>Connection construction and the pragmas every connection in this library depends on.</summary>
internal static class Sqlite
{
    internal static SqliteConnection OpenWriter(string path, ChimeraOptions options)
    {
        // Checked here rather than where a batch is drained, for the same reason the busy timeout is: it
        // fails quietly. A batch size below one used to be clamped to one, so the setting a caller wrote was
        // not the setting they got and nothing said so — a burst of concurrent appends then cost one commit
        // each rather than one between them, which is a throughput cliff with no signal at the bottom of it.
        if (options.MaxAppendBatch < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxAppendBatch,
                "At least one operation must be committed at a time.");
        }

        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = false,
        }.ToString());

        connection.Open();
        Execute(connection, "PRAGMA journal_mode=WAL;");
        Execute(connection, options.Durability == Durability.PowerLoss
            ? "PRAGMA synchronous=FULL;"
            : "PRAGMA synchronous=NORMAL;");
        Execute(connection, $"PRAGMA busy_timeout={BusyMilliseconds(options)};");
        return connection;
    }

    /// <summary>
    /// Opens a reader. The mode flag stops this connection writing database content; it does not stop SQLite
    /// creating the -wal and -shm sidecars, so the process still needs write permission on the directory.
    /// </summary>
    internal static SqliteConnection OpenReader(string path, ChimeraOptions options)
    {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
            Pooling = false,
        }.ToString());

        connection.Open();
        Execute(connection, $"PRAGMA busy_timeout={BusyMilliseconds(options)};");
        return connection;
    }

    /// <summary>
    /// The wait every connection here depends on, checked because getting it wrong fails quietly. Zero or
    /// less reaches SQLite as a disabled busy handler, so a connection that meets a lock — a checkpoint, or
    /// another connection committing — fails on the spot with SQLITE_BUSY instead of waiting the moment the
    /// file is busy, which is the moment it matters. A positive timeout shorter than a millisecond truncates
    /// to the same thing, so it is rounded up rather than turned off.
    /// </summary>
    private static int BusyMilliseconds(ChimeraOptions options)
    {
        if (options.BusyTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BusyTimeout,
                "The busy timeout must be greater than zero: zero or less disables SQLite's busy handler, so " +
                "a connection meeting a lock fails immediately rather than waiting for it.");
        }

        return (int)Math.Clamp(options.BusyTimeout.TotalMilliseconds, 1, int.MaxValue);
    }

    internal static void Execute(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    internal static SqliteCommand Command(SqliteConnection connection, SqliteTransaction? transaction, string sql)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command;
    }

    internal static long ToUnixMs(DateTimeOffset value) => value.ToUnixTimeMilliseconds();

    internal static DateTimeOffset FromUnixMs(long value) => DateTimeOffset.FromUnixTimeMilliseconds(value);

    /// <summary>Time-ordered and globally unique, so an id sorts usefully without being mistaken for a position.</summary>
    internal static string NewMessageId() => Guid.CreateVersion7().ToString("n");
}
