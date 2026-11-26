using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>Connection construction and the pragmas every connection in this library depends on.</summary>
internal static class Sqlite
{
    internal static SqliteConnection OpenWriter(string path, ChimeraOptions options)
    {
        // This check runs when the writer opens, for the same reason as the busy timeout check: a bad value fails
        // quietly. Earlier versions clamped a batch size below one to one. The caller then got a different setting
        // from the one written, and no message told them. A burst of concurrent appends then cost one commit for
        // each append, where a batch costs one commit for all of them. That is a throughput cliff with no signal.
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
    /// Opens a reader. The mode flag stops this connection from a change to database content. SQLite still creates
    /// the -wal and -shm sidecars, so the process needs write permission on the directory.
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
    /// The busy wait that every connection here depends on. The method checks it because a bad value fails quietly.
    /// SQLite takes zero or less as a disabled busy handler. A connection that meets a lock, such as a checkpoint
    /// or a commit from a different connection, then fails at once with SQLITE_BUSY. It does not wait, and it fails
    /// at the moment when the file is busy, which is when the wait matters. A positive timeout shorter than one
    /// millisecond truncates to the same value, so the method rounds it up.
    /// </summary>
    private static int BusyMilliseconds(ChimeraOptions options)
    {
        if (options.BusyTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BusyTimeout,
                "The busy timeout must be greater than zero. At zero or less, SQLite turns off its busy " +
                "handler. Each lock then fails the connection at once.");
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

    /// <summary>Time-ordered and globally unique. An id sorts in a useful order, and its form is different from a
    /// position, so nobody mistakes one for the other.</summary>
    internal static string NewMessageId() => Guid.CreateVersion7().ToString("n");
}
