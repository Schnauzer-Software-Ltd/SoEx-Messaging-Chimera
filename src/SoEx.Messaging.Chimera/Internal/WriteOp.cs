using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// One unit of work for a file's single writer. Everything that changes a file goes through this, which is how
/// the one-writer-per-file rule survives callers on many threads without a lock on the file itself.
/// </summary>
internal sealed class WriteOp
{
    internal WriteOp(Func<SqliteConnection, SqliteTransaction, long> execute) => Execute = execute;

    internal Func<SqliteConnection, SqliteTransaction, long> Execute { get; }

    internal TaskCompletionSource<long> Completion { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
