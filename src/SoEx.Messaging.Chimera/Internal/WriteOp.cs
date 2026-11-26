using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// One unit of work for the single writer of a file. All changes to a file go through this type. Thus the
/// one-writer-per-file rule holds for callers on many threads, with no lock on the file itself.
/// </summary>
internal sealed class WriteOp
{
    internal WriteOp(Func<SqliteConnection, SqliteTransaction, long> execute) => Execute = execute;

    internal Func<SqliteConnection, SqliteTransaction, long> Execute { get; }

    internal TaskCompletionSource<long> Completion { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
