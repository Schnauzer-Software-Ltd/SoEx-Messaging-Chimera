using System.Threading.Channels;
using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// The one writer for one file. Callers queue work and await their own result. The loop drains all waiting work and
/// commits it in a single transaction. Thus a burst of concurrent appends costs one commit for the burst, and not
/// one commit for each append.
/// </summary>
/// <remarks>
/// A batch is all-or-nothing. If one operation throws, the writer rolls back the transaction, and no part of that
/// batch commits. The caller whose operation threw gets that exception unchanged, because it can be a deliberate
/// refusal that the caller expects to catch. The other callers get a clear message: the failure of a different
/// write took their write with it. This is true, and those callers could not cause it.
/// </remarks>
internal sealed class SingleWriter : IAsyncDisposable
{
    private readonly Channel<WriteOp> _queue = Channel.CreateUnbounded<WriteOp>(
        new UnboundedChannelOptions { SingleReader = true });

    private readonly SqliteConnection _connection;
    private readonly int _maxBatch;
    private readonly Task _loop;
    private volatile Exception? _fault;

    internal SingleWriter(SqliteConnection connection, int maxBatch)
    {
        _connection = connection;
        _maxBatch = maxBatch;
        _loop = Task.Factory.StartNew(RunAsync, TaskCreationOptions.LongRunning).Unwrap();
    }

    /// <summary>
    /// Queues work and completes when the work is durable. The method checks cancellation only before it queues the
    /// work. Queued work always gets an attempt. A report of a cancelled append that in fact committed is false,
    /// and the caller cannot detect it.
    /// </summary>
    internal async ValueTask<long> EnqueueAsync(
        Func<SqliteConnection, SqliteTransaction, long> execute, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_fault is { } stopped)
            throw Stopped(stopped);

        var op = new WriteOp(execute);
        if (!_queue.Writer.TryWrite(op))
            throw _fault is { } closed ? Stopped(closed) : new ChimeraException("The store is closed.");
        return await op.Completion.Task.ConfigureAwait(false);
    }

    private async Task RunAsync()
    {
        var batch = new List<WriteOp>(_maxBatch);
        try
        {
            while (await _queue.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                batch.Clear();
                while (batch.Count < _maxBatch && _queue.Reader.TryRead(out WriteOp? op))
                    batch.Add(op);
                if (batch.Count == 0)
                    continue;

                RunBatch(batch);
            }
        }
        catch (Exception ex)
        {
            // RunBatch answers a failed batch, so no exception from the code above is expected here. A loop that
            // ends silently is the worst failure of this class. Every caller in the batch waits on a result that
            // never comes, and every later caller queues work that nothing drains. Thus any other end of the loop
            // becomes an answer for every caller.
            Poison(ex, batch);
        }
    }

    /// <summary>
    /// Stops the writer and tells every waiting caller. The loop calls it only when the loop itself has failed. The
    /// writer cannot continue from that state: the condition of the connection is unknown, and nothing is left to
    /// run the queue.
    /// </summary>
    private void Poison(Exception cause, List<WriteOp> batch)
    {
        _fault = cause;
        _queue.Writer.TryComplete(cause);

        foreach (WriteOp op in batch)
            op.Completion.TrySetException(Stopped(cause));
        while (_queue.Reader.TryRead(out WriteOp? stranded))
            stranded.Completion.TrySetException(Stopped(cause));
    }

    private static ChimeraException Stopped(Exception cause) =>
        new("The store's writer has stopped and cannot take any more writes. Nothing queued after it stopped " +
            "was committed.", cause);

    private void RunBatch(List<WriteOp> batch)
    {
        var results = new long[batch.Count];
        int offender = -1;
        Exception? cause = null;

        // BEGIN is inside the try because it can fail too. A broken connection or a disk error can surface at
        // BEGIN, before the next statement. In an earlier version, a throw from here left the loop, and every
        // waiting caller got no answer.
        SqliteTransaction? transaction = null;
        try
        {
            transaction = _connection.BeginTransaction();

            for (int i = 0; i < batch.Count; i++)
            {
                try
                {
                    results[i] = batch[i].Execute(_connection, transaction);
                }
                catch (Exception ex)
                {
                    offender = i;
                    cause = ex;
                    break;
                }
            }

            if (cause is null)
                transaction.Commit();
            else
                Rollback(transaction);
        }
        catch (Exception ex)
        {
            // BEGIN or COMMIT failed. No operation in the batch caused it, so every caller gets the same answer.
            cause = ex;
            offender = -1;
            if (transaction is not null)
                Rollback(transaction);
        }
        finally
        {
            transaction?.Dispose();
        }

        if (cause is null)
        {
            for (int i = 0; i < batch.Count; i++)
                batch[i].Completion.TrySetResult(results[i]);
            return;
        }

        for (int i = 0; i < batch.Count; i++)
        {
            batch[i].Completion.TrySetException(i == offender
                ? cause
                : new ChimeraException(
                    "The write was rolled back because another write committed in the same batch failed.", cause));
        }
    }

    private static void Rollback(SqliteTransaction transaction)
    {
        try { transaction.Rollback(); }
        catch { /* the connection already has a fault. A second error adds no useful information. */ }
    }

    public async ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        try { await _loop.ConfigureAwait(false); }
        catch { /* a failed loop has already answered every caller that it left waiting */ }

        while (_queue.Reader.TryRead(out WriteOp? stranded))
            stranded.Completion.TrySetException(new ChimeraException("The store was closed before the write ran."));

        _connection.Dispose();
    }
}
