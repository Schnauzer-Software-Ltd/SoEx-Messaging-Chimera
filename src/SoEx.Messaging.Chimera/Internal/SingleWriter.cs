using System.Threading.Channels;
using Microsoft.Data.Sqlite;

namespace SoEx.Messaging.Chimera.Internal;

/// <summary>
/// The one writer for one file. Callers queue work and await their own result; the loop drains whatever is
/// waiting and commits it in a single transaction, so a burst of concurrent appends costs one commit rather
/// than one each.
/// </summary>
/// <remarks>
/// A batch is all-or-nothing: if one operation throws, the transaction is rolled back and nothing in that batch
/// committed. The caller whose operation threw gets that exception unchanged, because it may well be a
/// deliberate refusal it is expected to catch. The others are told plainly that someone else's failure took
/// their write with it, which is the truth and is not something they could have caused.
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
    /// Queues work and completes when it is durable. Cancellation is checked before the work is queued and not
    /// afterwards: once it is in the queue it will be attempted, because reporting a cancelled append that in
    /// fact committed would be a lie the caller could not detect.
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
            // Nothing above should get here — a failing batch is answered inside RunBatch — but a loop that
            // ends without saying so is the worst failure this class has: every caller in the batch waits on a
            // result that will never be set, and every later caller queues work nobody will ever drain. So the
            // loop ending for any other reason is turned into an answer for everyone rather than a silence.
            Poison(ex, batch);
        }
    }

    /// <summary>
    /// Stops the writer and tells everyone waiting on it. Called only when the loop itself has failed, which
    /// is not a state anything can carry on from: the connection is in an unknown condition and there is
    /// nobody left to run the queue.
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

        // Inside the try, because it can fail too — a broken connection or a disk error can surface at BEGIN
        // rather than at the statement that follows, and a throw from here used to leave the loop and take
        // every waiting caller with it in silence.
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
            // Beginning or committing failed, so nobody in the batch is at fault and everybody gets the same
            // answer.
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
        catch { /* the connection is already unhappy; there is nothing useful to add */ }
    }

    public async ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        try { await _loop.ConfigureAwait(false); }
        catch { /* a loop that failed has already answered everyone it left waiting */ }

        while (_queue.Reader.TryRead(out WriteOp? stranded))
            stranded.Completion.TrySetException(new ChimeraException("The store was closed before the write ran."));

        _connection.Dispose();
    }
}
