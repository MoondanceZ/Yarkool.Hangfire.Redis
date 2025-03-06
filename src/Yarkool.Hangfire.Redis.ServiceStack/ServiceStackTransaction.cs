namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackTransaction
(
    global::ServiceStack.Redis.IRedisTransaction transaction
) : ServiceStackCommand(transaction), IRedisTransaction
{
    private bool _disposed;
    private readonly global::ServiceStack.Redis.IRedisTransaction _transaction = transaction;

    public void Dispose()
    {
        if (_disposed)
            return;

        _transaction.Dispose();
        _disposed = true;

        GC.SuppressFinalize(this);
    }

    public object?[]? Execute()
    {
        _transaction.Commit();
        return GetTransactionResults();
    }

    ~ServiceStackTransaction()
    {
        _transaction.Dispose();
    }
}