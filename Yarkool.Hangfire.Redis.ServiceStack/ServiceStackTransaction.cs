using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackTransaction
(
    RedisClient.TransactionHook transaction
) : ServiceStackCommand(transaction), IRedisTransaction
{
    private bool _disposed;

    public void Dispose()
    {
        if (_disposed)
            return;

        transaction.Dispose();
        _disposed = true;

        GC.SuppressFinalize(this);
    }

    public object?[]? Execute()
    {
        var result = transaction.Exec();
        return result;
    }

    ~ServiceStackTransaction()
    {
        transaction.Dispose();
    }
}