using FreeRedis;

namespace Yarkool.Hangfire.Redis.FreeRedis;

internal class FreeRedisTransaction
(
    RedisClient.TransactionHook transaction
) : FreeRedisCommand(transaction), IRedisTransaction
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

    ~FreeRedisTransaction()
    {
        transaction.Dispose();
    }
}