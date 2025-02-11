using SharpRedis;

namespace Yarkool.Hangfire.Redis.SharpRedis;

internal class SharpRedisTransaction
(
    RedisTransaction transaction
) : SharpRedisCommand(transaction), IRedisTransaction
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

    public object?[]? Execute() => transaction.Exec();

    ~SharpRedisTransaction()
    {
        transaction.Dispose();
    }
}