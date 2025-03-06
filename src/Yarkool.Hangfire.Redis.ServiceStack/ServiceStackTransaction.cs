using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackTransaction
(
    global::ServiceStack.Redis.IRedisTransaction transaction
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
        var result = transaction.Commit();
        return result;
    }

    ~ServiceStackTransaction()
    {
        transaction.Dispose();
    }
}