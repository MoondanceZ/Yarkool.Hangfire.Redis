using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackPipeline
(
    IRedisClientsManager redisClientsManager
) : ServiceStackCommand(redisClientsManager), IRedisPipeline
{
    private readonly global::ServiceStack.Redis.Pipeline.IRedisPipeline _redisPipeline = redisPipeline;
    private List<string> resultList = new List<string>();
    private bool _disposed;

    public void Dispose()
    {
        if (_disposed)
            return;

        _redisPipeline.Dispose();
        _redisClient.Dispose();
        _disposed = true;

        GC.SuppressFinalize(this);
    }

    public void AddCommand(Action<IRedisPipeline> action) => action(this);

    public void AddCommand<T>(Func<IRedisPipeline, T> func) => func(this);

    public object?[]? Execute() => redisPipeline.Flush();

    ~ServiceStackPipeline()
    {
        redisPipeline.Dispose();
    }
}