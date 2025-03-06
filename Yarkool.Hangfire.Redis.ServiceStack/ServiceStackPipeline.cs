using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackPipeline
(
    RedisClient.PipelineHook redisPipeline
) : ServiceStackCommand(redisPipeline), IRedisPipeline
{
    private bool _disposed;

    public void Dispose()
    {
        if (_disposed)
            return;

        redisPipeline.Dispose();
        _disposed = true;

        GC.SuppressFinalize(this);
    }

    public object?[]? Execute() => redisPipeline.EndPipe();

    ~ServiceStackPipeline()
    {
        redisPipeline.Dispose();
    }
}