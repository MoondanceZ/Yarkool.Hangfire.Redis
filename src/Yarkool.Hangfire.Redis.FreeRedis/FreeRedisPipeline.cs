using FreeRedis;

namespace Yarkool.Hangfire.Redis.FreeRedis;

internal class FreeRedisPipeline
(
    RedisClient.PipelineHook redisPipeline
) : FreeRedisCommand(redisPipeline), IRedisPipeline
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

    ~FreeRedisPipeline()
    {
        redisPipeline.Dispose();
    }
}