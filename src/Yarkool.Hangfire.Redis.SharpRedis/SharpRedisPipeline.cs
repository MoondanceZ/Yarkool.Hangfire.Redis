using SharpRedis;

namespace Yarkool.Hangfire.Redis.SharpRedis;

internal class SharpRedisPipeline
(
    RedisPipelining pipeline
) : SharpRedisCommand(pipeline), IRedisPipeline
{
    private bool _disposed;

    public void Dispose()
    {
        if (_disposed)
            return;

        pipeline.Dispose();
        _disposed = true;

        GC.SuppressFinalize(this);
    }

    public object?[]? Execute() => pipeline.ExecutePipelining();

    ~SharpRedisPipeline()
    {
        pipeline.Dispose();
    }
}