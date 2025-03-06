namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackPipeline
(
    global::ServiceStack.Redis.Pipeline.IRedisPipeline redisPipeline
) : ServiceStackCommand(redisPipeline), IRedisPipeline
{
    private bool _disposed;
    private readonly global::ServiceStack.Redis.Pipeline.IRedisPipeline _redisPipeline1 = redisPipeline;

    public void Dispose()
    {
        if (_disposed)
            return;

        _redisPipeline1.Dispose();
        _disposed = true;

        GC.SuppressFinalize(this);
    }

    public object?[]? Execute()
    {
        _redisPipeline1.Flush();
        return GetPipelineResults();
    }

    ~ServiceStackPipeline()
    {
        _redisPipeline1.Dispose();
    }
}