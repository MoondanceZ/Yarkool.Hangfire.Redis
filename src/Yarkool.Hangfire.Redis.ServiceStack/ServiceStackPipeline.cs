namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackPipeline
(
    global::ServiceStack.Redis.Pipeline.IRedisPipeline redisPipeline
) : ServiceStackCommand(redisPipeline), IRedisPipeline
{
    private bool _disposed;
    private readonly global::ServiceStack.Redis.Pipeline.IRedisPipeline _redisPipeline = redisPipeline;

    public void Dispose()
    {
        if (_disposed)
            return;

        _redisPipeline.Dispose();
        _disposed = true;

        GC.SuppressFinalize(this);
    }

    public object?[]? Execute()
    {
        _redisPipeline.Flush();
        return GetPipelineResults();
    }

    ~ServiceStackPipeline()
    {
        _redisPipeline.Dispose();
    }
}