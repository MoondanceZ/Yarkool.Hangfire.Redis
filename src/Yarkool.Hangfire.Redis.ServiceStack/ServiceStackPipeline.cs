using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackPipeline
(
    global::ServiceStack.Redis.Pipeline.IRedisPipeline redisPipeline
) : ServiceStackCommand(redisPipeline), IRedisPipeline
{
    private List<string> resultList = new List<string>();
    private bool _disposed;

    public void Dispose()
    {
        if (_disposed)
            return;

        redisPipeline.Dispose();
        _disposed = true;

        GC.SuppressFinalize(this);
    }

    public object?[]? Execute() => redisPipeline.Flush();

    ~ServiceStackPipeline()
    {
        redisPipeline.Dispose();
    }
}