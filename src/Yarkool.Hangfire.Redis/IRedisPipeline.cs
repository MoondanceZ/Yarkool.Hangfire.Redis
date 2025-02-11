namespace Yarkool.Hangfire.Redis;

public interface IRedisPipeline : IRedisCommand<IRedisPipeline>, IRedisCommandCall, IDisposable
{
    object?[]? Execute();
}