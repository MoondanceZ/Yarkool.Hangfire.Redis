namespace Yarkool.Hangfire.Redis;

public interface IRedisPipeline : IRedisCommand<IRedisPipeline>, IRedisCommandCall, IDisposable
{
    void AddCommand(Action<IRedisPipeline> action);

    void AddCommand<T>(Func<IRedisPipeline, T> func);
    
    object?[]? Execute();
}