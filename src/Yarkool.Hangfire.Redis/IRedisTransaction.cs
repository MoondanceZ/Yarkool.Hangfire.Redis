namespace Yarkool.Hangfire.Redis;

public interface IRedisTransaction : IRedisCommand<IRedisTransaction>, IRedisCommandCall, IDisposable
{
    object?[]? Execute();
}