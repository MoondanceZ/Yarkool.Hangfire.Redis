namespace Yarkool.Hangfire.Redis;

public interface IRedisClient : IRedisCommand<IRedisClient>, IRedisCommandCall
{
    IRedisPipeline BeginPipeline();
    IRedisTransaction BeginTransaction();
}