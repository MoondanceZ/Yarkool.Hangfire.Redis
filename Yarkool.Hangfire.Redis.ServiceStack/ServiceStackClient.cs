using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

public class ServiceStackClient(IRedisClientsManager redisClientsManager) : ServiceStackCommand(redisClient), IRedisClient
{
    private readonly RedisClient _redisClient = redisClient;

    public IRedisPipeline BeginPipeline()
    {
        ArgumentNullException.ThrowIfNull(_redisClient);

        var pipeline = _redisClient.StartPipe();
        return new ServiceStackPipeline(pipeline);
    }

    public IRedisTransaction BeginTransaction()
    {
        ArgumentNullException.ThrowIfNull(_redisClient);

        var transaction = _redisClient.Multi();
        return new ServiceStackTransaction(transaction);
    }
}