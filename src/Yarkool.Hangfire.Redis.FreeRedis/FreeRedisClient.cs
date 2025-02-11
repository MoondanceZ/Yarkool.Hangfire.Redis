using FreeRedis;

namespace Yarkool.Hangfire.Redis.FreeRedis;

public class FreeRedisClient(RedisClient redisClient) : FreeRedisCommand(redisClient), IRedisClient
{
    private readonly RedisClient _redisClient = redisClient;

    public IRedisPipeline BeginPipeline()
    {
        ArgumentNullException.ThrowIfNull(_redisClient);

        var pipeline = _redisClient.StartPipe();
        return new FreeRedisPipeline(pipeline);
    }

    public IRedisTransaction BeginTransaction()
    {
        ArgumentNullException.ThrowIfNull(_redisClient);

        var transaction = _redisClient.Multi();
        return new FreeRedisTransaction(transaction);
    }
}