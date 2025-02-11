using SharpRedis;

namespace Yarkool.Hangfire.Redis.SharpRedis;

public class SharpRedisClient(global::SharpRedis.Redis redisClient) : SharpRedisCommand(redisClient), IRedisClient
{
    private readonly global::SharpRedis.Redis _redisClient = redisClient;

    public IRedisPipeline BeginPipeline()
    {
        ArgumentNullException.ThrowIfNull(_redisClient);

        var pipeline = _redisClient.BeginPipelining();
        return new SharpRedisPipeline(pipeline);
    }

    public IRedisTransaction BeginTransaction()
    {
        ArgumentNullException.ThrowIfNull(_redisClient);

        var transaction = _redisClient.Multi();
        return new SharpRedisTransaction(transaction);
    }
}