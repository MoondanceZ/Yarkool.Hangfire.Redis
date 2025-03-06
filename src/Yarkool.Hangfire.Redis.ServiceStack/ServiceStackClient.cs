using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

public class ServiceStackClient(IRedisClientsManager redisClientsManager) : ServiceStackCommand(redisClientsManager), IRedisClient
{
    public IRedisPipeline BeginPipeline()
    {
        ArgumentNullException.ThrowIfNull(redisClientsManager);

        using var redisClient = redisClientsManager.GetClient();
        var pipeline = redisClient.CreatePipeline();
        return new ServiceStackPipeline(pipeline);
    }

    public IRedisTransaction BeginTransaction()
    {
        ArgumentNullException.ThrowIfNull(redisClientsManager);

        using var redisClient = redisClientsManager.GetClient();
        var transaction = redisClient.CreateTransaction();
        return new ServiceStackTransaction(transaction);
    }
}