using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

public class ServiceStackClient(IRedisClientsManager redisClientsManager) : ServiceStackCommand(redisClientsManager), IRedisClient
{
    private readonly IRedisClientsManager _redisClientsManager = redisClientsManager;

    public IRedisPipeline BeginPipeline()
    {
        ArgumentNullException.ThrowIfNull(_redisClientsManager);

        using var redisClient = _redisClientsManager.GetClient();
        var pipeline = redisClient.CreatePipeline();
        return new ServiceStackPipeline(pipeline);
    }

    public IRedisTransaction BeginTransaction()
    {
        ArgumentNullException.ThrowIfNull(_redisClientsManager);

        using var redisClient = _redisClientsManager.GetClient();
        var transaction = redisClient.CreateTransaction();
        return new ServiceStackTransaction(transaction);
    }
}