using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

public class ServiceStackClient(IRedisClientsManager redisClientsManager) : ServiceStackCommand(redisClientsManager), IRedisClient
{
    private readonly IRedisClientsManager _redisClientsManager = redisClientsManager;

    public IRedisPipeline BeginPipeline()
    {
        ArgumentNullException.ThrowIfNull(_redisClientsManager);

        return new ServiceStackPipeline(_redisClientsManager);
    }

    public IRedisTransaction BeginTransaction()
    {
        ArgumentNullException.ThrowIfNull(_redisClientsManager);

        return new ServiceStackTransaction(_redisClientsManager);
    }
}