using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackPipeline
(
    IRedisClientsManager redisClientsManager
) : ServiceStackCommand(redisClientsManager), IRedisPipeline
{
}