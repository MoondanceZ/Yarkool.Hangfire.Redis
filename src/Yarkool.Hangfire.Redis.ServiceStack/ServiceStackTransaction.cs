using ServiceStack.Redis;

namespace Yarkool.Hangfire.Redis.ServiceStack;

internal class ServiceStackTransaction
(
    IRedisClientsManager redisClientsManager
) : ServiceStackCommand(redisClientsManager), IRedisTransaction
{
}