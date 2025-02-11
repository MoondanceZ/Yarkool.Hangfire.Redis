using Yarkool.Hangfire.Redis.FreeRedis;

namespace Yarkool.Hangfire.Redis.Test.Utils
{
    public static class RedisUtils
    {
        public static IRedisClient CreateClient()
        {
            return new FreeRedisClient(null!);
        }
    }
}