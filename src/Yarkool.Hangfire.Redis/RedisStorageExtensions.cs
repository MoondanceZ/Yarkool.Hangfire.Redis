using Hangfire;
using Hangfire.Annotations;

namespace Yarkool.Hangfire.Redis
{
    public static class RedisStorageExtensions
    {
        public static IGlobalConfiguration<RedisStorage> UseRedisStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] IRedisClient hangfireRedisClient,
            RedisStorageOptions? options = null)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));
            if (hangfireRedisClient == null)
                throw new ArgumentNullException(nameof(hangfireRedisClient));
            var storage = new RedisStorage(hangfireRedisClient, options);
            GlobalJobFilters.Filters.Add(new HangfireSubscriber());
            return configuration.UseStorage(storage);
        }
    }
}