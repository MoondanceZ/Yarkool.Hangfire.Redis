using Yarkool.Hangfire.Redis.Test.Utils;

namespace Yarkool.Hangfire.Redis.Test
{
    [Collection("Sequential")]
    public class RedisTest
    {
        private readonly IRedisClient _redis;

        public RedisTest()
        {
            _redis = RedisUtils.CreateClient();
        }

        [Fact]
        [CleanRedis]
        public void RedisSampleTest()
        {
            var defaultValue = _redis.Get("samplekey");
            Assert.True(string.IsNullOrEmpty(defaultValue));
        }
    }
}