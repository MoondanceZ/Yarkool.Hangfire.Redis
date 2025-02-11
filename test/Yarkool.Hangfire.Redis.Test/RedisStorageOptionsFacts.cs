namespace Yarkool.Hangfire.Redis.Test
{
    [Collection("Sequential")]
    public class RedisStorageOptionsFacts
    {
        [Fact]
        public void InvisibilityTimeout_HasDefaultValue()
        {
            var options = CreateOptions();
            Assert.Equal(TimeSpan.FromMinutes(30), options.InvisibilityTimeout);
        }

        private static RedisStorageOptions CreateOptions()
        {
            return new RedisStorageOptions();
        }
    }
}