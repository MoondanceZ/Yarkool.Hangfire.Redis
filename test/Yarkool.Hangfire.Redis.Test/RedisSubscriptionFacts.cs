using System.Diagnostics;
using Yarkool.Hangfire.Redis.Test.Utils;

namespace Yarkool.Hangfire.Redis.Test
{
    [CleanRedis]
    [Collection("Sequential")]
    public class RedisSubscriptionFacts
    {
        private readonly CancellationTokenSource _cts;
        private readonly RedisStorage _storage;

        public RedisSubscriptionFacts()
        {
            _cts = new CancellationTokenSource();

            var options = new RedisStorageOptions() { };
            _storage = new RedisStorage(RedisUtils.CreateClient(), options);
        }

        [Fact]
        public void Ctor_ThrowAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage", () => new RedisSubscription(null!));
        }

        [Fact]
        public void Ctor_ThrowAnException_WhenSubscriberIsNull()
        {
            Assert.Throws<ArgumentNullException>("subscriber", () => new RedisSubscription(_storage));
        }

        [Fact]
        public void WaitForJob_WaitForTheTimeout()
        {
            //Arrange
            Stopwatch sw = new Stopwatch();
            var subscription = new RedisSubscription(_storage);
            var timeout = TimeSpan.FromMilliseconds(100);
            sw.Start();

            //Act
            subscription.WaitForJob(timeout, _cts.Token);

            //Assert
            sw.Stop();
            Assert.InRange(sw.ElapsedMilliseconds, 99, 120);
        }
    }
}