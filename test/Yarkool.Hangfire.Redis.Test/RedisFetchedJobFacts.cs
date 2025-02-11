using Hangfire.Common;
using Yarkool.Hangfire.Redis.Test.Utils;

namespace Yarkool.Hangfire.Redis.Test
{
    [Collection("Sequential")]
    public class RedisFetchedJobFacts
    {
        private const string JobId = "id";
        private const string Queue = "queue";

        private readonly RedisStorage _storage;

        public RedisFetchedJobFacts()
        {
            var options = new RedisStorageOptions() { };
            _storage = new RedisStorage(RedisUtils.CreateClient(), options);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage", () => new RedisFetchedJob(null!, JobId, Queue, DateTime.UtcNow));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenRedisIsNull()
        {
            Assert.Throws<ArgumentNullException>("redis", () => new RedisFetchedJob(_storage, JobId, Queue, DateTime.UtcNow));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            Assert.Throws<ArgumentNullException>("jobId", () => new RedisFetchedJob(_storage, null!, Queue, DateTime.UtcNow));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            Assert.Throws<ArgumentNullException>("queue",
                () => new RedisFetchedJob(_storage, JobId, null!, DateTime.UtcNow));
        }

        [Fact]
        [CleanRedis]
        public void RemoveFromQueue_RemovesJobFromTheFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");

                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, "job-id", "my-queue", fetchedAt);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(0, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact]
        [CleanRedis]
        public void RemoveFromQueue_RemovesOnlyJobWithTheSpecifiedId()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "another-job-id");

                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                redis.HSet("{hangfire}:job:another-job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, "job-id", "my-queue", fetchedAt);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
                Assert.Equal("another-job-id", redis.RPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact]
        [CleanRedis]
        public void RemoveFromQueue_DoesNotRemoveIfFetchedDoesntMatch()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");

                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, "job-id", "my-queue", fetchedAt + TimeSpan.FromSeconds(1));

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
                Assert.Equal("job-id", redis.RPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact]
        [CleanRedis]
        public void RemoveFromQueue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Fetched"));
            });
        }

        [Fact]
        [CleanRedis]
        public void RemoveFromQueue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HSet("{hangfire}:job:my-job", "Checked", JobHelper.SerializeDateTime(DateTime.UtcNow));
                var fetchedJob = new RedisFetchedJob(_storage, "my-job", "my-queue", null);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Checked"));
            });
        }

        [Fact]
        [CleanRedis]
        public void Requeue_PushesAJobBackToQueue()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));

                var fetchedJob = new RedisFetchedJob(_storage, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal("my-job", redis.RPop("{hangfire}:queue:my-queue"));
                Assert.Null(redis.LPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact]
        [CleanRedis]
        public void Requeue_PushesAJobToTheRightSide()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue", "another-job");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));

                var fetchedJob = new RedisFetchedJob(_storage, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert - RPOP
                Assert.Equal("my-job", redis.RPop("{hangfire}:queue:my-queue"));
                Assert.Null(redis.RPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact]
        [CleanRedis]
        public void Requeue_RemovesAJobFromFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));

                var fetchedJob = new RedisFetchedJob(_storage, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal(0, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact]
        [CleanRedis]
        public void Requeue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                var fetchedAt = DateTime.UtcNow;
                redis.HSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Fetched"));
            });
        }

        [Fact]
        [CleanRedis]
        public void Requeue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HSet("{hangfire}:job:my-job", "Checked", JobHelper.SerializeDateTime(DateTime.UtcNow));
                var fetchedJob = new RedisFetchedJob(_storage, "my-job", "my-queue", null);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Checked"));
            });
        }

        [Fact]
        [CleanRedis]
        public void Dispose_WithNoComplete_RequeuesAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, "my-job", "my-queue", DateTime.UtcNow);

                // Act
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:queue:my-queue"));
            });
        }

        [Fact]
        [CleanRedis]
        public void Dispose_AfterRemoveFromQueue_DoesNotRequeueAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, "my-job", "my-queue", DateTime.UtcNow);

                // Act
                fetchedJob.RemoveFromQueue();
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(0, redis.LLen("{hangfire}:queue:my-queue"));
            });
        }

        private static void UseRedis(Action<IRedisClient> action)
        {
            var redis = RedisUtils.CreateClient();
            action(redis);
        }
    }
}