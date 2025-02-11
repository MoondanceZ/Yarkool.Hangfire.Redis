using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Storage;

namespace Yarkool.Hangfire.Redis
{
    internal class RedisFetchedJob : IFetchedJob
    {
        private readonly RedisStorage _storage;
        private readonly IRedisClient _redisClient;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public RedisFetchedJob(
            [NotNull] RedisStorage storage,
            [NotNull] string jobId,
            [NotNull] string queue,
            [CanBeNull] DateTime? fetchedAt)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _redisClient = storage.RedisClient;
            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
            FetchedAt = fetchedAt;
        }

        public string JobId { get; }
        public string Queue { get; }
        public DateTime? FetchedAt { get; }

        private DateTime? GetFetchedValue()
        {
            return JobHelper.DeserializeNullableDateTime(_redisClient.HGet(_storage.GetRedisKey($"job:{JobId}"), "Fetched"));
        }

        public void RemoveFromQueue()
        {
            var fetchedAt = GetFetchedValue();
            using var pipeline = _redisClient.BeginPipeline();
            if (fetchedAt == FetchedAt)
            {
                RemoveFromFetchedListAsync(pipeline);
            }

            pipeline.PublishAsync(_storage.SubscriptionChannel, JobId);
            pipeline.Execute();

            _removedFromQueue = true;
        }

        public void Requeue()
        {
            var fetchedAt = GetFetchedValue();
            using var pipeline = _redisClient.BeginPipeline();
            pipeline.RPush(_storage.GetRedisKey($"queue:{Queue}"), JobId);
            if (fetchedAt == FetchedAt)
            {
                RemoveFromFetchedListAsync(pipeline);
            }

            pipeline.PublishAsync(_storage.SubscriptionChannel, JobId);
            pipeline.Execute();

            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }

        private void RemoveFromFetchedListAsync(IRedisPipeline pipeline)
        {
            pipeline.LRem(_storage.GetRedisKey($"queue:{Queue}:dequeued"), -1, JobId);
            pipeline.HDel(_storage.GetRedisKey($"job:{JobId}"), [
                "Fetched",
                "Checked"
            ]);
        }

        private void RemoveFromFetchedList(IRedisClient redisClient)
        {
            redisClient.LRem(_storage.GetRedisKey($"queue:{Queue}:dequeued"), -1, JobId);
            redisClient.HDel(_storage.GetRedisKey($"job:{JobId}"), [
                "Fetched",
                "Checked"
            ]);
        }
    }
}