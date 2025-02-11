using Hangfire.Logging;
using Hangfire.Server;

namespace Yarkool.Hangfire.Redis
{
    #pragma warning disable 618
    internal class ExpiredJobsWatcher : IServerComponent
    #pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.For<ExpiredJobsWatcher>();

        private readonly RedisStorage _storage;
        private readonly IRedisClient _redisClient;
        private readonly TimeSpan _checkInterval;

        private static readonly string[] ProcessedKeys =
        [
            "succeeded",
            "deleted"
        ];

        public ExpiredJobsWatcher(RedisStorage storage, TimeSpan checkInterval)
        {
            if (checkInterval.Ticks <= 0)
                throw new ArgumentOutOfRangeException(nameof(checkInterval), "Check interval should be positive.");

            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _redisClient = storage.RedisClient;
            _checkInterval = checkInterval;
        }

        public override string ToString()
        {
            return GetType().ToString();
        }

        void IServerComponent.Execute(CancellationToken cancellationToken)
        {
            foreach (var key in ProcessedKeys)
            {
                var redisKey = _storage.GetRedisKey(key);

                var count = _redisClient.LLen(redisKey);
                if (count == 0)
                    continue;

                Logger.InfoFormat("Removing expired records from the '{0}' list...", key);

                const int batchSize = 100;
                var keysToRemove = new List<string>();

                for (var last = count - 1; last >= 0; last -= batchSize)
                {
                    var first = Math.Max(0, last - batchSize + 1);

                    var jobIds = _redisClient.LRange(redisKey, first, last).ToArray();
                    if (jobIds.Length == 0)
                        continue;

                    var tasks = new Task[jobIds.Length];
                    for (var i = 0; i < jobIds.Length; i++)
                    {
                        tasks[i] = _redisClient.ExistsAsync(_storage.GetRedisKey($"job:{jobIds[i]}"));
                    }

                    Task.WaitAll(tasks, cancellationToken);

                    keysToRemove.AddRange(jobIds.Where((t, i) => !((Task<bool>)tasks[i]).Result));
                }

                if (keysToRemove.Count == 0)
                    continue;

                Logger.InfoFormat("Removing {0} expired jobs from '{1}' list...", keysToRemove.Count, key);

                using var pipeline = _redisClient.BeginPipeline();
                foreach (var jobId in keysToRemove)
                {
                    pipeline.LRem(_storage.GetRedisKey(key), 0, jobId);
                }

                pipeline.Execute();
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }
    }
}