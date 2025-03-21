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
            using (RedisLock.Acquire(_redisClient, _storage.GetRedisKey("expired-jobs-watcher:execute:lock"), _checkInterval))
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

                        using var checkKeyPipeline = _redisClient.BeginPipeline();
                        foreach (var jobId in jobIds)
                        {
                            checkKeyPipeline.Exists(_storage.GetRedisKey($"job:{jobId}"));
                        }

                        var result = checkKeyPipeline.Execute()?.Select(Convert.ToBoolean).ToList() ?? [];

                        keysToRemove.AddRange(jobIds.Select((x, index) => new
                        {
                            JobId = x,
                            Exist = result[index]
                        }).Where(x => x.Exist).Select(x => x.JobId).ToList());
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
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }
    }
}