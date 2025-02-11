using Hangfire;
using Hangfire.Annotations;
using Hangfire.Dashboard;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;

namespace Yarkool.Hangfire.Redis
{
    public class RedisStorage : JobStorage
    {
        private readonly RedisStorageOptions _options;
        private readonly RedisSubscription _subscription;

        private readonly Dictionary<string, bool> _features =
            new(StringComparer.OrdinalIgnoreCase)
            {
                { JobStorageFeatures.ExtendedApi, true },
                { JobStorageFeatures.JobQueueProperty, true },
                { JobStorageFeatures.Connection.BatchedGetFirstByLowest, true },
                { JobStorageFeatures.Connection.GetUtcDateTime, true },
                { JobStorageFeatures.Connection.GetSetContains, true },
                { JobStorageFeatures.Connection.LimitedGetSetCount, true },
                { JobStorageFeatures.Transaction.AcquireDistributedLock, true },
                { JobStorageFeatures.Transaction.CreateJob, true }, // overridden in constructor
                { JobStorageFeatures.Transaction.SetJobParameter, true }, // overridden in constructor 
                { JobStorageFeatures.Transaction.RemoveFromQueue(typeof(RedisFetchedJob)), true }, // overridden in constructor
                { JobStorageFeatures.Monitoring.DeletedStateGraphs, true },
                { JobStorageFeatures.Monitoring.AwaitingJobs, true }
            };

        public RedisStorage(IRedisClient redisClient, RedisStorageOptions? options = null)
        {
            RedisClient = redisClient ?? throw new ArgumentNullException(nameof(redisClient));
            _options = options ?? new RedisStorageOptions();

            _subscription = new RedisSubscription(this);
        }

        internal IRedisClient RedisClient { get; }

        internal int SucceededListSize => _options.SucceededListSize;

        internal int DeletedListSize => _options.DeletedListSize;

        internal string SubscriptionChannel => _subscription.Channel;

        internal string[] LifoQueues => _options.LifoQueues;

        public override IMonitoringApi GetMonitoringApi()
        {
            return new RedisMonitoringApi(this);
        }

        public override bool HasFeature([NotNull] string featureId)
        {
            if (featureId == null)
                throw new ArgumentNullException(nameof(featureId));

            return _features.TryGetValue(featureId, out var isSupported) ? isSupported : base.HasFeature(featureId);
        }

        public override IStorageConnection GetConnection()
        {
            return new RedisConnection(this, _subscription, _options.FetchTimeout);
        }

        #pragma warning disable 618
        public override IEnumerable<IServerComponent> GetComponents()
            #pragma warning restore 618
        {
            yield return new FetchedJobsWatcher(this, _options.InvisibilityTimeout);
            yield return new ExpiredJobsWatcher(this, _options.ExpiryCheckInterval);
            yield return _subscription;
        }

        public static DashboardMetric GetDashboardMetricFromRedisInfo(string title, string key)
        {
            return new DashboardMetric("redis:" + key, title, (razorPage) => { return new Metric(""); });
        }

        public override IEnumerable<IStateHandler> GetStateHandlers()
        {
            yield return new FailedStateHandler();
            yield return new ProcessingStateHandler();
            yield return new SucceededStateHandler();
            yield return new DeletedStateHandler();
        }

        public override void WriteOptionsToLog(ILog logger) => base.WriteOptionsToLog(logger);

        internal string GetRedisKey([NotNull] string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return _options.Prefix + key;
        }
    }
}