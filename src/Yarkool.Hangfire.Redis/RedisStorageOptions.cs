namespace Yarkool.Hangfire.Redis
{
    public class RedisStorageOptions
    {
        public const string DefaultPrefix = "{hangfire}:";

        public RedisStorageOptions()
        {
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            FetchTimeout = TimeSpan.FromMinutes(3);
            ExpiryCheckInterval = TimeSpan.FromHours(1);
            Prefix = DefaultPrefix;
            SucceededListSize = 499;
            DeletedListSize = 499;
            LifoQueues = [];
        }

        /// <summary>
        /// It's a part of mechanism to requeue the job if the server processing it died for some reason
        /// </summary>
        /// <see cref="FetchedJobsWatcher"/>
        public TimeSpan InvisibilityTimeout { get; set; }

        /// <summary>
        /// It's a fallback for fetching jobs if pub/sub mechanism fails. This software use redis pub/sub to be notified
        /// when a new job has been enqueued. Redis pub/sub however doesn't guarantee delivery so, should the
        /// RedisConnection lose a message, within the FetchTimeout the queue will be scanned from scratch
        /// and any waiting jobs will be processed.
        /// </summary>
        public TimeSpan FetchTimeout { get; set; }

        /// <summary>
        /// Time that should pass between expired jobs cleanup.
        /// RedisStorage uses non-expiring keys, so to clean up the store there is a thread running a IServerComponent
        /// that take care of deleting expired jobs from redis.
        /// </summary>
        public TimeSpan ExpiryCheckInterval { get; set; }

        public string Prefix { get; set; }
        public int SucceededListSize { get; set; }
        public int DeletedListSize { get; set; }
        public string[] LifoQueues { get; set; }
    }
}