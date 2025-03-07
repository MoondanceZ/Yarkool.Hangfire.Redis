using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Server;

namespace Yarkool.Hangfire.Redis
{
    #pragma warning disable 618
    internal class FetchedJobsWatcher : IServerComponent
    #pragma warning restore 618
    {
        private readonly IRedisClient _redisClient;
        private readonly TimeSpan _invisibilityTimeout;
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(FetchedJobsWatcher));

        private readonly RedisStorage _storage;
        private readonly FetchedJobsWatcherOptions _options;

        public FetchedJobsWatcher(RedisStorage storage, TimeSpan invisibilityTimeout)
            : this(storage, invisibilityTimeout, new FetchedJobsWatcherOptions())
        {
        }

        public FetchedJobsWatcher(RedisStorage storage, TimeSpan invisibilityTimeout, FetchedJobsWatcherOptions options)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            if (invisibilityTimeout.Ticks <= 0)
                throw new ArgumentOutOfRangeException(nameof(invisibilityTimeout), "Invisibility timeout duration should be positive.");

            _storage = storage;
            _redisClient = storage.RedisClient;
            _invisibilityTimeout = invisibilityTimeout;
            _options = options;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var queues = _redisClient.SMembers(_storage.GetRedisKey("queues"));

            foreach (var queue in queues)
            {
                ProcessQueue(queue);
            }

            cancellationToken.WaitHandle.WaitOne(_options.SleepTimeout);
        }

        private void ProcessQueue(string queue)
        {
            // Allowing only one server at a time to process the timed out
            // jobs from the specified queue.
            Logger.DebugFormat("Acquiring the lock for the fetched list of the '{0}' queue...", queue);

            using (RedisLock.Acquire(_redisClient, _storage.GetRedisKey($"queue:{queue}:dequeued:lock"), _options.FetchedLockTimeout))
            {
                Logger.DebugFormat("Looking for timed out and aborted jobs in the '{0}' queue...", queue);

                var jobIds = _redisClient.LRange(_storage.GetRedisKey($"queue:{queue}:dequeued"), 0, -1);

                var requeued = 0;

                foreach (var jobId in jobIds)
                {
                    if (RequeueJobIfTimedOutOrAborted(jobId, queue))
                    {
                        requeued++;
                    }
                }

                if (requeued == 0)
                {
                    Logger.DebugFormat("No timed out or aborted jobs were found in the '{0}' queue", queue);
                }
                else
                {
                    Logger.InfoFormat("{0} timed out jobs were found in the '{1}' queue and re-queued.", requeued, queue);
                }
            }
        }

        private bool RequeueJobIfTimedOutOrAborted(string jobId, string queue)
        {
            var flags = _redisClient.HMGet($"job:{jobId}", "Fetched", "Checked")!;

            var fetched = flags[0];
            var @checked = flags[1];

            if (string.IsNullOrEmpty(fetched) && string.IsNullOrEmpty(@checked))
            {
                // If the job does not have these flags set, then it is
                // in the implicit 'Fetched' state. This state has no
                // information about the time it was fetched. So we
                // can not do anything with the job in this state, because
                // there are two options:

                // 1. It is going to move to the implicit 'Fetched' state
                //    in a short time.
                // 2. It will stay in the 'Fetched' state forever due to
                //    its processing server is dead.

                // To ensure its server is dead, we'll move the job to
                // the implicit 'Checked' state with the current timestamp
                // and will not do anything else at this pass of the watcher.
                // If job's state will still be 'Checked' on the later passes
                // and after the CheckedTimeout expired, then the server
                // is dead, and we'll re-queue the job.

                _redisClient.HSet(_storage.GetRedisKey($"job:{jobId}"), "Checked", JobHelper.SerializeDateTime(DateTime.UtcNow));

                // Checkpoint #1-2. The job is in the implicit 'Checked' state.
                // It will be re-queued after the CheckedTimeout will be expired.
            }
            else
            {
                if (TimedOutByFetchedTime(fetched!) || TimedOutByCheckedTime(fetched!, @checked!) || BeingProcessedByADeadServer(jobId))
                {
                    var fetchedJob = new RedisFetchedJob(_storage, jobId, queue, JobHelper.DeserializeNullableDateTime(fetched));
                    fetchedJob.Dispose();

                    return true;
                }
            }

            return false;
        }

        private bool TimedOutByFetchedTime(string fetchedTimestamp)
        {
            return !string.IsNullOrEmpty(fetchedTimestamp) && (DateTime.UtcNow - JobHelper.DeserializeDateTime(fetchedTimestamp) > _invisibilityTimeout);
        }

        private bool TimedOutByCheckedTime(string fetchedTimestamp, string checkedTimestamp)
        {
            // If the job has the 'fetched' flag set, then it is
            // in the implicit 'Fetched' state, and it can not be timed
            // out by the 'checked' flag.
            if (!string.IsNullOrEmpty(fetchedTimestamp))
            {
                return false;
            }

            return !string.IsNullOrEmpty(checkedTimestamp) && (DateTime.UtcNow - JobHelper.DeserializeDateTime(checkedTimestamp) > _options.CheckedTimeout);
        }

        private bool BeingProcessedByADeadServer(string jobId)
        {
            var serverId = _redisClient.HGet(_storage.GetRedisKey($"job:{jobId}:state"), "ServerId");
            if (string.IsNullOrEmpty(serverId))
            {
                // job is not fetched by a server
                return false;
            }

            var serverCheckedIn = _redisClient.HGet(_storage.GetRedisKey($"server:{serverId}"), "StartedAt");

            // if the server has not been removed by ServerWatchdog due to heartbeat timeout,
            // there will be a value in this field, so the server is considered alive,
            // and thus the job shouldn't be requeued just yet.
            return string.IsNullOrEmpty(serverCheckedIn);
        }
    }
}