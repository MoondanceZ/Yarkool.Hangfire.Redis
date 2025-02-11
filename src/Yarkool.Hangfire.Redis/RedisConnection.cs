using System.Collections.Concurrent;
using System.Globalization;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

namespace Yarkool.Hangfire.Redis
{
    internal class RedisConnection : JobStorageConnection
    {
        private static readonly ILog Logger = LogProvider.For<ExpiredJobsWatcher>();
        private readonly RedisStorage _storage;
        private readonly RedisSubscription _subscription;
        private readonly TimeSpan _fetchTimeout;
        private readonly IRedisClient _redisClient;

        public RedisConnection(
            [NotNull] RedisStorage storage,
            [NotNull] RedisSubscription subscription,
            TimeSpan fetchTimeout)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _redisClient = storage.RedisClient;
            _subscription = subscription ?? throw new ArgumentNullException(nameof(subscription));
            _fetchTimeout = fetchTimeout;
        }

        public override IDisposable AcquireDistributedLock([NotNull] string resource, TimeSpan timeout)
        {
            return RedisLock.Acquire(_redisClient, _storage.GetRedisKey(resource), timeout);
        }

        public override void AnnounceServer([NotNull] string serverId, [NotNull] ServerContext context)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            using var pipeline = _redisClient.BeginPipeline();
            pipeline.SAdd(_storage.GetRedisKey("servers"), serverId);
            pipeline.HSet(_storage.GetRedisKey($"server:{serverId}"), new Dictionary<string, string>
            {
                { "WorkerCount", context.WorkerCount.ToString(CultureInfo.InvariantCulture) },
                { "StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow) }
            });
            if (context.Queues.Length > 0)
            {
                pipeline.RPush(_storage.GetRedisKey($"server:{serverId}:queues"), context.Queues);
            }

            pipeline.Execute();
        }

        public override DateTime GetUtcDateTime()
        {
            //the returned time is the time on the first server of the cluster
            return _redisClient.Time();
        }

        public override long GetSetCount([NotNull] IEnumerable<string> keys, int limit)
        {
            var tasks = new Task[keys.Count()];
            var i = 0;
            var results = new ConcurrentDictionary<string, long>();
            foreach (var key in keys)
            {
                tasks[i] = _redisClient.ZCountAsync(_storage.GetRedisKey(key), max: limit).ContinueWith((Task<long> x) => results.TryAdd(_storage.GetRedisKey(key), x.Result));
            }

            Task.WaitAll(tasks);
            return results.Sum(x => x.Value);
        }

        public override bool GetSetContains([NotNull] string key, [NotNull] string value)
        {
            var result = _redisClient.ZScan(_storage.GetRedisKey(key), 0, value);
            return result.Length > 0;
        }

        public override string CreateExpiredJob([NotNull] Job job, [NotNull] IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            var jobId = Guid.NewGuid().ToString();

            var invocationData = InvocationData.SerializeJob(job);

            // Do not modify the original parameters.
            var storedParameters = new Dictionary<string, string>(parameters)
            {
                { "Type", invocationData.Type },
                { "Method", invocationData.Method },
                { "ParameterTypes", invocationData.ParameterTypes },
                { "Arguments", invocationData.Arguments },
                { "CreatedAt", JobHelper.SerializeDateTime(createdAt) }
            };

            if (invocationData.Queue != null)
            {
                storedParameters.Add("Queue", invocationData.Queue);
            }

            using var pipeline = _redisClient.BeginPipeline();
            pipeline.HSet(_storage.GetRedisKey($"job:{jobId}"), storedParameters);
            pipeline.Expire(_storage.GetRedisKey($"job:{jobId}"), expireIn);
            pipeline.Execute();

            return jobId;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new RedisWriteOnlyTransaction(_storage);
        }

        public override void Dispose()
        {
            // nothing to dispose
        }

        public override IFetchedJob FetchNextJob([NotNull] string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null)
                throw new ArgumentNullException(nameof(queues));

            string? jobId = null;
            string? queueName = null;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                for (int i = 0; i < queues.Length; i++)
                {
                    queueName = queues[i];
                    var queueKey = _storage.GetRedisKey($"queue:{queueName}");
                    var fetchedKey = _storage.GetRedisKey($"queue:{queueName}:dequeued");
                    try
                    {
                        jobId = _redisClient.RPopLPush(queueKey, fetchedKey);
                    }
                    catch (Exception ex)
                    {
                        Logger.ErrorException($"RPopLPush fetch {fetchedKey} from {queueKey} error", ex);
                    }

                    if (jobId != null)
                        break;
                }

                if (jobId == null)
                {
                    _subscription.WaitForJob(_fetchTimeout, cancellationToken);
                }
            } while (jobId == null);

            // The job was fetched by the server. To provide reliability,
            // we should ensure, that the job will be performed and acquired
            // resources will be disposed even if the server will crash
            // while executing one of the subsequent lines of code.

            // The job's processing is splitted into a couple of checkpoints.
            // Each checkpoint occurs after successful update of the
            // job information in the storage. And each checkpoint describes
            // the way to perform the job when the server was crashed after
            // reaching it.

            // Checkpoint #1-1. The job was fetched into the fetched list,
            // that is being inspected by the FetchedJobsWatcher instance.
            // Job's has the implicit 'Fetched' state.

            var fetchTime = DateTime.UtcNow;
            _redisClient.HSet(_storage.GetRedisKey($"job:{jobId}"), "Fetched", JobHelper.SerializeDateTime(fetchTime));

            // Checkpoint #2. The job is in the implicit 'Fetched' state now.
            // This state stores information about fetched time. The job will
            // be re-queued when the JobTimeout will be expired.

            return new RedisFetchedJob(_storage, jobId, queueName!, fetchTime);
        }

        public override Dictionary<string, string> GetAllEntriesFromHash([NotNull] string key)
        {
            return _redisClient.HGetAll(_storage.GetRedisKey(key));
        }

        public override List<string> GetAllItemsFromList([NotNull] string key)
        {
            return _redisClient.LRange(_storage.GetRedisKey(key), 0, -1)?.ToList() ?? [];
        }

        public override HashSet<string> GetAllItemsFromSet([NotNull] string key)
        {
            HashSet<string> result = new();
            foreach (var item in _redisClient.ZScan(_storage.GetRedisKey(key), 0).Items)
            {
                _ = result.Add(item.Member);
            }

            return result;
        }

        public override long GetCounter([NotNull] string key)
        {
            return long.Parse(_redisClient.Get(_storage.GetRedisKey(key)) ?? "0");
        }

        public override string GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore)
        {
            return _redisClient.ZRangeByScore(_storage.GetRedisKey(key), (decimal)fromScore, (decimal)toScore, 0, 1)?.FirstOrDefault()!;
        }

        public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
        {
            return _redisClient.ZRangeByScore(_storage.GetRedisKey(key), (decimal)fromScore, (decimal)toScore, 0, count).ToList();
        }

        public override long GetHashCount([NotNull] string key)
        {
            return _redisClient.HLen(_storage.GetRedisKey(key));
        }

        public override TimeSpan GetHashTtl([NotNull] string key)
        {
            var result = _redisClient.Ttl(_storage.GetRedisKey(key));
            return result == null ? TimeSpan.Zero : TimeSpan.FromSeconds((double)result);
        }

        public override JobData GetJobData([NotNull] string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            var storedData = _redisClient.HGetAll(_storage.GetRedisKey($"job:{jobId}"));
            if (storedData?.Any() != true)
                return null!;

            var queue = storedData.FirstOrDefault(x => x.Key == "Queue").Value;
            var type = storedData.FirstOrDefault(x => x.Key == "Type").Value;
            var method = storedData.FirstOrDefault(x => x.Key == "Method").Value;
            var parameterTypes = storedData.FirstOrDefault(x => x.Key == "ParameterTypes").Value;
            var arguments = storedData.FirstOrDefault(x => x.Key == "Arguments").Value;
            var createdAt = storedData.FirstOrDefault(x => x.Key == "CreatedAt").Value;

            Job job = null!;
            JobLoadException loadException = null!;

            var invocationData = new InvocationData(type, method, parameterTypes, arguments, queue);

            try
            {
                job = invocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = storedData.FirstOrDefault(x => x.Key == "State").Value,
                CreatedAt = JobHelper.DeserializeNullableDateTime(createdAt) ?? DateTime.MinValue,
                LoadException = loadException
            };
        }

        public override string GetJobParameter([NotNull] string jobId, [NotNull] string name)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return _redisClient.HGet(_storage.GetRedisKey($"job:{jobId}"), name)!;
        }

        public override long GetListCount([NotNull] string key)
        {
            return _redisClient.LLen(_storage.GetRedisKey(key));
        }

        public override TimeSpan GetListTtl([NotNull] string key)
        {
            var result = _redisClient.Ttl(_storage.GetRedisKey(key));
            return result == null ? TimeSpan.Zero : TimeSpan.FromSeconds((double)result);
        }

        public override List<string> GetRangeFromList([NotNull] string key, int startingFrom, int endingAt)
        {
            return _redisClient.LRange(_storage.GetRedisKey(key), startingFrom, endingAt)?.ToList() ?? [];
        }

        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            return _redisClient.ZRangeByRank(_storage.GetRedisKey(key), startingFrom, endingAt)?.ToList() ?? [];
        }

        public override long GetSetCount([NotNull] string key)
        {
            return _redisClient.ZCard(_storage.GetRedisKey(key));
        }

        public override TimeSpan GetSetTtl([NotNull] string key)
        {
            var result = _redisClient.Ttl(_storage.GetRedisKey(key));
            return result == null ? TimeSpan.Zero : TimeSpan.FromSeconds((double)result);
        }

        public override StateData GetStateData([NotNull] string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            var entries = _redisClient.HGetAll(_storage.GetRedisKey($"job:{jobId}:state"));
            if (!entries.Any())
                return null!;

            var stateData = entries.ToDictionary();

            stateData.Remove("State");
            stateData.Remove("Reason");

            return new StateData
            {
                Name = entries.First(x => x.Key == "State").Value,
                Reason = entries.FirstOrDefault(x => x.Key == "Reason").Value,
                Data = stateData
            };
        }

        public override string GetValueFromHash([NotNull] string key, [NotNull] string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return _redisClient.HGet(_storage.GetRedisKey(key), name)!;
        }

        public override void Heartbeat([NotNull] string serverId)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));

            _redisClient.HSet(_storage.GetRedisKey($"server:{serverId}"), "Heartbeat", JobHelper.SerializeDateTime(DateTime.UtcNow));
        }

        public override void RemoveServer([NotNull] string serverId)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));

            using var pipeline = _redisClient.BeginPipeline();
            pipeline.SRem(_storage.GetRedisKey("servers"), serverId);
            pipeline.Del(_storage.GetRedisKey($"server:{serverId}"), _storage.GetRedisKey($"server:{serverId}:queues"));
            pipeline.Execute();
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            var serverNames = _redisClient.SMembers(_storage.GetRedisKey("servers"));
            var heartbeats = new Dictionary<string, Tuple<DateTime, DateTime?>>();

            var utcNow = DateTime.UtcNow;

            foreach (var serverName in serverNames)
            {
                var srv = _redisClient.HMGet(_storage.GetRedisKey($"server:{serverName}"), "StartedAt", "Heartbeat")!;
                heartbeats.Add(serverName, new Tuple<DateTime, DateTime?>(JobHelper.DeserializeDateTime(srv[0]), JobHelper.DeserializeNullableDateTime(srv[1])));
            }

            var removedServerCount = 0;
            foreach (var heartbeat in heartbeats)
            {
                var maxTime = new DateTime(Math.Max(heartbeat.Value.Item1.Ticks, (heartbeat.Value.Item2 ?? DateTime.MinValue).Ticks));

                if (utcNow > maxTime.Add(timeOut))
                {
                    RemoveServer(heartbeat.Key);
                    removedServerCount++;
                }
            }

            return removedServerCount;
        }

        public override void SetJobParameter([NotNull] string jobId, [NotNull] string name, string value)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            _redisClient.HSet(_storage.GetRedisKey($"job:{jobId}"), name, value);
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (keyValuePairs == null)
                throw new ArgumentNullException(nameof(keyValuePairs));

            _redisClient.HSet(_storage.GetRedisKey(key), keyValuePairs.ToDictionary());
        }
    }
}