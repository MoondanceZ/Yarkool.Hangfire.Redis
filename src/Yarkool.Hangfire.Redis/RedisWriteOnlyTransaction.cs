using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Yarkool.Hangfire.Redis
{
    internal class RedisWriteOnlyTransaction([NotNull] RedisStorage storage) : JobStorageTransaction
    {
        private readonly RedisStorage _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        private readonly IRedisTransaction _transaction = storage.RedisClient.BeginTransaction();
        private readonly List<IDisposable> _lockToDispose = [];

        public override void AddRangeToSet([NotNull] string key, [NotNull] IList<string> items)
        {
            _transaction.ZAdd(_storage.GetRedisKey(key), items.ToDictionary(x => x, x => 0m));
        }

        public override void AcquireDistributedLock([NotNull] string resource, TimeSpan timeout)
        {
            var distributedLock = _storage.GetConnection().AcquireDistributedLock(resource, timeout);
            _lockToDispose.Add(distributedLock);
        }

        public override void ExpireHash([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.Expire(_storage.GetRedisKey(key), expireIn);
        }

        public override void ExpireList([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.Expire(_storage.GetRedisKey(key), expireIn);
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.Expire(_storage.GetRedisKey(key), expireIn);
        }

        public override void PersistHash([NotNull] string key)
        {
            _transaction.Persist(_storage.GetRedisKey(key));
        }

        public override void PersistList([NotNull] string key)
        {
            _transaction.Persist(_storage.GetRedisKey(key));
        }

        public override void PersistSet([NotNull] string key)
        {
            _transaction.Persist(_storage.GetRedisKey(key));
        }

        public override void RemoveSet([NotNull] string key)
        {
            _transaction.Del(_storage.GetRedisKey(key));
        }

        public override void Commit()
        {
            _transaction.Execute();
        }

        public override void ExpireJob([NotNull] string jobId, TimeSpan expireIn)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            _transaction.Expire(_storage.GetRedisKey($"job:{jobId}"), expireIn);
            _transaction.Expire(_storage.GetRedisKey($"job:{jobId}:history"), expireIn);
            _transaction.Expire(_storage.GetRedisKey($"job:{jobId}:state"), expireIn);
        }

        public override void PersistJob([NotNull] string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            _transaction.Persist(_storage.GetRedisKey($"job:{jobId}"));
            _transaction.Persist(_storage.GetRedisKey($"job:{jobId}:history"));
            _transaction.Persist(_storage.GetRedisKey($"job:{jobId}:state"));
        }

        public override void SetJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));
            if (state == null)
                throw new ArgumentNullException(nameof(state));

            _transaction.HSet(_storage.GetRedisKey($"job:{jobId}"), "State", state.Name);
            _transaction.Del(_storage.GetRedisKey($"job:{jobId}:state"));

            var storedData = new Dictionary<string, string>(state.SerializeData()) { { "State", state.Name } };

            if (state.Reason != null)
                storedData.Add("Reason", state.Reason);

            _transaction.HSet(_storage.GetRedisKey($"job:{jobId}:state"), storedData);

            AddJobState(jobId, state);
        }

        public override void AddJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));
            if (state == null)
                throw new ArgumentNullException(nameof(state));

            var storedData = new Dictionary<string, string>(state.SerializeData())
            {
                { "State", state.Name },
                { "Reason", state.Reason },
                { "CreatedAt", JobHelper.SerializeDateTime(DateTime.UtcNow) }
            };

            _transaction.RPush(_storage.GetRedisKey($"job:{jobId}:history"), SerializationHelper.Serialize(storedData));
        }

        public override void AddToQueue([NotNull] string queue, [NotNull] string jobId)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            _transaction.SAdd(_storage.GetRedisKey("queues"), queue);
            if (_storage.LifoQueues.Contains(queue, StringComparer.OrdinalIgnoreCase))
            {
                _transaction.RPush(_storage.GetRedisKey($"queue:{queue}"), jobId);
            }
            else
            {
                _transaction.LPush(_storage.GetRedisKey($"queue:{queue}"), jobId);
            }

            _transaction.Publish(_storage.SubscriptionChannel, jobId);
        }

        public override void IncrementCounter([NotNull] string key)
        {
            _transaction.IncrBy(_storage.GetRedisKey(key), 1);
        }

        public override void IncrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.IncrBy(_storage.GetRedisKey(key), 1);
            _transaction.Expire(_storage.GetRedisKey(key), expireIn);
        }

        public override void DecrementCounter([NotNull] string key)
        {
            _transaction.IncrBy(_storage.GetRedisKey(key), -1);
        }

        public override void DecrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.IncrBy(_storage.GetRedisKey(key), -1);
            _transaction.Expire(_storage.GetRedisKey(key), expireIn);
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value)
        {
            AddToSet(key, value, 0);
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value, double score)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            _transaction.ZAdd(_storage.GetRedisKey(key), value, (decimal)score);
        }

        public override void RemoveFromSet([NotNull] string key, [NotNull] string value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            _transaction.ZRem(_storage.GetRedisKey(key), value);
        }

        public override void InsertToList([NotNull] string key, string value)
        {
            _transaction.LPush(_storage.GetRedisKey(key), value);
        }

        public override void RemoveFromList([NotNull] string key, string value)
        {
            _transaction.LRem(_storage.GetRedisKey(key), 0, value);
        }

        public override void TrimList([NotNull] string key, int keepStartingFrom, int keepEndingAt)
        {
            _transaction.LTrim(_storage.GetRedisKey(key), keepStartingFrom, keepEndingAt);
        }

        public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (keyValuePairs == null)
                throw new ArgumentNullException(nameof(keyValuePairs));

            _transaction.HSet(_storage.GetRedisKey(key), keyValuePairs.ToDictionary());
        }

        public override void SetJobParameter([NotNull] string jobId, [NotNull] string name, [CanBeNull] string value)
        {
            _storage.GetConnection().SetJobParameter(jobId, name, value);
        }

        public override void RemoveHash([NotNull] string key)
        {
            _transaction.Del(_storage.GetRedisKey(key));
        }

        public override void RemoveFromQueue([NotNull] IFetchedJob fetchedJob)
        {
            fetchedJob.RemoveFromQueue();
        }

        public override void Dispose()
        {
            _transaction.Dispose();
            foreach (var lockObj in _lockToDispose)
            {
                lockObj.Dispose();
            }

            base.Dispose();
        }
    }
}