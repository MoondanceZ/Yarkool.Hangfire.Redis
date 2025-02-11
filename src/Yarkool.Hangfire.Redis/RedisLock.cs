using System.Collections.Concurrent;
using System.Diagnostics;
using Hangfire;
using Hangfire.Annotations;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;

namespace Yarkool.Hangfire.Redis
{
    internal class RedisLock : IDisposable
    {
        private static readonly TimeSpan DefaultHoldDuration = TimeSpan.FromSeconds(30);
        private static readonly string OwnerId = Guid.NewGuid().ToString();

        private static readonly ThreadLocal<ConcurrentDictionary<string, byte>> _heldLocks = new();

        private class StateBag
        {
            public PerformingContext? PerformingContext { get; set; }
            public TimeSpan TimeSpan { get; set; }
        }

        private static ConcurrentDictionary<string, byte> HeldLocks
        {
            get
            {
                var value = _heldLocks.Value;
                if (value == null)
                    _heldLocks.Value = value = new ConcurrentDictionary<string, byte>();
                return value;
            }
        }

        private readonly IRedisClient _redisClient;
        private readonly string _key;
        private readonly bool _holdsLock;
        private volatile bool _isDisposed = false;
        private readonly Timer? _slidingExpirationTimer;

        private RedisLock([NotNull] IRedisClient redisClient, string key, bool holdsLock, TimeSpan holdDuration)
        {
            _redisClient = redisClient;
            _key = key;
            _holdsLock = holdsLock;

            if (holdsLock)
            {
                HeldLocks.TryAdd(_key, 1);

                // start sliding expiration timer at half timeout intervals
                var halfLockHoldDuration = TimeSpan.FromTicks(holdDuration.Ticks / 2);
                var state = new StateBag()
                {
                    PerformingContext = HangfireSubscriber.Value,
                    TimeSpan = holdDuration
                };
                _slidingExpirationTimer = new Timer(ExpirationTimerTick!, state, halfLockHoldDuration, halfLockHoldDuration);
            }
        }

        private void ExpirationTimerTick(object state)
        {
            if (!_isDisposed)
            {
                var stateBag = state as StateBag;
                Exception redisEx = default!;
                var lockSuccessfullyExtended = false;
                var retryCount = 10;
                while (!lockSuccessfullyExtended && retryCount >= 0)
                {
                    try
                    {
                        lockSuccessfullyExtended = _redisClient.LockExtend(_key, OwnerId, stateBag!.TimeSpan);
                    }
                    catch (Exception ex)
                    {
                        redisEx = ex;
                        Thread.Sleep(3000);
                        retryCount--;
                    }
                }

                if (!lockSuccessfullyExtended)
                {
                    new BackgroundJobClient(stateBag!.PerformingContext!.Storage).ChangeState(
                        stateBag.PerformingContext.BackgroundJob.Id,
                        new FailedState(new Exception($"Unable to extend a distributed lock with Key {_key} and OwnerId {OwnerId}", redisEx))
                    );
                }
            }
        }

        public void Dispose()
        {
            if (_holdsLock)
            {
                _isDisposed = true;
                _slidingExpirationTimer?.Dispose();

                if (!_redisClient.LockRelease(_key, OwnerId))
                {
                    Debug.WriteLine("Lock {0} already timed out", _key);
                }

                HeldLocks.TryRemove(_key, out _);
            }
        }

        public static IDisposable Acquire([NotNull] IRedisClient redisClient, string key, TimeSpan timeOut)
        {
            return Acquire(redisClient, key, timeOut, DefaultHoldDuration);
        }

        internal static IDisposable Acquire([NotNull] IRedisClient redisClient, string key, TimeSpan timeOut, TimeSpan holdDuration)
        {
            if (redisClient == null)
                throw new ArgumentNullException(nameof(redisClient));

            if (HeldLocks.ContainsKey(key))
            {
                // lock is already held
                return new RedisLock(redisClient, key, false, holdDuration);
            }

            // The comparison below uses timeOut as a max timeSpan in waiting Lock
            var i = 0;
            var lockExpirationTime = DateTime.UtcNow + timeOut;
            do
            {
                if (redisClient.LockTake(key, OwnerId, holdDuration))
                {
                    // we have successfully acquired the lock
                    return new RedisLock(redisClient, key, true, holdDuration);
                }

                SleepBackOffMultiplier(i++, (int)(lockExpirationTime - DateTime.UtcNow).TotalMilliseconds);
            } while (DateTime.UtcNow < lockExpirationTime);

            throw new DistributedLockTimeoutException($"Failed to acquire lock on {key} within given timeout ({timeOut})");
        }

        private static void SleepBackOffMultiplier(int i, int maxWait)
        {
            if (maxWait <= 0)
                return;

            // exponential/random retry back-off.
            var rand = new Random(Guid.NewGuid().GetHashCode());
            var nextTry = rand.Next((int)Math.Pow(i, 2), (int)Math.Pow(i + 1, 2) + 1);

            nextTry = Math.Min(nextTry, maxWait);

            Thread.Sleep(nextTry);
        }
    }
}