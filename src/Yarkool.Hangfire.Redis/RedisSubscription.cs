using Hangfire.Annotations;
using Hangfire.Server;

namespace Yarkool.Hangfire.Redis
{
    #pragma warning disable 618
    internal class RedisSubscription : IServerComponent
    #pragma warning restore 618
    {
        private readonly ManualResetEvent _mre = new(false);
        private readonly IRedisClient _redisClient;
        private IDisposable? _subscribeObject;

        public RedisSubscription([NotNull] RedisStorage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));
            _redisClient = storage.RedisClient;
            Channel = storage.GetRedisKey("JobFetchChannel");
        }

        public string Channel { get; }

        public void WaitForJob(TimeSpan timeout, CancellationToken cancellationToken)
        {
            _mre.Reset();
            WaitHandle.WaitAny(new[]
            {
                _mre,
                cancellationToken.WaitHandle
            }, timeout);
        }

        void IServerComponent.Execute(CancellationToken cancellationToken)
        {
            _subscribeObject = _redisClient.Subscribe(Channel, (channel, value) => _mre.Set());
            cancellationToken.WaitHandle.WaitOne();

            if (cancellationToken.IsCancellationRequested)
            {
                _redisClient.UnSubscribe(Channel);
                _mre.Reset();
                _subscribeObject?.Dispose();
            }
        }

        ~RedisSubscription()
        {
            _mre.Dispose();
        }
    }
}