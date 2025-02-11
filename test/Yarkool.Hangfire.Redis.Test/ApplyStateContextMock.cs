using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Moq;
using Yarkool.Hangfire.Redis.Test.Utils;

namespace Yarkool.Hangfire.Redis.Test
{
    public class ApplyStateContextMock
    {
        private readonly Lazy<ApplyStateContext> _context;

        public ApplyStateContextMock(string jobId)
        {
            NewStateValue = new Mock<IState>().Object;
            OldStateValue = null!;
            var storage = CreateStorage();
            var connection = storage.GetConnection();
            var writeOnlyTransaction = connection.CreateWriteTransaction();

            var job = new Job(GetType().GetMethod("GetType"));
            var backgroundJob = new BackgroundJob(jobId, job, DateTime.MinValue);

            _context = new Lazy<ApplyStateContext>(
                () => new ApplyStateContext(storage, connection, writeOnlyTransaction, backgroundJob, NewStateValue, OldStateValue));
        }

        public IState NewStateValue { get; set; }
        public string OldStateValue { get; set; }

        public ApplyStateContext Object => _context.Value;

        private RedisStorage CreateStorage()
        {
            var options = new RedisStorageOptions() { };
            return new RedisStorage(RedisUtils.CreateClient(), options);
        }
    }
}