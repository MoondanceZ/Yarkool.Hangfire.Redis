using Hangfire.States;
using Hangfire.Storage;

namespace Yarkool.Hangfire.Redis
{
    internal class SucceededStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList("succeeded", context.BackgroundJob.Id);

            if (context.Storage is RedisStorage { SucceededListSize: > 0 } storage)
            {
                transaction.TrimList("succeeded", 0, storage.SucceededListSize);
            }
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromList("succeeded", context.BackgroundJob.Id);
        }

        public string StateName => SucceededState.StateName;
    }
}