using Hangfire.States;
using Hangfire.Storage;

namespace Yarkool.Hangfire.Redis
{
    internal class DeletedStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList("deleted", context.BackgroundJob.Id);

            if (context.Storage is RedisStorage { DeletedListSize: > 0 } storage)
            {
                transaction.TrimList("deleted", 0, storage.DeletedListSize);
            }
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromList("deleted", context.BackgroundJob.Id);
        }

        public string StateName => DeletedState.StateName;
    }
}