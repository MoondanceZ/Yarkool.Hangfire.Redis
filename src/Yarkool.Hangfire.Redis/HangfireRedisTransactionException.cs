namespace Yarkool.Hangfire.Redis
{
    public class HangfireRedisTransactionException : Exception
    {
        public HangfireRedisTransactionException(string message) : base(message)
        {
        }
    }
}