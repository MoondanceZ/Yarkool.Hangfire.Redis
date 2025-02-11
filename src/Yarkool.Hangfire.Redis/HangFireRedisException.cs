namespace Yarkool.Hangfire.Redis
{
    public class HangFireRedisException : Exception
    {
        public HangFireRedisException(string message) : base(message)
        {
        }
    }
}