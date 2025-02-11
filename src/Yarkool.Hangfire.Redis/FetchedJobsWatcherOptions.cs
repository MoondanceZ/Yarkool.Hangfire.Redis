namespace Yarkool.Hangfire.Redis
{
    internal class FetchedJobsWatcherOptions
    {
        public TimeSpan FetchedLockTimeout { get; set; } = TimeSpan.FromMinutes(1);
        public TimeSpan CheckedTimeout { get; set; } = TimeSpan.FromMinutes(1);
        public TimeSpan SleepTimeout { get; set; } = TimeSpan.FromMinutes(1);
    }
}