using Hangfire.Server;

namespace Yarkool.Hangfire.Redis
{
    /// <summary>
    /// Singelton used to keep track of hangfire jobs
    /// </summary>
    /// <remarks>Came from https://github.com/AnderssonPeter/Hangfire.Console.Extensions/blob/master/Hangfire.Console.Extensions/HangfireSubscriber.cs </remarks>
    internal class HangfireSubscriber : IServerFilter
    {
        private static readonly AsyncLocal<PerformingContext?> localStorage = new();

        public static PerformingContext? Value => localStorage.Value;

        public void OnPerforming(PerformingContext filterContext)
        {
            localStorage.Value = filterContext;
        }

        public void OnPerformed(PerformedContext filterContext)
        {
            localStorage.Value = null;
        }
    }
}