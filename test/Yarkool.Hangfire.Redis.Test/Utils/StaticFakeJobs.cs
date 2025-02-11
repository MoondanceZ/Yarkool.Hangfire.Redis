namespace Yarkool.Hangfire.Redis.Test.Utils
{
    public static class StaticFakeJobs
    {
        public static string Work(int identifier, int waitTime)
        {
            Thread.Sleep(waitTime);
            var jobResult = string.Format("{0} - {1} Job done after waiting {2} ms", DateTime.Now.ToString("hh:mm:ss fff"), identifier, waitTime);
            Console.WriteLine(jobResult);
            return jobResult;
        }
    }
}