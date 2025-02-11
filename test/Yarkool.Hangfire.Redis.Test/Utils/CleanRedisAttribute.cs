using System.Reflection;
using Xunit.Sdk;

namespace Yarkool.Hangfire.Redis.Test.Utils
{
    public class CleanRedisAttribute : BeforeAfterTestAttribute
    {
        public override void Before(MethodInfo methodUnderTest)
        {
        }

        public override void After(MethodInfo methodUnderTest)
        {
        }
    }
}