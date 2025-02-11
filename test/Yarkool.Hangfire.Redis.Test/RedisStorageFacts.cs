using Hangfire.Logging;
using Moq;
using Yarkool.Hangfire.Redis.Test.Utils;

namespace Yarkool.Hangfire.Redis.Test
{
    [Collection("Sequential")]
    public class RedisStorageFacts
    {
        [Fact]
        [CleanRedis]
        public void PasswordFromWriteOptionsToLogIsNotShown()
        {
            string password = Guid.NewGuid().ToString("N");
            var storage = new RedisStorage(RedisUtils.CreateClient());

            string loggedMessage = null!;

            var logMock = new Mock<ILog>();

            logMock.Setup(p => p.Log(LogLevel.Debug, null, null)).Returns(true); // logger.IsDebugEnabled()
            logMock.Setup(p => p.Log(
                    LogLevel.Debug,
                    It.Is<Func<string>>(f => f != null! && f.Invoke().StartsWith("ConnectionString: ")),
                    null))
                .Callback((LogLevel lvl, Func<string> msg, Exception ex) => { loggedMessage = msg.Invoke(); })
                .Returns(true)
                .Verifiable();

            storage.WriteOptionsToLog(logMock.Object);

            logMock.Verify();
            Assert.DoesNotContain(password, loggedMessage);
        }
    }
}