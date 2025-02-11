﻿using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Yarkool.Hangfire.Redis.Test.Utils;

namespace Yarkool.Hangfire.Redis.Test
{
    [CleanRedis]
    [Collection("Sequential")]
    public class FailedStateHandlerFacts
    {
        private const string JobId = "1";

        private readonly ApplyStateContextMock _context;
        private readonly Mock<IWriteOnlyTransaction> _transaction;

        public FailedStateHandlerFacts()
        {
            _context = new ApplyStateContextMock(JobId) { NewStateValue = new FailedState(new InvalidOperationException()) };

            _transaction = new Mock<IWriteOnlyTransaction>();
        }

        [Fact]
        public void StateName_ShouldBeEqualToFailedState()
        {
            var handler = new FailedStateHandler();
            Assert.Equal(FailedState.StateName, handler.StateName);
        }

        [Fact]
        public void Apply_ShouldAddTheJob_ToTheFailedSet()
        {
            var handler = new FailedStateHandler();
            handler.Apply(_context.Object, _transaction.Object);

            _transaction.Verify(x => x.AddToSet("failed", JobId, It.IsAny<double>()));
        }

        [Fact]
        public void Unapply_ShouldRemoveTheJob_FromTheFailedSet()
        {
            var handler = new FailedStateHandler();
            handler.Unapply(_context.Object, _transaction.Object);

            _transaction.Verify(x => x.RemoveFromSet("failed", JobId));
        }
    }
}