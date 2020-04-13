using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class StateRestorerTest
    {
        private const long OFFSET_LIMIT = 50;
        private MockRestoreCallback callback = new MockRestoreCallback();
        private MockStateRestoreListener reportingListener = new MockStateRestoreListener();
        private CompositeRestoreListener compositeRestoreListener = new CompositeRestoreListener(callback);
        private StateRestorer restorer = new StateRestorer(
            new TopicPartition("topic", 1),
            compositeRestoreListener,
            null,
            OFFSET_LIMIT,
            true,
            "storeName",
            identity());


        public void SetUp()
        {
            compositeRestoreListener.setUserRestoreListener(reportingListener);
        }

        [Fact]
        public void ShouldCallRestoreOnRestoreCallback()
        {
            restorer.restore(Collections.singletonList(new ConsumeResult<>("", 0, 0L, System.Array.Empty<byte>(), System.Array.Empty<byte>())));
            Assert.Equal(callback.restored.Count, 1);
        }

        [Fact]
        public void ShouldBeCompletedIfRecordOffsetGreaterThanEndOffset()
        {
            Assert.True(restorer.hasCompleted(11, 10));
        }

        [Fact]
        public void ShouldBeCompletedIfRecordOffsetGreaterThanOffsetLimit()
        {
            Assert.True(restorer.hasCompleted(51, 100));
        }

        [Fact]
        public void ShouldBeCompletedIfEndOffsetAndRecordOffsetAreZero()
        {
            Assert.True(restorer.hasCompleted(0, 0));
        }

        [Fact]
        public void ShouldBeCompletedIfOffsetAndOffsetLimitAreZero()
        {
            StateRestorer restorer = new StateRestorer(
                new TopicPartition("topic", 1),
                compositeRestoreListener,
                null,
                0,
                true,
                "storeName",
                identity());
            Assert.True(restorer.hasCompleted(0, 10));
        }

        [Fact]
        public void ShouldSetRestoredOffsetToMinOfLimitAndOffset()
        {
            restorer.setRestoredOffset(20);
            Assert.Equal(restorer.restoredOffset(), 20L);
            restorer.setRestoredOffset(100);
            Assert.Equal(restorer.restoredOffset(), OFFSET_LIMIT);
        }

        [Fact]
        public void ShouldSetStartingOffsetToMinOfLimitAndOffset()
        {
            restorer.setStartingOffset(20);
            Assert.Equal(restorer.startingOffset(), 20L);
            restorer.setRestoredOffset(100);
            Assert.Equal(restorer.restoredOffset(), OFFSET_LIMIT);
        }

        [Fact]
        public void ShouldReturnCorrectNumRestoredRecords()
        {
            restorer.setStartingOffset(20);
            restorer.setRestoredOffset(40);
            Assert.Equal(restorer.restoredNumRecords(), 20L);
            restorer.setRestoredOffset(100);
            Assert.Equal(restorer.restoredNumRecords(), OFFSET_LIMIT - 20);
        }
    }
}
