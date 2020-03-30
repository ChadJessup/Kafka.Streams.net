namespace Kafka.Streams.Tests.Processor.Internals
{
    /*






    *

    *





    */






















    public class StateConsumerTest
    {

        private static readonly long FLUSH_INTERVAL = 1000L;
        private TopicPartition topicOne = new TopicPartition("topic-one", 1);
        private TopicPartition topicTwo = new TopicPartition("topic-two", 1);
        private MockTime time = new MockTime();
        private MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        private Dictionary<TopicPartition, long> partitionOffsets = new HashMap<>();
        private LogContext logContext = new LogContext("test ");
        private GlobalStreamThread.StateConsumer stateConsumer;
        private StateMaintainerStub stateMaintainer;


        public void SetUp()
        {
            partitionOffsets.put(topicOne, 20L);
            partitionOffsets.put(topicTwo, 30L);
            stateMaintainer = new StateMaintainerStub(partitionOffsets);
            stateConsumer = new GlobalStreamThread.StateConsumer(logContext, consumer, stateMaintainer, time, Duration.ofMillis(10L), FLUSH_INTERVAL);
        }

        [Xunit.Fact]
        public void ShouldAssignPartitionsToConsumer()
        {
            stateConsumer.initialize();
            Assert.Equal(Utils.mkSet(topicOne, topicTwo), consumer.assignment());
        }

        [Xunit.Fact]
        public void ShouldSeekToInitialOffsets()
        {
            stateConsumer.initialize();
            Assert.Equal(20L, consumer.position(topicOne));
            Assert.Equal(30L, consumer.position(topicTwo));
        }

        [Xunit.Fact]
        public void ShouldUpdateStateWithReceivedRecordsForPartition()
        {
            stateConsumer.initialize();
            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 20L, new byte[0], new byte[0]));
            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 21L, new byte[0], new byte[0]));
            stateConsumer.pollAndUpdate();
            Assert.Equal(2, stateMaintainer.updatedPartitions.get(topicOne).intValue());
        }

        [Xunit.Fact]
        public void ShouldUpdateStateWithReceivedRecordsForAllTopicPartition()
        {
            stateConsumer.initialize();
            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 20L, new byte[0], new byte[0]));
            consumer.addRecord(new ConsumeResult<>("topic-two", 1, 31L, new byte[0], new byte[0]));
            consumer.addRecord(new ConsumeResult<>("topic-two", 1, 32L, new byte[0], new byte[0]));
            stateConsumer.pollAndUpdate();
            Assert.Equal(1, stateMaintainer.updatedPartitions.get(topicOne).intValue());
            Assert.Equal(2, stateMaintainer.updatedPartitions.get(topicTwo).intValue());
        }

        [Xunit.Fact]
        public void ShouldFlushStoreWhenFlushIntervalHasLapsed()
        {
            stateConsumer.initialize();
            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 20L, new byte[0], new byte[0]));
            time.sleep(FLUSH_INTERVAL);

            stateConsumer.pollAndUpdate();
            Assert.True(stateMaintainer.flushed);
        }

        [Xunit.Fact]
        public void ShouldNotFlushOffsetsWhenFlushIntervalHasNotLapsed()
        {
            stateConsumer.initialize();
            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 20L, new byte[0], new byte[0]));
            time.sleep(FLUSH_INTERVAL / 2);
            stateConsumer.pollAndUpdate();
            Assert.False(stateMaintainer.flushed);
        }

        [Xunit.Fact]
        public void ShouldCloseConsumer()
        { //throws IOException
            stateConsumer.close();
            Assert.True(consumer.closed());
        }

        [Xunit.Fact]
        public void ShouldCloseStateMaintainer()
        { //throws IOException
            stateConsumer.close();
            Assert.True(stateMaintainer.closed);
        }


        private static class StateMaintainerStub : GlobalStateMaintainer
        {
            private Dictionary<TopicPartition, long> partitionOffsets;
            private Dictionary<TopicPartition, int> updatedPartitions = new HashMap<>();
            private bool flushed;
            private bool closed;

            StateMaintainerStub(Dictionary<TopicPartition, long> partitionOffsets)
            {
                this.partitionOffsets = partitionOffsets;
            }


            public Dictionary<TopicPartition, long> Initialize()
            {
                return partitionOffsets;
            }

            public void FlushState()
            {
                flushed = true;
            }


            public void Close()
            {
                closed = true;
            }


            public void Update(ConsumeResult<byte[], byte[]> record)
            {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                if (!updatedPartitions.containsKey(tp))
                {
                    updatedPartitions.put(tp, 0);
                }
                updatedPartitions.put(tp, updatedPartitions.get(tp) + 1);
            }

        }

    }
}
/*






*

*





*/






















