//using Confluent.Kafka;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.Threads.GlobalStream;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class StateConsumerTest
//    {

//        private const long FLUSH_INTERVAL = 1000L;
//        private TopicPartition topicOne = new TopicPartition("topic-one", 1);
//        private TopicPartition topicTwo = new TopicPartition("topic-two", 1);
//        private MockTime time = new MockTime();
//        private MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
//        private Dictionary<TopicPartition, long> partitionOffsets = new HashMap<>();
//        private LogContext logContext = new LogContext("test ");
//        private GlobalStreamThread.StateConsumer stateConsumer;
//        private StateMaintainerStub stateMaintainer;


//        public void SetUp()
//        {
//            partitionOffsets.put(topicOne, 20L);
//            partitionOffsets.put(topicTwo, 30L);
//            stateMaintainer = new StateMaintainerStub(partitionOffsets);
//            stateConsumer = new GlobalStreamThread.StateConsumer(logContext, consumer, stateMaintainer, time, Duration.FromMilliseconds(10L), FLUSH_INTERVAL);
//        }

//        [Xunit.Fact]
//        public void ShouldAssignPartitionsToConsumer()
//        {
//            stateConsumer.initialize();
//            Assert.Equal(Utils.mkSet(topicOne, topicTwo), consumer.assignment());
//        }

//        [Xunit.Fact]
//        public void ShouldSeekToInitialOffsets()
//        {
//            stateConsumer.initialize();
//            Assert.Equal(20L, consumer.position(topicOne));
//            Assert.Equal(30L, consumer.position(topicTwo));
//        }

//        [Xunit.Fact]
//        public void ShouldUpdateStateWithReceivedRecordsForPartition()
//        {
//            stateConsumer.initialize();
//            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 20L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 21L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            stateConsumer.pollAndUpdate();
//            Assert.Equal(2, stateMaintainer.updatedPartitions.Get(topicOne).intValue());
//        }

//        [Xunit.Fact]
//        public void ShouldUpdateStateWithReceivedRecordsForAllTopicPartition()
//        {
//            stateConsumer.initialize();
//            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 20L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.addRecord(new ConsumeResult<>("topic-two", 1, 31L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.addRecord(new ConsumeResult<>("topic-two", 1, 32L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            stateConsumer.pollAndUpdate();
//            Assert.Equal(1, stateMaintainer.updatedPartitions.Get(topicOne).intValue());
//            Assert.Equal(2, stateMaintainer.updatedPartitions.Get(topicTwo).intValue());
//        }

//        [Xunit.Fact]
//        public void ShouldFlushStoreWhenFlushIntervalHasLapsed()
//        {
//            stateConsumer.initialize();
//            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 20L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            time.sleep(FLUSH_INTERVAL);

//            stateConsumer.pollAndUpdate();
//            Assert.True(stateMaintainer.flushed);
//        }

//        [Xunit.Fact]
//        public void ShouldNotFlushOffsetsWhenFlushIntervalHasNotLapsed()
//        {
//            stateConsumer.initialize();
//            consumer.addRecord(new ConsumeResult<>("topic-one", 1, 20L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            time.sleep(FLUSH_INTERVAL / 2);
//            stateConsumer.pollAndUpdate();
//            Assert.False(stateMaintainer.flushed);
//        }

//        [Xunit.Fact]
//        public void ShouldCloseConsumer()
//        { //throws IOException
//            stateConsumer.close();
//            Assert.True(consumer.closed());
//        }

//        [Xunit.Fact]
//        public void ShouldCloseStateMaintainer()
//        { //throws IOException
//            stateConsumer.close();
//            Assert.True(stateMaintainer.closed);
//        }


//        private class StateMaintainerStub : IGlobalStateMaintainer
//        {
//            private Dictionary<TopicPartition, long> partitionOffsets;
//            private Dictionary<TopicPartition, int> updatedPartitions = new HashMap<>();
//            private bool flushed;
//            private bool closed;

//            StateMaintainerStub(Dictionary<TopicPartition, long> partitionOffsets)
//            {
//                this.partitionOffsets = partitionOffsets;
//            }


//            public Dictionary<TopicPartition, long> Initialize()
//            {
//                return partitionOffsets;
//            }

//            public void FlushState()
//            {
//                flushed = true;
//            }


//            public void Close()
//            {
//                closed = true;
//            }


//            public void Update(ConsumeResult<byte[], byte[]> record)
//            {
//                TopicPartition tp = new TopicPartition(record.Topic, record.Partition);
//                if (!updatedPartitions.containsKey(tp))
//                {
//                    updatedPartitions.put(tp, 0);
//                }
//                updatedPartitions.put(tp, updatedPartitions.Get(tp) + 1);
//            }

//        }

//    }
//}
