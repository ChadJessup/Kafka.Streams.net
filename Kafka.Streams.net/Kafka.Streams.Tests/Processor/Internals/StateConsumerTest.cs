//using Confluent.Kafka;
//using Kafka.Streams.Clients.Consumers;
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
//        private MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
//        private Dictionary<TopicPartition, long> partitionOffsets = new HashMap<>();
//        //private LogContext logContext = new LogContext("test ");
//        private StateConsumer stateConsumer;
//        private StateMaintainerStub stateMaintainer;

//        public StateConsumerTest()
//        {
//            partitionOffsets.Put(topicOne, 20L);
//            partitionOffsets.Put(topicTwo, 30L);
//            stateMaintainer = new StateMaintainerStub(partitionOffsets);
//            //stateConsumer = new StateConsumer(logContext, consumer, stateMaintainer, time, TimeSpan.FromMilliseconds(10L), FLUSH_INTERVAL);
//        }

//        [Fact]
//        public void ShouldAssignPartitionsToConsumer()
//        {
//            stateConsumer.Initialize();
//            // Assert.Equal(Utils.mkSet(topicOne, topicTwo), consumer.Assignment());
//        }

//        [Fact]
//        public void ShouldSeekToInitialOffsets()
//        {
//            stateConsumer.Initialize();
//            Assert.Equal(20L, consumer.Position(topicOne).Value);
//            Assert.Equal(30L, consumer.Position(topicTwo).Value);
//        }

//        [Fact]
//        public void ShouldUpdateStateWithReceivedRecordsForPartition()
//        {
//            stateConsumer.initialize();
//            consumer.AddRecord(new ConsumeResult<>("topic-one", 1, 20L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.AddRecord(new ConsumeResult<>("topic-one", 1, 21L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            stateConsumer.pollAndUpdate();
//            Assert.Equal(2, stateMaintainer.updatedPartitions.Get(topicOne).intValue());
//        }

//        [Fact]
//        public void ShouldUpdateStateWithReceivedRecordsForAllTopicPartition()
//        {
//            stateConsumer.initialize();
//            consumer.AddRecord(new ConsumeResult<>("topic-one", 1, 20L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.AddRecord(new ConsumeResult<>("topic-two", 1, 31L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.AddRecord(new ConsumeResult<>("topic-two", 1, 32L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            stateConsumer.pollAndUpdate();
//            Assert.Equal(1, stateMaintainer.updatedPartitions.Get(topicOne).intValue());
//            Assert.Equal(2, stateMaintainer.updatedPartitions.Get(topicTwo).intValue());
//        }

//        [Fact]
//        public void ShouldFlushStoreWhenFlushIntervalHasLapsed()
//        {
//            stateConsumer.initialize();
//            consumer.AddRecord(new ConsumeResult<>("topic-one", 1, 20L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            time.Sleep(FLUSH_INTERVAL);

//            stateConsumer.pollAndUpdate();
//            Assert.True(stateMaintainer.flushed);
//        }

//        [Fact]
//        public void ShouldNotFlushOffsetsWhenFlushIntervalHasNotLapsed()
//        {
//            stateConsumer.initialize();
//            consumer.AddRecord(new ConsumeResult<>("topic-one", 1, 20L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            time.Sleep(FLUSH_INTERVAL / 2);
//            stateConsumer.pollAndUpdate();
//            Assert.False(stateMaintainer.flushed);
//        }

//        [Fact]
//        public void ShouldCloseConsumer()
//        { //throws IOException
//            stateConsumer.Close();
//            Assert.True(consumer.closed());
//        }

//        [Fact]
//        public void ShouldCloseStateMaintainer()
//        { //throws IOException
//            stateConsumer.Close();
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
//                if (!updatedPartitions.ContainsKey(tp))
//                {
//                    updatedPartitions.Put(tp, 0);
//                }
//                updatedPartitions.Put(tp, updatedPartitions.Get(tp) + 1);
//            }

//        }

//    }
//}
