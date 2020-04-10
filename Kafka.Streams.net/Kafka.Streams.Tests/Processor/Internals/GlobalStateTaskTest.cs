//using Confluent.Kafka;
//using Kafka.Streams.Nodes;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class GlobalStateTaskTest
//    {

//        private LogContext logContext = new LogContext();

//        private readonly string topic1 = "t1";
//        private readonly string topic2 = "t2";
//        private TopicPartition t1 = new TopicPartition(topic1, 1);
//        private TopicPartition t2 = new TopicPartition(topic2, 1);
//        private MockSourceNode sourceOne = new MockSourceNode<>(
//            new string[] { topic1 },
//            new Serdes.String().Deserializer(),
//            new Serdes.String().Deserializer());
//        private MockSourceNode sourceTwo = new MockSourceNode<>(
//            new string[] { topic2 },
//            Serializers.Int32,
//            Serializers.Int32);
//        private MockProcessorNode processorOne = new MockProcessorNode<>();
//        private MockProcessorNode processorTwo = new MockProcessorNode<>();

//        private Dictionary<TopicPartition, long> offsets = new HashMap<>();
//        private NoOpProcessorContext context = new NoOpProcessorContext();

//        private ProcessorTopology topology;
//        private GlobalStateManagerStub stateMgr;
//        private GlobalStateUpdateTask globalStateTask;


//        public void Before()
//        {
//            HashSet<string> storeNames = Utils.mkSet("t1-store", "t2-store");
//            Dictionary<string, SourceNode> sourceByTopics = new HashMap<>();
//            sourceByTopics.Add(topic1, sourceOne);
//            sourceByTopics.Add(topic2, sourceTwo);
//            Dictionary<string, string> storeToTopic = new HashMap<>();
//            storeToTopic.Add("t1-store", topic1);
//            storeToTopic.Add("t2-store", topic2);
//            topology = ProcessorTopologyFactories.with(
//                asList(sourceOne, sourceTwo, processorOne, processorTwo),
//                sourceByTopics,
//                Collections.< IStateStore > emptyList(),
//                storeToTopic);

//            offsets.Add(t1, 50L);
//            offsets.Add(t2, 100L);
//            stateMgr = new GlobalStateManagerStub(storeNames, offsets);
//            globalStateTask = new GlobalStateUpdateTask(topology, context, stateMgr, new LogAndFailExceptionHandler(), logContext);
//        }

//        [Fact]
//        public void ShouldInitializeStateManager()
//        {
//            Dictionary<TopicPartition, long> startingOffsets = globalStateTask.initialize();
//            Assert.True(stateMgr.initialized);
//            Assert.Equal(offsets, startingOffsets);
//        }

//        [Fact]
//        public void ShouldInitializeContext()
//        {
//            globalStateTask.initialize();
//            Assert.True(context.initialized);
//        }

//        [Fact]
//        public void ShouldInitializeProcessorTopology()
//        {
//            globalStateTask.initialize();
//            Assert.True(sourceOne.initialized);
//            Assert.True(sourceTwo.initialized);
//            Assert.True(processorOne.initialized);
//            Assert.True(processorTwo.initialized);
//        }

//        [Fact]
//        public void ShouldProcessRecordsForTopic()
//        {
//            globalStateTask.initialize();
//            globalStateTask.update(new ConsumeResult<>(topic1, 1, 1, "foo".getBytes(), "bar".getBytes()));
//            Assert.Equal(1, sourceOne.numReceived);
//            Assert.Equal(0, sourceTwo.numReceived);
//        }

//        [Fact]
//        public void ShouldProcessRecordsForOtherTopic()
//        {
//            byte[] integerBytes = new IntegerSerializer().Serialize("foo", 1);
//            globalStateTask.initialize();
//            globalStateTask.update(new ConsumeResult<>(topic2, 1, 1, integerBytes, integerBytes));
//            Assert.Equal(1, sourceTwo.numReceived);
//            Assert.Equal(0, sourceOne.numReceived);
//        }

//        private void MaybeDeserialize(GlobalStateUpdateTask globalStateTask,
//                                      byte[] key,
//                                      byte[] recordValue,
//                                      bool failExpected)
//        {
//            ConsumeResult<byte[], byte[]> record = new ConsumeResult<>(
//                topic2, 1, 1, 0L, TimestampType.CreateTime,
//                0L, 0, 0, key, recordValue
//            );
//            globalStateTask.initialize();
//            try
//            {
//                globalStateTask.update(record);
//                if (failExpected)
//                {
//                    Assert.True(false, "Should have failed to deserialize.");
//                }
//            }
//            catch (StreamsException e)
//            {
//                if (!failExpected)
//                {
//                    Assert.True(false, "Shouldn't have failed to deserialize.");
//                }
//            }
//        }


//        [Fact]
//        public void ShouldThrowStreamsExceptionWhenKeyDeserializationFails()
//        {
//            byte[] key = new Serdes.Long().Serializer().Serialize(topic2, 1L);
//            byte[] recordValue = new IntegerSerializer().Serialize(topic2, 10);
//            MaybeDeserialize(globalStateTask, key, recordValue, true);
//        }


//        [Fact]
//        public void ShouldThrowStreamsExceptionWhenValueDeserializationFails()
//        {
//            byte[] key = new IntegerSerializer().Serialize(topic2, 1);
//            byte[] recordValue = new Serdes.Long().Serializer().Serialize(topic2, 10L);
//            MaybeDeserialize(globalStateTask, key, recordValue, true);
//        }

//        [Fact]
//        public void ShouldNotThrowStreamsExceptionWhenKeyDeserializationFailsWithSkipHandler()
//        {
//            GlobalStateUpdateTask globalStateTask2 = new GlobalStateUpdateTask(
//                topology,
//                context,
//                stateMgr,
//                new LogAndContinueExceptionHandler(),
//                logContext
//            );
//            byte[] key = new Serdes.Long().Serializer().Serialize(topic2, 1L);
//            byte[] recordValue = new IntegerSerializer().Serialize(topic2, 10);

//            MaybeDeserialize(globalStateTask2, key, recordValue, false);
//        }

//        [Fact]
//        public void ShouldNotThrowStreamsExceptionWhenValueDeserializationFails()
//        {
//            GlobalStateUpdateTask globalStateTask2 = new GlobalStateUpdateTask(
//                topology,
//                context,
//                stateMgr,
//                new LogAndContinueExceptionHandler(),
//                logContext
//            );
//            byte[] key = new IntegerSerializer().Serialize(topic2, 1);
//            byte[] recordValue = new Serdes.Long().Serializer().Serialize(topic2, 10L);

//            MaybeDeserialize(globalStateTask2, key, recordValue, false);
//        }


//        [Fact]
//        public void ShouldFlushStateManagerWithOffsets()
//        { //throws IOException
//            Dictionary<TopicPartition, long> expectedOffsets = new HashMap<>();
//            expectedOffsets.Put(t1, 52L);
//            expectedOffsets.Put(t2, 100L);
//            globalStateTask.initialize();
//            globalStateTask.update(new ConsumeResult<>(topic1, 1, 51, "foo".getBytes(), "foo".getBytes()));
//            globalStateTask.flushState();
//            Assert.Equal(expectedOffsets, stateMgr.checkpointed());
//        }

//        [Fact]
//        public void ShouldCheckpointOffsetsWhenStateIsFlushed()
//        {
//            Dictionary<TopicPartition, long> expectedOffsets = new HashMap<>();
//            expectedOffsets.Add(t1, 102L);
//            expectedOffsets.Add(t2, 100L);
//            globalStateTask.initialize();
//            globalStateTask.Update(new ConsumeResult<>(topic1, 1, 101, "foo".getBytes(), "foo".getBytes()));
//            globalStateTask.flushState();
//            Assert.Equal(stateMgr.checkpointed(), (expectedOffsets));
//        }

//    }
//}
