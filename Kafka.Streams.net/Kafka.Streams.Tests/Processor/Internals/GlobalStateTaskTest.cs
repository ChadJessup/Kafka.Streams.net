namespace Kafka.Streams.Tests.Processor.Internals
{
    /*






    *

    *





    */



































    public class GlobalStateTaskTest
    {

        private LogContext logContext = new LogContext();

        private readonly string topic1 = "t1";
        private readonly string topic2 = "t2";
        private TopicPartition t1 = new TopicPartition(topic1, 1);
        private TopicPartition t2 = new TopicPartition(topic2, 1);
        private MockSourceNode sourceOne = new MockSourceNode<>(
            new string[] { topic1 },
            new StringDeserializer(),
            new StringDeserializer());
        private MockSourceNode sourceTwo = new MockSourceNode<>(
            new string[] { topic2 },
            Serializers.Int32,
            Serializers.Int32);
        private MockProcessorNode processorOne = new MockProcessorNode<>();
        private MockProcessorNode processorTwo = new MockProcessorNode<>();

        private Dictionary<TopicPartition, long> offsets = new HashMap<>();
        private NoOpProcessorContext context = new NoOpProcessorContext();

        private ProcessorTopology topology;
        private GlobalStateManagerStub stateMgr;
        private GlobalStateUpdateTask globalStateTask;


        public void Before()
        {
            HashSet<string> storeNames = Utils.mkSet("t1-store", "t2-store");
            Dictionary<string, SourceNode> sourceByTopics = new HashMap<>();
            sourceByTopics.put(topic1, sourceOne);
            sourceByTopics.put(topic2, sourceTwo);
            Dictionary<string, string> storeToTopic = new HashMap<>();
            storeToTopic.put("t1-store", topic1);
            storeToTopic.put("t2-store", topic2);
            topology = ProcessorTopologyFactories.with(
                asList(sourceOne, sourceTwo, processorOne, processorTwo),
                sourceByTopics,
                Collections.< StateStore > emptyList(),
                storeToTopic);

            offsets.put(t1, 50L);
            offsets.put(t2, 100L);
            stateMgr = new GlobalStateManagerStub(storeNames, offsets);
            globalStateTask = new GlobalStateUpdateTask(topology, context, stateMgr, new LogAndFailExceptionHandler(), logContext);
        }

        [Xunit.Fact]
        public void ShouldInitializeStateManager()
        {
            Dictionary<TopicPartition, long> startingOffsets = globalStateTask.initialize();
            Assert.True(stateMgr.initialized);
            Assert.Equal(offsets, startingOffsets);
        }

        [Xunit.Fact]
        public void ShouldInitializeContext()
        {
            globalStateTask.initialize();
            Assert.True(context.initialized);
        }

        [Xunit.Fact]
        public void ShouldInitializeProcessorTopology()
        {
            globalStateTask.initialize();
            Assert.True(sourceOne.initialized);
            Assert.True(sourceTwo.initialized);
            Assert.True(processorOne.initialized);
            Assert.True(processorTwo.initialized);
        }

        [Xunit.Fact]
        public void ShouldProcessRecordsForTopic()
        {
            globalStateTask.initialize();
            globalStateTask.update(new ConsumeResult<>(topic1, 1, 1, "foo".getBytes(), "bar".getBytes()));
            Assert.Equal(1, sourceOne.numReceived);
            Assert.Equal(0, sourceTwo.numReceived);
        }

        [Xunit.Fact]
        public void ShouldProcessRecordsForOtherTopic()
        {
            byte[] integerBytes = new IntegerSerializer().serialize("foo", 1);
            globalStateTask.initialize();
            globalStateTask.update(new ConsumeResult<>(topic2, 1, 1, integerBytes, integerBytes));
            Assert.Equal(1, sourceTwo.numReceived);
            Assert.Equal(0, sourceOne.numReceived);
        }

        private void MaybeDeserialize(GlobalStateUpdateTask globalStateTask,
                                      byte[] key,
                                      byte[] recordValue,
                                      bool failExpected)
        {
            ConsumeResult<byte[], byte[]> record = new ConsumeResult<>(
                topic2, 1, 1, 0L, TimestampType.CreateTime,
                0L, 0, 0, key, recordValue
            );
            globalStateTask.initialize();
            try
            {
                globalStateTask.update(record);
                if (failExpected)
                {
                    Assert.True(false, "Should have failed to deserialize.");
                }
            }
            catch (StreamsException e)
            {
                if (!failExpected)
                {
                    Assert.True(false, "Shouldn't have failed to deserialize.");
                }
            }
        }


        [Xunit.Fact]
        public void ShouldThrowStreamsExceptionWhenKeyDeserializationFails()
        {
            byte[] key = new LongSerializer().serialize(topic2, 1L);
            byte[] recordValue = new IntegerSerializer().serialize(topic2, 10);
            MaybeDeserialize(globalStateTask, key, recordValue, true);
        }


        [Xunit.Fact]
        public void ShouldThrowStreamsExceptionWhenValueDeserializationFails()
        {
            byte[] key = new IntegerSerializer().serialize(topic2, 1);
            byte[] recordValue = new LongSerializer().serialize(topic2, 10L);
            MaybeDeserialize(globalStateTask, key, recordValue, true);
        }

        [Xunit.Fact]
        public void ShouldNotThrowStreamsExceptionWhenKeyDeserializationFailsWithSkipHandler()
        {
            GlobalStateUpdateTask globalStateTask2 = new GlobalStateUpdateTask(
                topology,
                context,
                stateMgr,
                new LogAndContinueExceptionHandler(),
                logContext
            );
            byte[] key = new LongSerializer().serialize(topic2, 1L);
            byte[] recordValue = new IntegerSerializer().serialize(topic2, 10);

            MaybeDeserialize(globalStateTask2, key, recordValue, false);
        }

        [Xunit.Fact]
        public void ShouldNotThrowStreamsExceptionWhenValueDeserializationFails()
        {
            GlobalStateUpdateTask globalStateTask2 = new GlobalStateUpdateTask(
                topology,
                context,
                stateMgr,
                new LogAndContinueExceptionHandler(),
                logContext
            );
            byte[] key = new IntegerSerializer().serialize(topic2, 1);
            byte[] recordValue = new LongSerializer().serialize(topic2, 10L);

            MaybeDeserialize(globalStateTask2, key, recordValue, false);
        }


        [Xunit.Fact]
        public void ShouldFlushStateManagerWithOffsets()
        { //throws IOException
            Dictionary<TopicPartition, long> expectedOffsets = new HashMap<>();
            expectedOffsets.put(t1, 52L);
            expectedOffsets.put(t2, 100L);
            globalStateTask.initialize();
            globalStateTask.update(new ConsumeResult<>(topic1, 1, 51, "foo".getBytes(), "foo".getBytes()));
            globalStateTask.flushState();
            Assert.Equal(expectedOffsets, stateMgr.checkpointed());
        }

        [Xunit.Fact]
        public void ShouldCheckpointOffsetsWhenStateIsFlushed()
        {
            Dictionary<TopicPartition, long> expectedOffsets = new HashMap<>();
            expectedOffsets.put(t1, 102L);
            expectedOffsets.put(t2, 100L);
            globalStateTask.initialize();
            globalStateTask.update(new ConsumeResult<>(topic1, 1, 101, "foo".getBytes(), "foo".getBytes()));
            globalStateTask.flushState();
            Assert.Equal(stateMgr.checkpointed(), (expectedOffsets));
        }

    }
}
/*






*

*





*/



































