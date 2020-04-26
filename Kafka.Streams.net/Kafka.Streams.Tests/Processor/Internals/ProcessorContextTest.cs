using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using Moq;
using System;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class ProcessorContextTest
    {
        private IProcessorContext context;
        private readonly StreamsConfig streamsConfig;

        public ProcessorContextTest()
        {
            this.streamsConfig = Mock.Of<StreamsConfig>(sc =>
                sc.ApplicationId == "add-id"
                && sc.DefaultKeySerdeType == Serdes.ByteArray().GetType()
                && sc.DefaultValueSerdeType == Serdes.ByteArray().GetType()); ;

            context = new ProcessorContext<byte[], byte[]>(
                Mock.Of<KafkaStreamsContext>(),
                Mock.Of<TaskId>(),
                Mock.Of<StreamTask>(),
                streamsConfig,
                Mock.Of<RecordCollector>(),
                Mock.Of<ProcessorStateManager>(),
                Mock.Of<ThreadCache>());
        }

        [Fact]
        public void ShouldNotAllowToScheduleZeroMillisecondPunctuation()
        {
            try
            {
                context.Schedule(TimeSpan.FromMilliseconds(0L), null, null);
                Assert.True(false, "Should have thrown ArgumentException");
            }
            catch (ArgumentException expected)
            {
                Assert.Equal("The minimum supported scheduling interval is 1 millisecond.", expected.Message);
            }
        }

        [Fact]
        public void ShouldNotAllowToScheduleSubMillisecondPunctuation()
        {
            try
            {
                context.Schedule(TimeSpan.FromTicks(1000), null, null);
                Assert.True(false, "Should have thrown ArgumentException");
            }
            catch (ArgumentException expected)
            {
                Assert.Equal(expected.Message, "The minimum supported scheduling interval is 1 millisecond.");
            }
        }
    }
}
