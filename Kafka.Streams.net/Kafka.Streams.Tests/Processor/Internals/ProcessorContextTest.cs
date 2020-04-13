using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class ProcessorContextTest
    {
        private IProcessorContext context;


        public void Prepare()
        {
            StreamsConfig streamsConfig = Mock.Of<StreamsConfig);
            expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("add-id");
            expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
            expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
            replay(streamsConfig);

            context = new ProcessorContextImpl(
                Mock.Of<TaskId),
                Mock.Of<StreamTask),
                streamsConfig,
                Mock.Of<RecordCollector),
                Mock.Of<ProcessorStateManager),
                Mock.Of<StreamsMetricsImpl),
                Mock.Of<ThreadCache)
            );
        }

        [Fact]
        public void ShouldNotAllowToScheduleZeroMillisecondPunctuation()
        {
            try
            {
                context.schedule(TimeSpan.FromMilliseconds(0L), null, null);
                Assert.True(false, "Should have thrown ArgumentException");
            }
            catch (ArgumentException expected)
            {
                Assert.Equal(expected.getMessage(), "The minimum supported scheduling interval is 1 millisecond.");
            }
        }

        [Fact]
        public void ShouldNotAllowToScheduleSubMillisecondPunctuation()
        {
            try
            {
                context.schedule(TimeSpan.ofNanos(999_999L), null, null);
                Assert.True(false, "Should have thrown ArgumentException");
            }
            catch (ArgumentException expected)
            {
                Assert.Equal(expected.getMessage(), "The minimum supported scheduling interval is 1 millisecond.");
            }
        }
    }
}
