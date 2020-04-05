using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class ProcessorRecordContextTest
    {
        // timestamp + offset + partition: 8 + 8 + 4
        private const long MIN_SIZE = 20L;

        [Xunit.Fact]
        public void ShouldEstimateNullTopicAndNullHeadersAsZeroLength()
        {
            Headers headers = new Headers();
            ProcessorRecordContext context = new ProcessorRecordContext(
                42L,
                73L,
                0,
                null,
                null
            );

            Assert.Equal(MIN_SIZE, context.ResidentMemorySizeEstimate());
        }

        [Xunit.Fact]
        public void ShouldEstimateEmptyHeaderAsZeroLength()
        {
            ProcessorRecordContext context = new ProcessorRecordContext(
                42L,
                73L,
                0,
                null,
                new Headers()
            );

            Assert.Equal(MIN_SIZE, context.ResidentMemorySizeEstimate());
        }

        [Xunit.Fact]
        public void ShouldEstimateTopicLength()
        {
            ProcessorRecordContext context = new ProcessorRecordContext(
                42L,
                73L,
                0,
                "topic",
                null
            );

            Assert.Equal(MIN_SIZE + 5L, context.ResidentMemorySizeEstimate());
        }

        [Xunit.Fact]
        public void ShouldEstimateHeadersLength()
        {
            Headers headers = new Headers();
            //headers.Add("header-key", "header-value");
            ProcessorRecordContext context = new ProcessorRecordContext(
                42L,
                73L,
                0,
                null,
                headers
            );

            Assert.Equal(MIN_SIZE + 10L + 12L, context.ResidentMemorySizeEstimate());
        }

        [Xunit.Fact]
        public void ShouldEstimateNullValueInHeaderAsZero()
        {
            Headers headers = new Headers();
            headers.Add("header-key", null);
            ProcessorRecordContext context = new ProcessorRecordContext(
                42L,
                73L,
                0,
                null,
                headers
            );

            Assert.Equal(MIN_SIZE + 10L, context.ResidentMemorySizeEstimate());
        }
    }
}
