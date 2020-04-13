using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Xunit;

namespace Kafka.Streams.Tests.Processor
{
    public class UsePreviousTimeOnInvalidTimestampTest : TimestampExtractorTest
    {
        [Fact]
        public void ExtractMetadataTimestamp()
        {
            TestExtractMetadataTimestamp(new UsePreviousTimeOnInvalidTimestamp());
        }

        [Fact]
        public void UsePreviousTimeOnInvalidTimestamp()
        {
            long previousTime = 42;

            ITimestampExtractor extractor = new UsePreviousTimeOnInvalidTimestamp();
            long timestamp = extractor.Extract(
                new ConsumeResult<>("anyTopic", 0, 0, null, null),
                previousTime
            );

            Assert.Equal(timestamp, previousTime);
        }

        [Fact]
        public void ShouldThrowStreamsException()
        {
            ITimestampExtractor extractor = new UsePreviousTimeOnInvalidTimestamp();
            ConsumeResult<object, object> record = new ConsumeResult<object, object>("anyTopic", 0, 0, null, null);
            try
            {
                extractor.Extract(record, -1);
                Assert.True(false, "should have thrown StreamsException");
            }
            catch (StreamsException expected) { }
        }
    }
}
