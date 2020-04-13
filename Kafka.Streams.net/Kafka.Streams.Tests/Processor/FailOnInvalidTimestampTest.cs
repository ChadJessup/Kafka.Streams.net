using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Xunit;

namespace Kafka.Streams.Tests.Processor
{
    public class FailOnInvalidTimestampTest : TimestampExtractorTest
    {
        [Fact]
        public void ExtractMetadataTimestamp()
        {
            TestExtractMetadataTimestamp(new FailOnInvalidTimestamp());
        }

        [Fact]
        public void FailOnInvalidTimestamp()
        {
            ITimestampExtractor extractor = new FailOnInvalidTimestamp();

            Assert.Throws<StreamsException>(() => extractor.Extract(
               new ConsumeResult<long, long>
               {
                   TopicPartitionOffset = new TopicPartitionOffset("anyTopic", 0, 0),
                   Message = new Message<long, long>
                   {
                       Timestamp = new Timestamp(-1, TimestampType.NotAvailable),
                   },
               },
           42));
        }
    }
}
