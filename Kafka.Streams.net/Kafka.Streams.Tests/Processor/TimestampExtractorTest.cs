using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Xunit;

namespace Kafka.Streams.Tests.Processor
{
    public class TimestampExtractorTest
    {
        protected void TestExtractMetadataTimestamp(ITimestampExtractor extractor)
        {
            long metadataTimestamp = 42;

            long timestamp = extractor.Extract(
                new ConsumeResult<long, long>
                {
                    Topic = "anyTopic",
                    Offset = 0,
                    Partition = 0,
                    Message = new Message<long, long>
                    {
                        Timestamp = new Timestamp(metadataTimestamp, TimestampType.NotAvailable),
                        Key = 0,
                        Value = 0,
                    }
                },
                0);

            Assert.Equal(timestamp, metadataTimestamp);
        }
    }
}
