//using Confluent.Kafka;
//using Kafka.Streams.Interfaces;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor
//{
//    public class LogAndSkipOnInvalidTimestampTest : TimestampExtractorTest
//    {
//        [Xunit.Fact]
//        public void ExtractMetadataTimestamp()
//        {
//            TestExtractMetadataTimestamp(new LogAndSkipOnInvalidTimestamp());
//        }

//        [Xunit.Fact]
//        public void LogAndSkipOnInvalidTimestamp()
//        {
//            long invalidMetadataTimestamp = -42;

//            ITimestampExtractor extractor = new LogAndSkipOnInvalidTimestamp();
//            long timestamp = extractor.Extract(
//                new ConsumeResult<>(
//                    "anyTopic",
//                    0,
//                    0,
//                    invalidMetadataTimestamp,
//                    TimestampType.NotAvailable,
//                    0,
//                    0,
//                    0,
//                    null,
//                    null),
//                0
//            );

//            Assert.Equal(timestamp, invalidMetadataTimestamp);
//        }

//    }
//}
