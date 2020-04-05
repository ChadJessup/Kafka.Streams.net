//using Confluent.Kafka;
//using Kafka.Streams.Interfaces;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor
//{
//    public class TimestampExtractorTest
//    {

//        void TestExtractMetadataTimestamp(ITimestampExtractor extractor)
//        {
//            long metadataTimestamp = 42;

//            long timestamp = extractor.Extract(
//                new ConsumeResult<long, long>(
//                    "anyTopic",
//                    0,
//                    0,
//                    metadataTimestamp,
//                    TimestampType.NotAvailable,
//                    0,
//                    0,
//                    0,
//                    null,
//                    null),
//                0
//            );

//            Assert.Equal(timestamp, metadataTimestamp);
//        }
//    }
//}
