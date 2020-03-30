namespace Kafka.Streams.Tests.Processor
{
    /*






    *

    *





    */






    public class FailOnInvalidTimestampTest : TimestampExtractorTest
    {

        [Xunit.Fact]
        public void ExtractMetadataTimestamp()
        {
            TestExtractMetadataTimestamp(new FailOnInvalidTimestamp());
        }

        [Xunit.Fact]// (expected = StreamsException)
        public void FailOnInvalidTimestamp()
        {
            TimestampExtractor extractor = new FailOnInvalidTimestamp();
            extractor.extract(new ConsumeResult<>("anyTopic", 0, 0, null, null), 42);
        }

    }
}
/*






*

*





*/






