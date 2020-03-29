/*






 *

 *





 */






public class FailOnInvalidTimestampTest : TimestampExtractorTest {

    [Xunit.Fact]
    public void extractMetadataTimestamp() {
        testExtractMetadataTimestamp(new FailOnInvalidTimestamp());
    }

    [Xunit.Fact]// (expected = StreamsException)
    public void failOnInvalidTimestamp() {
        TimestampExtractor extractor = new FailOnInvalidTimestamp();
        extractor.extract(new ConsumeResult<>("anyTopic", 0, 0, null, null), 42);
    }

}
