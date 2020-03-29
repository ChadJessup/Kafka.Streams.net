/*






 *

 *





 */










public class UsePreviousTimeOnInvalidTimestampTest : TimestampExtractorTest {

    [Xunit.Fact]
    public void extractMetadataTimestamp() {
        testExtractMetadataTimestamp(new UsePreviousTimeOnInvalidTimestamp());
    }

    [Xunit.Fact]
    public void usePreviousTimeOnInvalidTimestamp() {
        long previousTime = 42;

        TimestampExtractor extractor = new UsePreviousTimeOnInvalidTimestamp();
        long timestamp = extractor.extract(
            new ConsumeResult<>("anyTopic", 0, 0, null, null),
            previousTime
        );

        Assert.Equal(timestamp, is(previousTime));
    }

    [Xunit.Fact]
    public void shouldThrowStreamsException() {
        TimestampExtractor extractor = new UsePreviousTimeOnInvalidTimestamp();
        ConsumeResult<object, object> record = new ConsumeResult<>("anyTopic", 0, 0, null, null);
        try {
            extractor.extract(record, -1);
            Assert.True(false, "should have thrown StreamsException");
        } catch (StreamsException expected) { }
    }
}
