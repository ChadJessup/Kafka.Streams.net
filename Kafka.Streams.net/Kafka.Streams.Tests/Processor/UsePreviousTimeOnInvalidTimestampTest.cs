/*






 *

 *





 */










public class UsePreviousTimeOnInvalidTimestampTest : TimestampExtractorTest {

    [Xunit.Fact]
    public void ExtractMetadataTimestamp() {
        TestExtractMetadataTimestamp(new UsePreviousTimeOnInvalidTimestamp());
    }

    [Xunit.Fact]
    public void UsePreviousTimeOnInvalidTimestamp() {
        long previousTime = 42;

        TimestampExtractor extractor = new UsePreviousTimeOnInvalidTimestamp();
        long timestamp = extractor.extract(
            new ConsumeResult<>("anyTopic", 0, 0, null, null),
            previousTime
        );

        Assert.Equal(timestamp, (previousTime));
    }

    [Xunit.Fact]
    public void ShouldThrowStreamsException() {
        TimestampExtractor extractor = new UsePreviousTimeOnInvalidTimestamp();
        ConsumeResult<object, object> record = new ConsumeResult<>("anyTopic", 0, 0, null, null);
        try {
            extractor.extract(record, -1);
            Assert.True(false, "should have thrown StreamsException");
        } catch (StreamsException expected) { }
    }
}
