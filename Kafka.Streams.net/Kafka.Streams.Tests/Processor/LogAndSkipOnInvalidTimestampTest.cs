/*






 *

 *





 */









public class LogAndSkipOnInvalidTimestampTest : TimestampExtractorTest {

    [Xunit.Fact]
    public void ExtractMetadataTimestamp() {
        TestExtractMetadataTimestamp(new LogAndSkipOnInvalidTimestamp());
    }

    [Xunit.Fact]
    public void LogAndSkipOnInvalidTimestamp() {
        long invalidMetadataTimestamp = -42;

        TimestampExtractor extractor = new LogAndSkipOnInvalidTimestamp();
        long timestamp = extractor.extract(
            new ConsumeResult<>(
                "anyTopic",
                0,
                0,
                invalidMetadataTimestamp,
                TimestampType.NO_TIMESTAMP_TYPE,
                0,
                0,
                0,
                null,
                null),
            0
        );

        Assert.Equal(timestamp, (invalidMetadataTimestamp));
    }

}
