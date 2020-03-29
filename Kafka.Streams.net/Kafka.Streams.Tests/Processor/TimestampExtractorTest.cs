/*






 *

 *





 */








class TimestampExtractorTest {

    void testExtractMetadataTimestamp(TimestampExtractor extractor) {
        long metadataTimestamp = 42;

        long timestamp = extractor.extract(
            new ConsumeResult<>(
                "anyTopic",
                0,
                0,
                metadataTimestamp,
                TimestampType.NO_TIMESTAMP_TYPE,
                0,
                0,
                0,
                null,
                null),
            0
        );

        Assert.Equal(timestamp, is(metadataTimestamp));
    }

}
