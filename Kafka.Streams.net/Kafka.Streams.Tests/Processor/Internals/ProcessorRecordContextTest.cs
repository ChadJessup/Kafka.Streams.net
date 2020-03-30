/*






 *

 *





 */








public class ProcessorRecordContextTest {
    // timestamp + offset + partition: 8 + 8 + 4
    private static readonly long MIN_SIZE = 20L;

    [Xunit.Fact]
    public void ShouldEstimateNullTopicAndNullHeadersAsZeroLength() {
        Headers headers = new Headers();
        ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            null
        );

        Assert.Equal(MIN_SIZE, context.residentMemorySizeEstimate());
    }

    [Xunit.Fact]
    public void ShouldEstimateEmptyHeaderAsZeroLength() {
        ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            new Headers()
        );

        Assert.Equal(MIN_SIZE, context.residentMemorySizeEstimate());
    }

    [Xunit.Fact]
    public void ShouldEstimateTopicLength() {
        ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            null
        );

        Assert.Equal(MIN_SIZE + 5L, context.residentMemorySizeEstimate());
    }

    [Xunit.Fact]
    public void ShouldEstimateHeadersLength() {
        Headers headers = new Headers();
        headers.add("header-key", "header-value".getBytes());
        ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        Assert.Equal(MIN_SIZE + 10L + 12L, context.residentMemorySizeEstimate());
    }

    [Xunit.Fact]
    public void ShouldEstimateNullValueInHeaderAsZero() {
        Headers headers = new Headers();
        headers.add("header-key", null);
        ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        Assert.Equal(MIN_SIZE + 10L, context.residentMemorySizeEstimate());
    }
}
