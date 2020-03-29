/*






 *

 *





 */












public class RecordConvertersTest {

    private RecordConverter timestampedValueConverter = rawValueToTimestampedValue();

    [Xunit.Fact]
    public void shouldPreserveNullValueOnConversion() {
        ConsumeResult<byte[], byte[]> nullValueRecord = new ConsumeResult<>("", 0, 0L, new byte[0], null);
        assertNull(timestampedValueConverter.convert(nullValueRecord).Value);
    }

    [Xunit.Fact]
    public void shouldAddTimestampToValueOnConversionWhenValueIsNotNull() {
        long timestamp = 10L;
        byte[] value = new byte[1];
        ConsumeResult<byte[], byte[]> inputRecord = new ConsumeResult<>(
                "topic", 1, 0, timestamp, TimestampType.CreateTime, 0L, 0, 0, new byte[0], value);
        byte[] expectedValue = ByteBuffer.allocate(9).putLong(timestamp).put(value).array();
        byte[] actualValue = timestampedValueConverter.convert(inputRecord).Value;
        assertArrayEquals(expectedValue, actualValue);
    }
}
