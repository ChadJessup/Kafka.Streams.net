/*






 *

 *





 */
















public class RecordDeserializerTest {

    private Headers headers = new Headers(new Header[] {new RecordHeader("key", "value".getBytes())});
    private ConsumeResult<byte[], byte[]> rawRecord = new ConsumeResult<>("topic",
        1,
        1,
        10,
        TimestampType.LOG_APPEND_TIME,
        5L,
        3,
        5,
        new byte[0],
        new byte[0],
        headers);

    
    [Xunit.Fact]
    public void ShouldReturnConsumerRecordWithDeserializedValueWhenNoExceptions() {
        RecordDeserializer recordDeserializer = new RecordDeserializer(
            new TheSourceNode(
                false,
                false,
                "key", "value"
            ),
            null,
            new LogContext(),
            new Metrics().sensor("skipped-records")
        );
        ConsumeResult<object, object> record = recordDeserializer.deserialize(null, rawRecord);
        Assert.Equal(rawRecord.topic(), record.topic());
        Assert.Equal(rawRecord.partition(), record.partition());
        Assert.Equal(rawRecord.Offset, record.Offset);
        Assert.Equal(rawRecord.checksum(), record.checksum());
        Assert.Equal("key", record.Key);
        Assert.Equal("value", record.Value);
        Assert.Equal(rawRecord.Timestamp, record.Timestamp);
        Assert.Equal(TimestampType.CreateTime, record.timestampType());
        Assert.Equal(rawRecord.headers(), record.headers());
    }

    static class TheSourceNode : SourceNode<object, object> {
        private bool keyThrowsException;
        private bool valueThrowsException;
        private object key;
        private object value;

        TheSourceNode(bool keyThrowsException,
                      bool valueThrowsException,
                      object key,
                      object value) {
            super("", Collections.emptyList(), null, null);
            this.keyThrowsException = keyThrowsException;
            this.valueThrowsException = valueThrowsException;
            this.key = key;
            this.value = value;
        }

        
        public object DeserializeKey(string topic, Headers headers, byte[] data) {
            if (keyThrowsException) {
                throw new RuntimeException();
            }
            return key;
        }

        
        public object DeserializeValue(string topic, Headers headers, byte[] data) {
            if (valueThrowsException) {
                throw new RuntimeException();
            }
            return value;
        }
    }

}
