using Confluent.Kafka;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Internals;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class RecordDeserializerTest
    {

        private Headers headers = new Headers(new Header[] { new Header("key", "value".getBytes()) });
        private ConsumeResult<byte[], byte[]> rawRecord = new ConsumeResult<byte[], byte[]>("topic",
            1,
            1,
            10,
            TimestampType.LogAppendTime,
            5L,
            3,
            5,
            System.Array.Empty<byte>(),
            System.Array.Empty<byte>(),
            headers);


        [Fact]
        public void ShouldReturnConsumerRecordWithDeserializedValueWhenNoExceptions()
        {
            RecordDeserializer recordDeserializer = new RecordDeserializer(
                new TheSourceNode(
                    false,
                    false,
                    "key", "value"
                ),
                null
            //new LogContext()
            //new Metrics().sensor("skipped-records")
            );

            ConsumeResult<object, object> record = recordDeserializer.Deserialize(null, rawRecord);
            Assert.Equal(rawRecord.Topic, record.Topic);
            Assert.Equal(rawRecord.Partition, record.Partition);
            Assert.Equal(rawRecord.Offset, record.Offset);
            //Assert.Equal(rawRecord.Checksum, record.Checksum);
            Assert.Equal("key", record.Key);
            Assert.Equal("value", record.Value);
            Assert.Equal(rawRecord.Timestamp, record.Timestamp);
            Assert.Equal(TimestampType.CreateTime, record.Timestamp.Type);
            Assert.Equal(rawRecord.Headers, record.Headers);
        }

        class TheSourceNode : SourceNode<object, object>
        {
            private readonly bool keyThrowsException;
            private readonly bool valueThrowsException;
            private readonly object key;
            private readonly object value;

            public TheSourceNode(bool keyThrowsException,
                          bool valueThrowsException,
                          object key,
                          object value)
                : base("", Collections.emptyList(), null, null)
            {
                this.keyThrowsException = keyThrowsException;
                this.valueThrowsException = valueThrowsException;
                this.Key = key;
                this.Value = value;
            }

            public override object DeserializeKey(string topic, Headers headers, byte[] data)
            {
                if (keyThrowsException)
                {
                    throw new RuntimeException();
                }
                return key;
            }

            public override object DeserializeValue(string topic, Headers headers, byte[] data)
            {
                if (valueThrowsException)
                {
                    throw new RuntimeException();
                }
                return value;
            }
        }

    }
}
