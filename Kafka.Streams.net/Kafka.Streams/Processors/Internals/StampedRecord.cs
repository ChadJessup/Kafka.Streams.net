using Confluent.Kafka;

namespace Kafka.Streams.Processors.Internals
{
    public class StampedRecord : Stamped<ConsumeResult<object, object>>
    {
        private readonly ConsumeResult<object, object> record;

        public StampedRecord(ConsumeResult<object, object> record, long timestamp)
            : base(record, timestamp)
        {
            this.record = record;
        }

        public string Topic => this.record.Topic;
        public int partition => this.record.Partition;
        public object Key => this.record.Key;
        public object Value => this.record.Value;
        public long offset => this.record.Offset;
        public Headers Headers => this.record.Headers;

        public override string ToString()
        {
            return this.value.ToString() + ", timestamp = " + this.timestamp;
        }
    }
}