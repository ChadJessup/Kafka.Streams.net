using Confluent.Kafka;

namespace Kafka.Streams.Processor.Internals
{
    public class StampedRecord<K, V> : Stamped<ConsumeResult<K, V>>
    {
        private ConsumeResult<K, V> record;

        public StampedRecord(ConsumeResult<K, V> record, long timestamp)
            : base(record, timestamp)
        {
            this.record = record;
        }

        public string Topic => this.record.Topic;

        public int partition => this.record.Partition;

        public K Key => this.record.Key;

        public V Value => this.record.Value;

        public long offset => this.record.Offset;

        public Headers Headers => this.record.Headers;

        public override string ToString()
        {
            return value.ToString() + ", timestamp = " + timestamp;
        }
    }
}