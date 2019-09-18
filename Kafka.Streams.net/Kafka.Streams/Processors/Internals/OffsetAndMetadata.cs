using Confluent.Kafka;

namespace Kafka.Streams.Processor.Internals
{
    public class OffsetAndMetadata// : TopicPartitionOffset
    {
        public long offset { get; }

        public OffsetAndMetadata(long offset)
        {
            this.offset = offset;
        }
    }
}