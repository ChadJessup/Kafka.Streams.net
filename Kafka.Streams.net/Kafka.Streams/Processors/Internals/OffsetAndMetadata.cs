namespace Kafka.Streams.Processors.Internals
{
    public class OffsetAndMetadata// : TopicPartitionOffset
    {
        public long offset { get; }

        public OffsetAndMetadata(long offset)
        {
            this.offset = offset;
        }

        public OffsetAndMetadata(long? offset1, string v)
        {
        }
    }
}