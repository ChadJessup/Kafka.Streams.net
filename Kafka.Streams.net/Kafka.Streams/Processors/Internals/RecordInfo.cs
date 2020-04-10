using Confluent.Kafka;
using Kafka.Streams.Nodes;

namespace Kafka.Streams.Processors.Internals
{
    public class RecordInfo
    {
        public RecordQueue queue { get; set; }

        public ProcessorNode Node()
        {
            return null;// queue.source();
        }

        public TopicPartition Partition()
        {
            return this.queue.partition;
        }
    }
}