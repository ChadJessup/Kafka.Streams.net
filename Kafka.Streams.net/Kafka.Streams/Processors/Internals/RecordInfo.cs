using Confluent.Kafka;
using Kafka.Streams.Nodes;

namespace Kafka.Streams.Processors.Internals
{
    public class RecordInfo
    {
        public RecordQueue queue { get; set; }

        public ProcessorNode node()
        {
            return null;// queue.source();
        }

        public TopicPartition partition()
        {
            return queue.partition;
        }
    }
}