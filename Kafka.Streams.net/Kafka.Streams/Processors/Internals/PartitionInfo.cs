using Confluent.Kafka;
using Kafka.Common;

namespace Kafka.Streams.Processors.Internals
{
    public class PartitionInfo
    {
        private string topic;
        private int partition;
        private object p;
        private Node[] nodes1;
        private Node[] nodes2;

        public PartitionInfo(string topic, int partition, object p, Node[] nodes1, Node[] nodes2)
        {
            this.topic = topic;
            this.partition = partition;
            this.p = p;
            this.nodes1 = nodes1;
            this.nodes2 = nodes2;
        }

        public string Topic { get; internal set; }
        public Partition Partition { get; internal set; }
    }
}
