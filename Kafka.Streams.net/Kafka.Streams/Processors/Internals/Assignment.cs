using Confluent.Kafka;
using Kafka.Streams.KStream.Internals;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class Assignment
    {
        private readonly List<TopicPartition> partitions;
        private readonly ByteBuffer userData;

        public Assignment(List<TopicPartition> partitions, ByteBuffer userData)
        {
            this.partitions = partitions;
            this.userData = userData;
        }

        public Assignment(List<TopicPartition> partitions)
            : this(partitions, null)
        {
        }
    }
}
