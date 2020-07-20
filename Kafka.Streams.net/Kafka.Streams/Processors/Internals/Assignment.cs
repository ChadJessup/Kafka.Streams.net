using Confluent.Kafka;
using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class Assignment
    {
        public List<TopicPartition> partitions { get; }
        public ByteBuffer? userData { get; }

        public Assignment(List<TopicPartition> partitions, ByteBuffer? userData)
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
