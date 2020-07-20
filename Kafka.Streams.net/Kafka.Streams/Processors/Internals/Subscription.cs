using Confluent.Kafka;
using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class Subscription
    {
        private readonly List<string> topics;
        public ByteBuffer userData { get; private set; }
        public List<TopicPartition> ownedPartitions { get; private set; }
        private string? groupInstanceId;

        public Subscription(List<string> topics, ByteBuffer userData, List<TopicPartition> ownedPartitions)
        {
            this.topics = topics;
            this.userData = userData;
            this.ownedPartitions = ownedPartitions;
            this.groupInstanceId = "";
        }

        public Subscription(List<string> topics, ByteBuffer userData)
            : this(topics, userData, new List<TopicPartition>())
        {
        }

        public Subscription(List<string> topics)
            : this(topics, null, new List<TopicPartition>())
        {
        }

        public void SetGroupInstanceId(string? groupInstanceId)
        {
            this.groupInstanceId = groupInstanceId;
        }
    }
}
