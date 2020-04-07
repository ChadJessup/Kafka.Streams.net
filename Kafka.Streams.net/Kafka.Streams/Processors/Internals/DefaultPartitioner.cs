
using System;
using Confluent.Kafka;
using Kafka.Common;

namespace Kafka.Streams.Processors.Internals
{
    public class DefaultPartitioner// : IPartitioner
    {
        public int Partition<K, V>(string topic, K key, byte[] keyBytes, V value, object p, Cluster cluster)
        {
            throw new NotImplementedException();
        }
    }
}
