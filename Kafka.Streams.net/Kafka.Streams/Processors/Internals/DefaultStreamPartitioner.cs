using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Processors.Internals
{
    public class DefaultStreamPartitioner<K, V> : IStreamPartitioner<K, V>
    {
        private readonly Cluster cluster;
        private readonly DefaultPartitioner defaultPartitioner;
        private readonly ISerializer<K> keySerializer;

        public DefaultStreamPartitioner(ISerializer<K> keySerializer, Cluster cluster)
        {
            this.cluster = cluster;
            this.defaultPartitioner = new DefaultPartitioner();
            this.keySerializer = keySerializer;
        }

        public int Partition(string topic, K key, V value, int numPartitions)
        {
            var keyBytes = this.keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic));
            return defaultPartitioner.Partition(topic, key, keyBytes, value, null, cluster);
        }
    }
}