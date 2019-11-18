using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Factories;
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

        public int partition(string topic, K key, V value, int numPartitions)
        {
            byte[] keyBytes = this.keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic));
            return defaultPartitioner.partition(topic, key, keyBytes, value, null, cluster);
        }
    }
}