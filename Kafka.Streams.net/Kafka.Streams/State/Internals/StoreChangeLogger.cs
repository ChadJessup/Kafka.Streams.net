using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.State.Internals
{
    public class StoreChangeLogger<K, V>
    {
        private readonly string topic;
        private readonly int partition;
        private readonly IProcessorContext context;
        private readonly IRecordCollector collector;
        private readonly ISerializer<K> keySerializer;
        private readonly ISerializer<V> valueSerializer;

        public StoreChangeLogger(
            string storeName,
            IProcessorContext context,
            StateSerdes<K, V> serialization)
            : this(storeName, context, context.taskId.partition, serialization)
        {
        }

        private StoreChangeLogger(
            string storeName,
            IProcessorContext context,
            int partition,
            StateSerdes<K, V> serialization)
        {
            topic = ProcessorStateManager.storeChangelogTopic(context.applicationId, storeName);
            this.context = context;
            this.partition = partition;
            this.collector = ((ISupplier)context).recordCollector();
            keySerializer = serialization.keySerializer();
            valueSerializer = serialization.valueSerializer();
        }

        public void logChange(K key, V value)
        {
            logChange(key, value, context.timestamp);
        }

        void logChange(K key, V value, long timestamp)
        {
            // Sending null headers to changelog topics (KIP-244)
            collector.send(topic, key, value, null, partition, timestamp, keySerializer, valueSerializer);
        }
    }
}