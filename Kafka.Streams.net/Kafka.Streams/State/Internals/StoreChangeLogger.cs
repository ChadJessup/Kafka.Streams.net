using System;
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
            IStateSerdes<K, V> serialization)
            : this(storeName, context, context.TaskId.partition, serialization)
        {
        }

        private StoreChangeLogger(
            string storeName,
            IProcessorContext context,
            int partition,
            IStateSerdes<K, V> serialization)
        {
            this.topic = ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, storeName);
            this.context = context;
            this.partition = partition;
            this.collector = ((ISupplier)context).RecordCollector();
            this.keySerializer = serialization.KeySerializer();
            this.valueSerializer = serialization.ValueSerializer();
        }

        public void LogChange(K key, V value)
        {
            this.LogChange(key, value, this.context.Timestamp);
        }

        private void LogChange(K key, V value, DateTime timestamp)
        {
            // Sending null headers to changelog topics (KIP-244)
            this.collector.Send(this.topic, key, value, null, this.partition, timestamp, this.keySerializer, this.valueSerializer);
        }
    }
}
