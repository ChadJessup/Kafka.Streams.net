using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Interfaces
{
    /**
     * A supplier of a {@link RecordCollectorImpl} instance.
     */
    public interface ISupplier
    {
        /**
         * Get the record collector.
         * @return the record collector
         */
        IRecordCollector RecordCollector();
    }

    public interface IRecordCollector : IDisposable
    {
        ISupplier Supplier { get; }

        void Send<K, V>(
            string topic,
            K key,
            V value,
            Headers headers,
            int? partition,
            DateTime timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer);

        void Send<K, V>(
            string topic,
            K key,
            V value,
            Headers headers,
            DateTime timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IStreamPartitioner<K, V> partitioner);

        /**
         * Initialize the collector with a producer.
         * @param producer the producer that should be used by this collector
         */
        void Init(IProducer<byte[], byte[]> producer);

        /**
         * Flush the internal {@link Producer}.
         */
        void Flush();

        /**
         * Close the internal {@link Producer}.
         */
        void Close();

        /**
         * The last acked offsets from the internal {@link Producer}.
         *
         * @return the map from TopicPartition to offset
         */
        Dictionary<TopicPartition, long> offsets { get; }
    }
}
