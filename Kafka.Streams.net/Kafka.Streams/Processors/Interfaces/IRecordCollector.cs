using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Interfaces
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
        IRecordCollector recordCollector();
    }

    public interface IRecordCollector : IDisposable
    {
        ISupplier Supplier { get; }

        void send<K, V>(
            string topic,
            K key,
            V value,
            Headers headers,
            int? partition,
            long timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer);

        void send<K, V>(
            string topic,
            K key,
            V value,
            Headers headers,
            long timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IStreamPartitioner<K, V> partitioner);

        /**
         * Initialize the collector with a producer.
         * @param producer the producer that should be used by this collector
         */
        void init(IProducer<byte[], byte[]> producer);

        /**
         * Flush the internal {@link Producer}.
         */
        void flush();

        /**
         * Close the internal {@link Producer}.
         */
        void close();

        /**
         * The last acked offsets from the internal {@link Producer}.
         *
         * @return the map from TopicPartition to offset
         */
        Dictionary<TopicPartition, long> offsets { get; }
    }
}
