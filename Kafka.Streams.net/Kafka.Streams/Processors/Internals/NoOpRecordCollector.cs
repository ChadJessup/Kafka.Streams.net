using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class NoOpRecordCollector : IRecordCollector
    {
        public ISupplier Supplier { get; }
        public Dictionary<TopicPartition, long> offsets { get; } = new Dictionary<TopicPartition, long>();

        public void Send<K, V>(
            string topic,
            K key,
            V value,
            Headers headers,
            int? partition,
            DateTime timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer)
        {
        }

        public void Send<K, V>(
            string topic,
            K key,
            V value,
            Headers headers,
            DateTime timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IStreamPartitioner<K, V> partitioner)
        { }

        public void Init(IProducer<byte[], byte[]> producer)
        { }

        public void Flush() { }

        public void Close() { }

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                }

                this.disposedValue = true;
            }
        }

        public void Dispose() => this.Dispose(true);

    }
}
