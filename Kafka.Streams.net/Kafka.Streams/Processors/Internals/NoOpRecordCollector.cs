using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
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
            long timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer)
        {
        }

        public void Send<K, V>(
            string topic,
            K key,
            V value,
            Headers headers,
            long timestamp,
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
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                this.disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~NoOpRecordCollector()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            this.Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
    }
}