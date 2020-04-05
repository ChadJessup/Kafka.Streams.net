using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.RocksDbState;

namespace Kafka.Streams.State.Internals
{
    public class RocksDbKeyValueBytesStoreSupplier : IKeyValueBytesStoreSupplier
    {
        public string Name { get; }
        private readonly bool returnTimestampedStore;

        public RocksDbKeyValueBytesStoreSupplier(
            string name,
            bool returnTimestampedStore)
        {
            this.Name = name;
            this.returnTimestampedStore = returnTimestampedStore;
        }

        public IKeyValueStore<Bytes, byte[]> Get()
        {
            return returnTimestampedStore
                ? new RocksDbTimestampedStore(Name)
                : new RocksDbStore(Name);
        }

        public string MetricsScope()
        {
            return "rocksdb-state";
        }
    }
}