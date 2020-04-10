using Kafka.Streams.RocksDbState;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.RocksDbState
{
    public class RocksDbKeyTimestampedValueBytesStoreSupplier : ITimestampedKeyValueBytesStoreSupplier
    {
        private readonly KafkaStreamsContext context;

        public string Name { get; private set; }

        public RocksDbKeyTimestampedValueBytesStoreSupplier(
            KafkaStreamsContext context)
        {
            this.context = context;
        }

        public virtual IKeyValueStore<Bytes, byte[]> Get()
        {
            return new RocksDbTimestampedStore(this.Name);
        }

        public void SetName(string Name)
            => this.Name = Name;
    }
}