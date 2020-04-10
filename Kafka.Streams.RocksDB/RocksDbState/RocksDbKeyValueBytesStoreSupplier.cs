using Kafka.Streams.RocksDbState;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.RocksDbState
{
    public class RocksDbKeyValueBytesStoreSupplier : IKeyValueBytesStoreSupplier
    {
        private readonly KafkaStreamsContext context;

        public string Name { get; private set; }

        public RocksDbKeyValueBytesStoreSupplier(KafkaStreamsContext context)
        {
            this.context = context;
        }

        public virtual IKeyValueStore<Bytes, byte[]> Get()
        {
            return new RocksDbStore(this.Name);
        }

        public void SetName(string Name)
            => this.Name = Name;
    }
}