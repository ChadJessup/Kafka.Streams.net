

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMaterializedValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        private readonly KafkaStreamsContext context;
        private readonly string storeName;

        public KTableMaterializedValueGetterSupplier(
            KafkaStreamsContext context,
            string storeName)
        {
            this.context = context;
            this.storeName = storeName;
        }

        public IKTableValueGetter<K, V> Get()
        {
            return new KTableMaterializedValueGetter<K, V>(this.context);
        }


        public string[] StoreNames()
        {
            return new string[] { this.storeName };
        }
    }
}
