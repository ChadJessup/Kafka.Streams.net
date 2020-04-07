

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMaterializedValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        private readonly string storeName;

        public KTableMaterializedValueGetterSupplier(string storeName)
        {
            this.storeName = storeName;
        }

        public IKTableValueGetter<K, V> Get()
        {
            return new KTableMaterializedValueGetter<K, V>();
        }


        public string[] StoreNames()
        {
            return new string[] { storeName };
        }
    }
}
