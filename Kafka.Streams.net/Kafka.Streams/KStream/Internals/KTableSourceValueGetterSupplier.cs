namespace Kafka.Streams.KStream.Internals
{
    public class KTableSourceValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        private readonly string? storeName;

        public KTableSourceValueGetterSupplier(string? storeName)
        {
            this.storeName = storeName;
        }

        public IKTableValueGetter<K, V> get()
        {
            return new KTableSourceValueGetter<K, V>();
        }

        public string[] storeNames()
        {
            return new string[] { storeName ?? string.Empty };
        }
    }
}
