namespace Kafka.Streams.KStream.Internals
{
    public class KTableSourceValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        private readonly KafkaStreamsContext context;
        private readonly string? storeName;

        public KTableSourceValueGetterSupplier(
            KafkaStreamsContext context,
            string? storeName)
        {
            this.context = context;
            this.storeName = storeName;
        }

        public IKTableValueGetter<K, V> Get()
        {
            return new KTableSourceValueGetter<K, V>(this.context);
        }

        public string[] StoreNames()
        {
            return new string[] { storeName ?? string.Empty };
        }
    }
}
