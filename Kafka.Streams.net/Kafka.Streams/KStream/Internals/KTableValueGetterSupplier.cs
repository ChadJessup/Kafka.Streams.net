using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        private readonly IKTableValueGetterSupplier<K, V> parentValueGetterSupplier;

        public KTableValueGetterSupplier(IKTable<K, V> parentKTable)
        {
            if (parentKTable is null)
            {
                throw new System.ArgumentNullException(nameof(parentKTable));
            }

            this.parentValueGetterSupplier = parentKTable.ValueGetterSupplier<V>();
        }

        public IKTableValueGetter<K, V> Get()
        {
            return new KTableMapValueGetter<K, V>(this.parentValueGetterSupplier.Get());
        }

        public string[] StoreNames()
        {
            throw new StreamsException("Underlying state store not accessible due to repartitioning.");
        }
    }
}
