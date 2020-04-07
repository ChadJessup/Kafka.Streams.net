
using Kafka.Streams.Errors;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableValueGetterSupplier<K, V, K1, V1> : IKTableValueGetterSupplier<K, KeyValuePair<K1, V1>>
    {
        IKTableValueGetterSupplier<K, V> parentValueGetterSupplier;// = parentKTable.valueGetterSupplier();
        public IKTableValueGetter<K, KeyValuePair<K1, V1>> Get()
        {
            return new KTableMapValueGetter<K, V, K1, V1>(parentValueGetterSupplier.Get());
        }

        public string[] StoreNames()
        {
            throw new StreamsException("Underlying state store not accessible due to repartitioning.");
        }
    }
}
