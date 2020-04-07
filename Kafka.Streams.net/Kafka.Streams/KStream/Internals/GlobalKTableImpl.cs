
using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class GlobalKTableImpl<K, V> : IGlobalKTable<K, V>
    {
        public IKTableValueGetterSupplier<K, V> valueGetterSupplier { get; }
        public string QueryableStoreName { get; }

        public GlobalKTableImpl(
            IKTableValueGetterSupplier<K, V> valueGetterSupplier,
            string queryableStoreName)
        {
            this.valueGetterSupplier = valueGetterSupplier;
            this.QueryableStoreName = queryableStoreName;
        }
    }
}
