using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValue;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.State.ReadOnly
{
    public class ReadOnlyKeyValueStoreFacade<K, V> : IReadOnlyKeyValueStore<K, V>
    {
        protected ITimestampedKeyValueStore<K, V> inner;

        public ReadOnlyKeyValueStoreFacade(ITimestampedKeyValueStore<K, V> store)
        {
            inner = store;
        }

        public V get(K key)
        {
            return ValueAndTimestamp<V>.GetValueOrNull(inner.get(key));
        }

        public IKeyValueIterator<K, V> range(K from, K to)
        {
            return new KeyValueIteratorFacade<K, V>((IKeyValueIterator<K, V>)inner.range(from, to));
        }

        public IKeyValueIterator<K, V> all()
        {
            return new KeyValueIteratorFacade<K, V>((IKeyValueIterator<K, V>)inner.all());
        }

        public long approximateNumEntries
            => inner.approximateNumEntries;
    }
}