using Kafka.Streams.State.KeyValues;
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

        public V Get(K key)
        {
            return ValueAndTimestamp.GetValueOrNull(inner.Get(key));
        }

        public IKeyValueIterator<K, V> Range(K from, K to)
        {
            return new KeyValueIteratorFacade<K, V>((IKeyValueIterator<K, V>)inner.Range(from, to));
        }

        public IKeyValueIterator<K, V> All()
        {
            return new KeyValueIteratorFacade<K, V>((IKeyValueIterator<K, V>)inner.All());
        }

        public long approximateNumEntries
            => inner.approximateNumEntries;
    }
}