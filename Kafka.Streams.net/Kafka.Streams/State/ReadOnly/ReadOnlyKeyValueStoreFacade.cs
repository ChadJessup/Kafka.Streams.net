using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.State.ReadOnly
{
    public class ReadOnlyKeyValueStoreFacade<K, V> : IReadOnlyKeyValueStore<K, V>
    {
        protected ITimestampedKeyValueStore<K, V> Inner { get; }

        public ReadOnlyKeyValueStoreFacade(ITimestampedKeyValueStore<K, V> store)
        {
            Inner = store;
        }

        public V Get(K key)
        {
            return ValueAndTimestamp.GetValueOrNull(Inner.Get(key));
        }

        public IKeyValueIterator<K, V> Range(K from, K to)
        {
            return new KeyValueIteratorFacade<K, V>((IKeyValueIterator<K, V>)Inner.Range(from, to));
        }

        public IKeyValueIterator<K, V> All()
        {
            return new KeyValueIteratorFacade<K, V>((IKeyValueIterator<K, V>)Inner.All());
        }

        public long approximateNumEntries
            => Inner.approximateNumEntries;
    }
}
