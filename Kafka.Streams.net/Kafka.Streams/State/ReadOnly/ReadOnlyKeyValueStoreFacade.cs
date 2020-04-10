using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.State.ReadOnly
{
    public class ReadOnlyKeyValueStoreFacade<K, V> : IReadOnlyKeyValueStore<K, V>
    {
        protected ITimestampedKeyValueStore<K, V> Inner { get; }

        public ReadOnlyKeyValueStoreFacade(ITimestampedKeyValueStore<K, V> store)
        {
            this.Inner = store;
        }

        public V Get(K key)
        {
            return ValueAndTimestamp.GetValueOrNull(this.Inner.Get(key));
        }

        public IKeyValueIterator<K, V> Range(K from, K to)
        {
            return new KeyValueIteratorFacade<K, V>((IKeyValueIterator<K, V>)this.Inner.Range(from, to));
        }

        public IKeyValueIterator<K, V> All()
        {
            return new KeyValueIteratorFacade<K, V>((IKeyValueIterator<K, V>)this.Inner.All());
        }

        public long approximateNumEntries
            => this.Inner.approximateNumEntries;
    }
}
