namespace Kafka.Streams.State.Internals
{
    public class ReadOnlyKeyValueStoreFacade<K, V> : IReadOnlyKeyValueStore<K, V>
    {
        protected TimestampedKeyValueStore<K, V> inner;

        protected ReadOnlyKeyValueStoreFacade(TimestampedKeyValueStore<K, V> store)
        {
            inner = store;
        }

        public override V get(K key)
        {
            return getValueOrNull(inner[key));
        }

        public override KeyValueIterator<K, V> range(K from,
                                            K to)
        {
            return new KeyValueIteratorFacade<>(inner.range(from, to));
        }

        public override KeyValueIterator<K, V> all()
        {
            return new KeyValueIteratorFacade<>(inner.all());
        }

        public override long approximateNumEntries()
        {
            return inner.approximateNumEntries();
        }
    }
}