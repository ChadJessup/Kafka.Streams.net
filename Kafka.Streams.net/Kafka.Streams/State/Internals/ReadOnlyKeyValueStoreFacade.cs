namespace Kafka.Streams.State.Internals
{
    public class ReadOnlyKeyValueStoreFacade<K, V> : IReadOnlyKeyValueStore<K, V>
    {
        protected ITimestampedKeyValueStore<K, V> inner;

        protected ReadOnlyKeyValueStoreFacade(ITimestampedKeyValueStore<K, V> store)
        {
            inner = store;
        }

        public override V get(K key)
        {
            return getValueOrNull(inner[key]);
        }

        public override IKeyValueIterator<K, V> range(K from,
                                            K to)
        {
            return new KeyValueIteratorFacade<>(inner.range(from, to));
        }

        public override IKeyValueIterator<K, V> all()
        {
            return new KeyValueIteratorFacade<>(inner.all());
        }

        public override long approximateNumEntries()
        {
            return inner.approximateNumEntries();
        }
    }
}