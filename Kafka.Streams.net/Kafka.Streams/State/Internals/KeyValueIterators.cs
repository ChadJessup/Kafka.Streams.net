using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State.Internals
{
    public class KeyValueIterators<V> : EmptyKeyValueIterator<long, V>, IWindowStoreIterator<V>
    {
        public static IKeyValueIterator<long, V> EMPTY_ITERATOR { get; } = new EmptyKeyValueIterator<long, V>();
        public static IWindowStoreIterator<V> EMPTY_WINDOW_STORE_ITERATOR { get; } = new KeyValueIterators<V>();
    }
}