using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Internals
{
    public interface IPeekingKeyValueIterator<K, V> : IKeyValueIterator<K, V>
    {
        KeyValue<K, V> peekNext();
    }
}