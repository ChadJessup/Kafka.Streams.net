using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValue;

namespace Kafka.Streams.State.Internals
{
    public interface IPeekingKeyValueIterator<K, V> : IKeyValueIterator<K, V>
    {
        KeyValue<K, V> peekNext();
    }
}