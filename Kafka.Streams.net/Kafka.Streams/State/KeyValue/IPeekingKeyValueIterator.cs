using Kafka.Streams.State.KeyValues;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public interface IPeekingKeyValueIterator<K, V> : IKeyValueIterator<K, V>
    {
        KeyValuePair<K, V>? PeekNext();
    }
}