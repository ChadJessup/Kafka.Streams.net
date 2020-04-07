
using Kafka.Streams.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public interface ISerialized<K, V>
    {
        ISerde<K> keySerde { get; }
        ISerde<V> valueSerde { get; }
    }
}
