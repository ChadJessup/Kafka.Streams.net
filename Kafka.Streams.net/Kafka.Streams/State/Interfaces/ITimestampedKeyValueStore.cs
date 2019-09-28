using Kafka.Streams.State.Internals;

namespace Kafka.Streams.State
{
    /**
     * A key-(value/timestamp) store that supports put/get/delete and range queries.
     *
     * @param The key type
     * @param The value type
     */
    public interface ITimestampedKeyValueStore<K, V> : IKeyValueStore<K, ValueAndTimestamp<V>>
    {
    }
}