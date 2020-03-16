using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.TimeStamped
{
    /**
     * A key-(value/timestamp) store that supports put/get/delete and range queries.
     *
     * @param The key type
     * @param The value type
     */
    public interface ITimestampedKeyValueStore<K, V> : ITimestampedKeyValueStore, IKeyValueStore<K, ValueAndTimestamp<V>>
    {
    }

    public interface ITimestampedKeyValueStore
    {
    }
}
