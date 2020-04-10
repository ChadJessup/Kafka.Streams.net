using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.TimeStamped
{
    /**
     * A key-(value/timestamp) store that supports Put/get/delete and range queries.
     *
     * @param The key type
     * @param The value type
     */
    public interface ITimestampedKeyValueStore<K, V> : ITimestampedKeyValueStore, IKeyValueStore<K, IValueAndTimestamp<V>>
    {
    }

    public interface ITimestampedKeyValueStore
    {
    }
}
