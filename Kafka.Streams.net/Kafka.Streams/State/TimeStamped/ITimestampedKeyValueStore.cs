using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValue;

namespace Kafka.Streams.State.TimeStamped
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