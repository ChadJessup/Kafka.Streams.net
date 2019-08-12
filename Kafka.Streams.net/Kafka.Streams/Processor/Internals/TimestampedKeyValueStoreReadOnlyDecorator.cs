using Kafka.Streams.State;

namespace Kafka.Streams.Processor.Internals
{
    public class TimestampedKeyValueStoreReadOnlyDecorator<K, V>
        : KeyValueStoreReadOnlyDecorator<K, ValueAndTimestamp<V>>
        , ITimestampedKeyValueStore<K, V>
    {

        private TimestampedKeyValueStoreReadOnlyDecorator(ITimestampedKeyValueStore<K, V> inner)
            : base(inner)
        {
        }
    }
}
