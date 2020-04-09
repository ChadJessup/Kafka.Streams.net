using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.Processors.Internals
{
    public class TimestampedKeyValueStoreReadOnlyDecorator<K, V>
        : KeyValueStoreReadOnlyDecorator<K, ValueAndTimestamp<V>>
        , ITimestampedKeyValueStore<K, V>
    {

        public TimestampedKeyValueStoreReadOnlyDecorator(KafkaStreamsContext context, ITimestampedKeyValueStore<K, V> inner)
            : base(context, inner)
        {
        }
    }
}
