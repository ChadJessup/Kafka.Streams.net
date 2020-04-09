using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.Processors.Internals
{
    public class TimestampedKeyValueStoreReadWriteDecorator<K, V>
        : KeyValueStoreReadWriteDecorator<K, ValueAndTimestamp<V>>
        , ITimestampedKeyValueStore<K, V>
    {
        public TimestampedKeyValueStoreReadWriteDecorator(KafkaStreamsContext context, ITimestampedKeyValueStore<K, V> inner)
            : base(context, inner)
        {
        }
    }
}
