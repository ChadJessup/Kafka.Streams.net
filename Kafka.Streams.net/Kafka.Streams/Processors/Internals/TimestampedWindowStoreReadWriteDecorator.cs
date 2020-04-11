using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.Processors.Internals
{
    public class TimestampedWindowStoreReadWriteDecorator<K, V>
        : WindowStoreReadWriteDecorator<K, IValueAndTimestamp<V>>,
        ITimestampedWindowStore<K, V>
    {
        public TimestampedWindowStoreReadWriteDecorator(
            KafkaStreamsContext context,
            ITimestampedWindowStore<K, V> inner)
            : base(context, inner)
        {
        }
    }
}
