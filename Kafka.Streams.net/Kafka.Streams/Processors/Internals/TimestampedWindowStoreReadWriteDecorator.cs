using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.Processors.Internals
{
    public class TimestampedWindowStoreReadWriteDecorator<K, V>
        : WindowStoreReadWriteDecorator<K, ValueAndTimestamp<V>>
        , ITimestampedWindowStore<K, V>
    {
        public TimestampedWindowStoreReadWriteDecorator(ITimestampedWindowStore<K, V> inner)
            : base(inner)
        {
        }
    }
}
