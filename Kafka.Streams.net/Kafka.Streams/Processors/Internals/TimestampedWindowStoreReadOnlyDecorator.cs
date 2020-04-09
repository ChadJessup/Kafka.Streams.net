﻿using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.Processors.Internals
{
    public class TimestampedWindowStoreReadOnlyDecorator<K, V>
        : WindowStoreReadOnlyDecorator<K, ValueAndTimestamp<V>>
        , ITimestampedWindowStore<K, V>
    {

        public TimestampedWindowStoreReadOnlyDecorator(KafkaStreamsContext context, ITimestampedWindowStore<K, V> inner)
            : base(context, inner)
        {
        }
    }
}

