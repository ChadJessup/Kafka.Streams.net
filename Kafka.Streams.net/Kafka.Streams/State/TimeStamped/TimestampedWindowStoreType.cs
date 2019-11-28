﻿using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.TimeStamped
{
    public class TimestampedWindowStoreType<K, V>
        : QueryableStoreTypeMatcher<IReadOnlyWindowStore<K, ValueAndTimestamp<V>>>
    {
        public TimestampedWindowStoreType()
            : base(new HashSet<Type>(new[]
            { typeof(ITimestampedWindowStore<K, V>), typeof(IReadOnlyWindowStore<K, V>) }))
        {
        }

        public override IReadOnlyWindowStore<K, ValueAndTimestamp<V>> create(IStateStoreProvider storeProvider, string storeName)
        {
            return null;// new CompositeReadOnlyWindowStore<K, ValueAndTimestamp<V>>(storeProvider, this, storeName);
        }
    }
}
