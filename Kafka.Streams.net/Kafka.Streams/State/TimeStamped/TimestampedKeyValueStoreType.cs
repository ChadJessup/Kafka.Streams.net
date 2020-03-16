using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.TimeStamped
{
    public class TimestampedKeyValueStoreType<K, V>
            : QueryableStoreTypeMatcher<IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>>
    {
        public TimestampedKeyValueStoreType()
            : base(new HashSet<Type>(new[] { typeof(ITimestampedKeyValueStore<K, V>), typeof(IReadOnlyKeyValueStore<K, V>) }))
        {
        }

        public override IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> create(IStateStoreProvider storeProvider, string storeName)
        {
            return null; // new CompositeReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>(storeProvider, this, storeName);
        }
    }
}
