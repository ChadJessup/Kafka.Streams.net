using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Windowed
{
    public class WindowStoreType<K, V> : QueryableStoreTypeMatcher<IReadOnlyWindowStore<K, V>>
    {
        public WindowStoreType()
            : base(new HashSet<Type>(new[] { typeof(IReadOnlyWindowStore<K, V>) }))
        {
        }

        public override IReadOnlyWindowStore<K, V> Create(IStateStoreProvider storeProvider, string storeName)
        {
            return null; // new CompositeReadOnlyWindowStore<K, V>(storeProvider, this, storeName);
        }
    }
}
