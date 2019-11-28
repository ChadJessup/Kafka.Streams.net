using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State
{
    public class KeyValueStoreType<K, V> : QueryableStoreTypeMatcher<IReadOnlyKeyValueStore<K, V>>
    {
        public KeyValueStoreType()
            : base(new HashSet<Type> { typeof(IReadOnlyKeyValueStore<K, V>) })
        {
        }

        public override IReadOnlyKeyValueStore<K, V> create(IStateStoreProvider storeProvider, string storeName)
        {
            return null; // new CompositeReadOnlyKeyValueStore<K, V>(storeProvider, this, storeName);
        }
    }
}
