using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State
{
    public class SessionStoreType<K, V> : QueryableStoreTypeMatcher<IReadOnlySessionStore<K, V>>
    {
        public SessionStoreType()
            : base(new HashSet<Type> { typeof(IReadOnlySessionStore<K, V>) })
        {
        }

        public override IReadOnlySessionStore<K, V> create(IStateStoreProvider storeProvider, string storeName)
        {
            return null;// new CompositeReadOnlySessionStore<>(storeProvider, this, storeName);
        }
    }
}
