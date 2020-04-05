using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Sessions
{
    public class SessionStoreType<K, V> : QueryableStoreTypeMatcher<IReadOnlySessionStore<K, V>>
    {
        public SessionStoreType()
            : base(new HashSet<Type> { typeof(IReadOnlySessionStore<K, V>) })
        {
        }

        public override IReadOnlySessionStore<K, V> Create(IStateStoreProvider storeProvider, string storeName)
        {
            return null;// new CompositeReadOnlySessionStore<>(storeProvider, this, storeName);
        }
    }
}
