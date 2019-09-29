using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State
{
    public abstract class QueryableStoreTypeMatcher<T> : IQueryableStoreType<T>
    {
        private HashSet<Type> matchTo;

        public QueryableStoreTypeMatcher(HashSet<Type> matchTo)
        {
            this.matchTo = matchTo;
        }

        public bool accepts(IStateStore stateStore)
        {
            foreach (var matchToClass in matchTo)
            {
                if (!matchToClass.IsAssignableFrom(stateStore.GetType()))
                {
                    return false;
                }
            }

            return true;
        }

        public abstract T create(IStateStoreProvider storeProvider, string storeName);
    }
}
