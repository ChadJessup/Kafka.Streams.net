using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Queryable
{
    public abstract class QueryableStoreTypeMatcher<T> : IQueryableStoreType<T>
    {
        private readonly HashSet<Type> matchTo;

        public QueryableStoreTypeMatcher(HashSet<Type> matchTo)
        {
            this.matchTo = matchTo;
        }

        public bool Accepts(IStateStore stateStore)
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

        public abstract T Create(IStateStoreProvider storeProvider, string storeName);
    }
}
