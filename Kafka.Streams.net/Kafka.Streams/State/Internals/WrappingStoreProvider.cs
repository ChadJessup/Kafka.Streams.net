
using Kafka.Streams.Errors;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Queryable;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.State.Internals
{
    /**
     * Provides a wrapper over multiple underlying {@link StateStoreProvider}s
     */
    public class WrappingStoreProvider : IStateStoreProvider
    {
        private readonly List<IStateStoreProvider> storeProviders;

        public WrappingStoreProvider(List<IStateStoreProvider> storeProviders)
        {
            this.storeProviders = storeProviders;
        }

        /**
         * Provides access to {@link org.apache.kafka.streams.processor.IStateStore}s accepted
         * by {@link QueryableStoreType#accepts(IStateStore)}
         * @param storeName  name of the store
         * @param type      The {@link QueryableStoreType}
         * @param       The type of the Store, for example, {@link org.apache.kafka.streams.state.IReadOnlyKeyValueStore}
         * @return  a List of all the stores with the storeName and are accepted by {@link QueryableStoreType#accepts(IStateStore)}
         */
        public List<T> Stores<T>(
            string storeName,
            IQueryableStoreType<T> type)
        {
            var allStores = new List<T>();
            foreach (IStateStoreProvider provider in storeProviders)
            {
                List<T> stores = provider.Stores(storeName, type);
                allStores.AddRange(stores);
            }
            if (!allStores.Any())
            {
                throw new InvalidStateStoreException("The state store, " + storeName + ", may have migrated to another instance.");
            }

            return allStores;
        }
    }
}
