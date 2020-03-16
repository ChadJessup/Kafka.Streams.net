using Kafka.Streams.Errors;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Queryable;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.State.Internals
{
    /**
     * A wrapper over all of the {@link StateStoreProvider}s in a Topology
     */
    public class QueryableStoreProvider
    {
        private readonly List<IStateStoreProvider> storeProviders;
        private readonly GlobalStateStoreProvider globalStoreProvider;

        public QueryableStoreProvider(
            List<IStateStoreProvider> storeProviders,
            GlobalStateStoreProvider globalStateStoreProvider)
        {
            this.storeProviders = new List<IStateStoreProvider>(storeProviders);
            this.globalStoreProvider = globalStateStoreProvider;
        }

        /**
         * Get a composite object wrapping the instances of the {@link IStateStore} with the provided
         * storeName and {@link QueryableStoreType}
         *
         * @param storeName          name of the store
         * @param queryableStoreType accept stores passing {@link QueryableStoreType#accepts(IStateStore)}
         * @param                The expected type of the returned store
         * @return A composite object that wraps the store instances.
         */
        public T getStore<T>(
            string storeName,
            IQueryableStoreType<T> queryableStoreType)
        {
            List<T> globalStore = globalStoreProvider.stores<T>(storeName, queryableStoreType);
            if (globalStore.Any())
            {
                return queryableStoreType.create(new WrappingStoreProvider(new List<IStateStoreProvider> { globalStoreProvider }), storeName);
            }

            var allStores = new List<T>();
            foreach (IStateStoreProvider storeProvider in storeProviders)
            {
                allStores.AddRange(storeProvider.stores(storeName, queryableStoreType));
            }
            if (!allStores.Any())
            {
                throw new InvalidStateStoreException("The state store, " + storeName + ", may have migrated to another instance.");
            }
            return queryableStoreType.create(
                    new WrappingStoreProvider(storeProviders),
                    storeName);
        }
    }
}