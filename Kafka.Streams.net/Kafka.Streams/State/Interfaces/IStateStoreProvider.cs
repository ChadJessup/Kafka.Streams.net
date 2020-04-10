
using Kafka.Streams.State.Queryable;
using System.Collections.Generic;

namespace Kafka.Streams.State.Interfaces
{
    /**
     * Provides access to {@link IStateStore}s that have been created
     * as part of the {@link org.apache.kafka.streams.processor.Internals.ProcessorTopology}.
     * To get access to custom stores developers should implement {@link QueryableStoreType}.
     * @see QueryableStoreTypes
     */
    public interface IStateStoreProvider
    {
        /**
         * Find instances of IStateStore that are accepted by {@link QueryableStoreType#accepts} and
         * have the provided storeName.
         *
         * @param storeName             Name of the store
         * @param queryableStoreType    filter stores based on this queryableStoreType
         * @param                   The type of the Store
         * @return  List of the instances of the store in this topology. Empty List if not found
         */
        List<T> Stores<T>(string storeName, IQueryableStoreType<T> queryableStoreType);
    }
}
