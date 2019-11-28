using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.State.Queryable
{
    /**
     * Used to enable querying of custom {@link IStateStore} types via the {@link KafkaStreams} API.
     *
     * @param The store type
     * @see QueryableStoreTypes
     */
    public interface IQueryableStoreType<T>
    {
        /**
         * Called when searching for {@link IStateStore}s to see if they
         * match the type expected by implementors of this interface.
         *
         * @param stateStore    The stateStore
         * @return true if it is a match
         */
        bool accepts(IStateStore stateStore);

        /**
         * Create an instance of {@code T} (usually a facade) that developers can use
         * to query the underlying {@link IStateStore}s.
         *
         * @param storeProvider     provides access to all the underlying IStateStore instances
         * @param storeName         The name of the Store
         * @return a read-only interface over a {@code IStateStore}
         *        (cf. {@link org.apache.kafka.streams.state.QueryableStoreTypes.KeyValueStoreType})
         */
        T create(IStateStoreProvider storeProvider,
                 string storeName);
    }
}