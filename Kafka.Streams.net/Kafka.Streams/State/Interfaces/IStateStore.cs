using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.State
{
    /**
     * A storage engine for managing state maintained by a stream processor.
     * <p>
     * If the store is implemented as a Persistent store, it <em>must</em> use the store Name as directory Name and write
     * All data into this store directory.
     * The store directory must be created with the state directory.
     * The state directory can be obtained via {@link IProcessorContext#stateDir() #stateDir()} using the
     * {@link IProcessorContext} provided via {@link #Init(IProcessorContext, IStateStore) Init(...)}.
     * <p>
     * Using nested store directories within the state directory isolates different state stores.
     * If a state store would write into the state directory directly, it might conflict with others state stores and thus,
     * data might get corrupted and/or Streams might fail with an error.
     * Furthermore, Kafka Streams relies on using the store Name as store directory Name to perform internal cleanup tasks.
     * <p>
     * This interface does not specify any query capabilities, which, of course,
     * would be query engine specific. Instead it just specifies the minimum
     * functionality required to reload a storage engine from its changelog as well
     * as basic lifecycle management.
     */
    public interface IStateStore
    {
        /**
         * The Name of this store.
         * @return the storage Name
         */
        string Name { get; }

        /**
         * Initializes this state store.
         * <p>
         * The implementation of this function must register the root store in the context via the
         * {@link IProcessorContext#register(IStateStore, StateRestoreCallback)} function, where the
         * first {@link IStateStore} parameter should always be the passed-in {@code root} object, and
         * the second parameter should be an object of user's implementation
         * of the {@link StateRestoreCallback} interface used for restoring the state store from the changelog.
         * <p>
         * Note that if the state store engine itself supports bulk writes, users can implement another
         * interface {@link BatchingStateRestoreCallback} which : {@link StateRestoreCallback} to
         * let users implement bulk-load restoration logic instead of restoring one record at a time.
         *
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         */
        void Init(IProcessorContext context, IStateStore root);

        /**
         * Flush any cached data
         */
        void Flush();

        /**
         * Close the storage engine.
         * Note that this function needs to be idempotent since it may be called
         * several times on the same state store.
         * <p>
         * Users only need to implement this function but should NEVER need to call this api explicitly
         * as it will be called by the library automatically when necessary
         */
        void Close();

        /**
         * Return if the storage is Persistent or not.
         *
         * @return  {@code true} if the storage is Persistent&mdash;{@code false} otherwise
         */
        bool Persistent();

        /**
         * Is this store open for reading and writing
         * @return {@code true} if the store is open
         */
        bool IsOpen();
        bool IsPresent();
    }
}
