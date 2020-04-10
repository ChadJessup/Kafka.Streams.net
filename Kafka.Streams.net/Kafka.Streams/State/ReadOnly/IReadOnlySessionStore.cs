using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.ReadOnly
{
    /**
     * A session store that only supports read operations.
     * Implementations should be thread-safe as concurrent reads and writes
     * are expected.
     *
     * @param the key type
     * @param <AGG> the aggregated value type
     */
    public interface IReadOnlySessionStore<K, AGG> : IReadOnlySessionStore
    {
        /**
         * Retrieve All aggregated sessions for the provided key.
         * This iterator must be closed after use.
         *
         * For each key, the iterator guarantees ordering of sessions, starting from the oldest/earliest
         * available session to the newest/latest session.
         *
         * @param    key record key to find aggregated session values for
         * @return   KeyValueIterator containing All sessions for the provided key.
         * @throws   ArgumentNullException If null is used for key.
         *
         */
        IKeyValueIterator<IWindowed<K>, AGG> Fetch(K key);

        /**
         * Retrieve All aggregated sessions for the given range of keys.
         * This iterator must be closed after use.
         *
         * For each key, the iterator guarantees ordering of sessions, starting from the oldest/earliest
         * available session to the newest/latest session.
         *
         * @param    from first key in the range to find aggregated session values for
         * @param    to last key in the range to find aggregated session values for
         * @return   KeyValueIterator containing All sessions for the provided key.
         * @throws   ArgumentNullException If null is used for any of the keys.
         */
        IKeyValueIterator<IWindowed<K>, AGG> Fetch(K from, K to);
    }

    public interface IReadOnlySessionStore
    {
    }
}
