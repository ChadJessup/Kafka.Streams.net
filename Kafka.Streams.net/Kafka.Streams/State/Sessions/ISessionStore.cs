using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValue;
using Kafka.Streams.State.ReadOnly;

namespace Kafka.Streams.State.Sessions
{
    /**
     * Interface for storing the aggregated values of sessions.
     * <p>
     * The key is internally represented as {@link Windowed Windowed&lt;K&gt;} that comprises the plain key
     * and the {@link Window} that represents window start- and end-timestamp.
     * <p>
     * If two sessions are merged, a new session with new start- and end-timestamp must be inserted into the store
     * while the two old sessions must be deleted.
     *
     * @param   type of the record keys
     * @param <AGG> type of the aggregated values
     */
    public interface ISessionStore<K, AGG> : IStateStore, IReadOnlySessionStore<K, AGG>
    {

        /**
         * Fetch any sessions with the matching key and the sessions end is &ge; earliestSessionEndTime and the sessions
         * start is &le; latestSessionStartTime
         *
         * This iterator must be closed after use.
         *
         * @param key the key to return sessions for
         * @param earliestSessionEndTime the end timestamp of the earliest session to search for
         * @param latestSessionStartTime the end timestamp of the latest session to search for
         * @return iterator of sessions with the matching key and aggregated values
         * @throws ArgumentNullException If null is used for key.
         */
        IKeyValueIterator<Windowed<K>, AGG> findSessions(K key, long earliestSessionEndTime, long latestSessionStartTime);

        /**
         * Fetch any sessions in the given range of keys and the sessions end is &ge; earliestSessionEndTime and the sessions
         * start is &le; latestSessionStartTime
         *
         * This iterator must be closed after use.
         *
         * @param keyFrom The first key that could be in the range
         * @param keyTo The last key that could be in the range
         * @param earliestSessionEndTime the end timestamp of the earliest session to search for
         * @param latestSessionStartTime the end timestamp of the latest session to search for
         * @return iterator of sessions with the matching keys and aggregated values
         * @throws ArgumentNullException If null is used for any key.
         */
        IKeyValueIterator<Windowed<K>, AGG> findSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime);

        /**
         * Get the value of key from a single session.
         *
         * @param key            the key to fetch
         * @param startTime      start timestamp of the session
         * @param endTime        end timestamp of the session
         * @return The value or {@code null} if no session associated with the key can be found
         * @throws ArgumentNullException If {@code null} is used for any key.
         */
        AGG fetchSession(K key, long startTime, long endTime);

        /**
         * Remove the session aggregated with provided {@link Windowed} key from the store
         * @param sessionKey key of the session to Remove
         * @throws ArgumentNullException If null is used for sessionKey.
         */
        void Remove(Windowed<K> sessionKey);

        /**
         * Write the aggregated value for the provided key to the store
         * @param sessionKey key of the session to write
         * @param aggregate  the aggregated value for the session, it can be null;
         *                   if the serialized bytes are also null it is interpreted as deletes
         * @throws ArgumentNullException If null is used for sessionKey.
         */
        void put(Windowed<K> sessionKey, AGG aggregate);
    }
}