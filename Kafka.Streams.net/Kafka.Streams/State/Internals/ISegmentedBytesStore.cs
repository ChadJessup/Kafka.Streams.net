using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.Internals
{
    /**
    * The interface representing a StateStore that has 1 or more segments that are based
    * on time.
    * @see RocksDBSegmentedBytesStore
*/
    public interface ISegmentedBytesStore : IStateStore
    {
        /**
         * Fetch All records from the segmented store with the provided key and time range
         * from All existing segments
         * @param key       the key to match
         * @param from      earliest time to match
         * @param to        latest time to match
         * @return  an iterator over key-value pairs
         */
        IKeyValueIterator<Bytes, byte[]> Fetch(Bytes key, long from, long to);

        /**
         * Fetch All records from the segmented store in the provided key range and time range
         * from All existing segments
         * @param keyFrom   The first key that could be in the range
         * @param keyTo     The last key that could be in the range
         * @param from      earliest time to match
         * @param to        latest time to match
         * @return  an iterator over key-value pairs
         */
        IKeyValueIterator<Bytes, byte[]> Fetch(Bytes keyFrom, Bytes keyTo, long from, long to);

        /**
         * Gets All the key-value pairs in the existing windows.
         *
         * @return an iterator over windowed key-value pairs {@code <IWindowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         */
        IKeyValueIterator<Bytes, byte[]> All();

        /**
         * Gets All the key-value pairs that belong to the windows within in the given time range.
         *
         * @param from the beginning of the time slot from which to search
         * @param to   the end of the time slot from which to search
         * @return an iterator over windowed key-value pairs {@code <IWindowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws NullReferenceException if null is used for any key
         */
        IKeyValueIterator<Bytes, byte[]> FetchAll(long from, long to);

        /**
         * Remove the record with the provided key. The key
         * should be a composite of the record key, and the timestamp information etc
         * as described by the {@link KeySchema}
         * @param key   the segmented key to remove
         */
        void Remove(Bytes key);

        /**
         * Write a new value to the store with the provided key. The key
         * should be a composite of the record key, and the timestamp information etc
         * as described by the {@link KeySchema}
         * @param key
         * @param value
         */
        void Put(Bytes key, byte[] value);

        /**
         * Get the record from the store with the given key. The key
         * should be a composite of the record key, and the timestamp information etc
         * as described by the {@link KeySchema}
         * @param key
         * @return
         */
        byte[] Get(Bytes key);
    }
}
