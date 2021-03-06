using System;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.ReadOnly;

namespace Kafka.Streams.State.Windowed
{
    /**
     * Interface for storing the aggregated values of fixed-size time windows.
     * <p>
     * Note, that the stores's physical key type is {@link Windowed Windowed&lt;K&gt;}.
     *
     * @param Type of keys
     * @param Type of values
     */
    public interface IWindowStore<K, V> : IWindowStore, IStateStore, IReadOnlyWindowStore<K, V>
    {
        /**
         * Use the current record timestamp as the {@code windowStartTimestamp} and
         * delegate to {@link WindowStore#Put(object, object, long)}.
         *
         * It's highly recommended to use {@link WindowStore#Put(object, object, long)} instead, as the record timestamp
         * is unlikely to be the correct windowStartTimestamp in general.
         *
         * @param key The key to associate the value to
         * @param value The value to update, it can be null;
         *              if the serialized bytes are also null it is interpreted as deletes
         * @throws ArgumentNullException if the given key is {@code null}
         */
        void Put(K key, V value);

        /**
         * Put a key-value pair into the window with given window start timestamp
         * @param key The key to associate the value to
         * @param value The value; can be null
         * @param windowStartTimestamp The timestamp of the beginning of the window to Put the key/value into
         * @throws ArgumentNullException if the given key is {@code null}
         */
        void Put(K key, V value, DateTime windowStartTimestamp);

        /**
         * Get All the key-value pairs with the given key and the time range from All the existing windows.
         * <p>
         * This iterator must be closed after use.
         * <p>
         * The time range is inclusive and applies to the starting timestamp of the window.
         * For example, if we have the following windows:
         * <pre>
         * +-------------------------------+
         * |  key  | start time | end time |
         * +-------+------------+----------+
         * |   A   |     10     |    20    |
         * +-------+------------+----------+
         * |   A   |     15     |    25    |
         * +-------+------------+----------+
         * |   A   |     20     |    30    |
         * +-------+------------+----------+
         * |   A   |     25     |    35    |
         * +--------------------------------
         * </pre>
         * And we call {@code store.Fetch("A", 10, 20)} then the results will contain the first
         * three windows from the table above, i.e., All those where 10 &lt;= start time &lt;= 20.
         * <p>
         * For each key, the iterator guarantees ordering of windows, starting from the oldest/earliest
         * available window to the newest/latest window.
         *
         * @param key       the key to Fetch
         * @param timeFrom  time range start (inclusive)
         * @param timeTo    time range end (inclusive)
         * @return an iterator over key-value pairs {@code <timestamp, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws ArgumentNullException if the given key is {@code null}
         */
        new IWindowStoreIterator<V> Fetch(
            K key,
            DateTime timeFrom,
            DateTime timeTo);

        //public IWindowStoreIterator<V> Fetch(K key, DateTime from, DateTime to)
        //{
        //    return Fetch(
        //        key,
        //        ApiUtils.validateMillisecondInstant(from, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(from, "from")),
        //        ApiUtils.validateMillisecondInstant(to, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(to, "to")));
        //}

        /**
         * Get All the key-value pairs in the given key range and time range from All the existing windows.
         * <p>
         * This iterator must be closed after use.
         *
         * @param from      the first key in the range
         * @param to        the last key in the range
         * @param timeFrom  time range start (inclusive)
         * @param timeTo    time range end (inclusive)
         * @return an iterator over windowed key-value pairs {@code <IWindowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws ArgumentNullException if one of the given keys is {@code null}
         */
        new IKeyValueIterator<IWindowed<K>, V> Fetch(K from, K to, DateTime timeFrom, DateTime timeTo);

        //public IKeyValueIterator<IWindowed<K>, V> Fetch(
        //    K from,
        //    K to,
        //    DateTime fromTime,
        //    DateTime toTime)
        //{
        //    return Fetch(
        //        from,
        //        to,
        //        ApiUtils.validateMillisecondInstant(fromTime, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(fromTime, "fromTime")),
        //        ApiUtils.validateMillisecondInstant(toTime, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(toTime, "toTime")));
        //}

        /**
         * Gets All the key-value pairs that belong to the windows within in the given time range.
         *
         * @param timeFrom the beginning of the time slot from which to search (inclusive)
         * @param timeTo   the end of the time slot from which to search (inclusive)
         * @return an iterator over windowed key-value pairs {@code <IWindowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         */
        new IKeyValueIterator<IWindowed<K>, V> FetchAll(DateTime timeFrom, DateTime timeTo);

        void Add(K key, V value);

        //public IKeyValueIterator<IWindowed<K>, V> FetchAll(
        //    DateTime from,
        //    DateTime to)
        //{
        //    return FetchAll(
        //        ApiUtils.validateMillisecondInstant(from, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(from, "from")),
        //        ApiUtils.validateMillisecondInstant(to, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(to, "to")));
        //}
    }

    public interface IWindowStore
    {
    }
}
