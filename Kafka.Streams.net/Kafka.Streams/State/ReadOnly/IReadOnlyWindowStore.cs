using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;
using System;

namespace Kafka.Streams.State.ReadOnly
{
    /**
     * A window store that only supports read operations.
     * Implementations should be thread-safe as concurrent reads and writes are expected.
     *
     * @param Type of keys
     * @param Type of values
     */
    public interface IReadOnlyWindowStore<K, V> : IReadOnlyWindowStore
    {
        /**
         * Get the value of key from a window.
         *
         * @param key       the key to Fetch
         * @param time      start timestamp (inclusive) of the window
         * @return The value or {@code null} if no value is found in the window
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws ArgumentNullException If {@code null} is used for any key.
         */
        V Fetch(K key, long time);

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
         * @throws ArgumentNullException If {@code null} is used for key.
         * @deprecated Use {@link #Fetch(object, Instant, Instant)} instead
         */
        [Obsolete]
        IWindowStoreIterator<V> Fetch(K key, long timeFrom, long timeTo);

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
         * And we call {@code store.Fetch("A", Instant.ofEpochMilli(10), Instant.ofEpochMilli(20))} then the results will contain the first
         * three windows from the table above, i.e., All those where 10 &lt;= start time &lt;= 20.
         * <p>
         * For each key, the iterator guarantees ordering of windows, starting from the oldest/earliest
         * available window to the newest/latest window.
         *
         * @param key       the key to Fetch
         * @param from      time range start (inclusive)
         * @param to        time range end (inclusive)
         * @return an iterator over key-value pairs {@code <timestamp, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws ArgumentNullException If {@code null} is used for key.
         * @throws ArgumentException if duration is negative or can't be represented as {@code long milliseconds}
         */
        IWindowStoreIterator<V> Fetch(K key, DateTime from, DateTime to);

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
         * @throws ArgumentNullException If {@code null} is used for any key.
         * @deprecated Use {@link #Fetch(object, object, Instant, Instant)} instead
         */
        [Obsolete]
        IKeyValueIterator<IWindowed<K>, V> Fetch(K from, K to, long timeFrom, long timeTo);

        /**
         * Get All the key-value pairs in the given key range and time range from All the existing windows.
         * <p>
         * This iterator must be closed after use.
         *
         * @param from      the first key in the range
         * @param to        the last key in the range
         * @param fromTime  time range start (inclusive)
         * @param toTime    time range end (inclusive)
         * @return an iterator over windowed key-value pairs {@code <IWindowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws ArgumentNullException If {@code null} is used for any key.
         * @throws ArgumentException if duration is negative or can't be represented as {@code long milliseconds}
         */
        IKeyValueIterator<IWindowed<K>, V> Fetch(K from, K to, DateTime fromTime, DateTime toTime);

        /**
        * Gets All the key-value pairs in the existing windows.
        *
        * @return an iterator over windowed key-value pairs {@code <IWindowed<K>, value>}
        * @throws InvalidStateStoreException if the store is not initialized
        */
        IKeyValueIterator<IWindowed<K>, V> All();

        /**
         * Gets All the key-value pairs that belong to the windows within in the given time range.
         *
         * @param timeFrom the beginning of the time slot from which to search (inclusive)
         * @param timeTo   the end of the time slot from which to search (inclusive)
         * @return an iterator over windowed key-value pairs {@code <IWindowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws ArgumentNullException if {@code null} is used for any key
         * @deprecated Use {@link #FetchAll(Instant, Instant)} instead
         */
        [Obsolete]
        IKeyValueIterator<IWindowed<K>, V> FetchAll(long timeFrom, long timeTo);

        /**
         * Gets All the key-value pairs that belong to the windows within in the given time range.
         *
         * @param from the beginning of the time slot from which to search (inclusive)
         * @param to   the end of the time slot from which to search (inclusive)
         * @return an iterator over windowed key-value pairs {@code <IWindowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws ArgumentNullException if {@code null} is used for any key
         * @throws ArgumentException if duration is negative or can't be represented as {@code long milliseconds}
         */
        IKeyValueIterator<IWindowed<K>, V> FetchAll(DateTime from, DateTime to);
    }

    public interface IReadOnlyWindowStore
    {
    }
}
