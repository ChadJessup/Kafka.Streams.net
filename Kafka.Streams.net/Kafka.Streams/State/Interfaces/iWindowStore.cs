/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Kafka.Streams.KStream;
using Kafka.Streams.Processor.Interfaces;

namespace Kafka.Streams.State.Interfaces
{
    /**
     * Interface for storing the aggregated values of fixed-size time windows.
     * <p>
     * Note, that the stores's physical key type is {@link Windowed Windowed&lt;K&gt;}.
     *
     * @param Type of keys
     * @param Type of values
     */
    public interface IWindowStore<K, V> : IStateStore, IReadOnlyWindowStore<K, V>
    {
        /**
         * Use the current record timestamp as the {@code windowStartTimestamp} and
         * delegate to {@link WindowStore#put(object, object, long)}.
         *
         * It's highly recommended to use {@link WindowStore#put(object, object, long)} instead, as the record timestamp
         * is unlikely to be the correct windowStartTimestamp in general.
         *
         * @param key The key to associate the value to
         * @param value The value to update, it can be null;
         *              if the serialized bytes are also null it is interpreted as deletes
         * @throws ArgumentNullException if the given key is {@code null}
         */
        void put(K key, V value);

        /**
         * Put a key-value pair into the window with given window start timestamp
         * @param key The key to associate the value to
         * @param value The value; can be null
         * @param windowStartTimestamp The timestamp of the beginning of the window to put the key/value into
         * @throws ArgumentNullException if the given key is {@code null}
         */
        void put(K key, V value, long windowStartTimestamp);

        /**
         * Get all the key-value pairs with the given key and the time range from all the existing windows.
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
         * And we call {@code store.fetch("A", 10, 20)} then the results will contain the first
         * three windows from the table above, i.e., all those where 10 &lt;= start time &lt;= 20.
         * <p>
         * For each key, the iterator guarantees ordering of windows, starting from the oldest/earliest
         * available window to the newest/latest window.
         *
         * @param key       the key to fetch
         * @param timeFrom  time range start (inclusive)
         * @param timeTo    time range end (inclusive)
         * @return an iterator over key-value pairs {@code <timestamp, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws ArgumentNullException if the given key is {@code null}
         */
        IWindowStoreIterator<V> fetch(
            K key,
            long timeFrom,
            long timeTo);

        //public IWindowStoreIterator<V> fetch(K key, DateTime from, DateTime to)
        //{
        //    return fetch(
        //        key,
        //        ApiUtils.validateMillisecondInstant(from, ApiUtils.prepareMillisCheckFailMsgPrefix(from, "from")),
        //        ApiUtils.validateMillisecondInstant(to, ApiUtils.prepareMillisCheckFailMsgPrefix(to, "to")));
        //}

        /**
         * Get all the key-value pairs in the given key range and time range from all the existing windows.
         * <p>
         * This iterator must be closed after use.
         *
         * @param from      the first key in the range
         * @param to        the last key in the range
         * @param timeFrom  time range start (inclusive)
         * @param timeTo    time range end (inclusive)
         * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws ArgumentNullException if one of the given keys is {@code null}
         */

        IKeyValueIterator<Windowed<K>, V> fetch(K from, K to, long timeFrom, long timeTo);

        //public IKeyValueIterator<Windowed<K>, V> fetch(
        //    K from,
        //    K to,
        //    DateTime fromTime,
        //    DateTime toTime)
        //{
        //    return fetch(
        //        from,
        //        to,
        //        ApiUtils.validateMillisecondInstant(fromTime, ApiUtils.prepareMillisCheckFailMsgPrefix(fromTime, "fromTime")),
        //        ApiUtils.validateMillisecondInstant(toTime, ApiUtils.prepareMillisCheckFailMsgPrefix(toTime, "toTime")));
        //}

        /**
         * Gets all the key-value pairs that belong to the windows within in the given time range.
         *
         * @param timeFrom the beginning of the time slot from which to search (inclusive)
         * @param timeTo   the end of the time slot from which to search (inclusive)
         * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         */
        IKeyValueIterator<Windowed<K>, V> fetchAll(
            long timeFrom,
            long timeTo);

        //public IKeyValueIterator<Windowed<K>, V> fetchAll(
        //    DateTime from,
        //    DateTime to)
        //{
        //    return fetchAll(
        //        ApiUtils.validateMillisecondInstant(from, ApiUtils.prepareMillisCheckFailMsgPrefix(from, "from")),
        //        ApiUtils.validateMillisecondInstant(to, ApiUtils.prepareMillisCheckFailMsgPrefix(to, "to")));
        //}
    }
}