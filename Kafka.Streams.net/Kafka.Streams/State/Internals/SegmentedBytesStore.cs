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
using Kafka.Common.Utils;
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * The interface representing a IStateStore that has 1 or more segments that are based
     * on time.
     * @see RocksDbSegmentedBytesStore
     */
    public interface SegmentedBytesStore : IStateStore
    {

        /**
         * Fetch all records from the segmented store with the provided key and time range
         * from all existing segments
         * @param key       the key to match
         * @param from      earliest time to match
         * @param to        latest time to match
         * @return  an iterator over key-value pairs
         */
        IKeyValueIterator<Bytes, byte[]> fetch(Bytes key, long from, long to);

        /**
         * Fetch all records from the segmented store in the provided key range and time range
         * from all existing segments
         * @param keyFrom   The first key that could be in the range
         * @param keyTo     The last key that could be in the range
         * @param from      earliest time to match
         * @param to        latest time to match
         * @return  an iterator over key-value pairs
         */
        IKeyValueIterator<Bytes, byte[]> fetch(Bytes keyFrom, Bytes keyTo, long from, long to);

        /**
         * Gets all the key-value pairs in the existing windows.
         *
         * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         */
        IKeyValueIterator<Bytes, byte[]> all();

        /**
         * Gets all the key-value pairs that belong to the windows within in the given time range.
         *
         * @param from the beginning of the time slot from which to search
         * @param to   the end of the time slot from which to search
         * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
         * @throws InvalidStateStoreException if the store is not initialized
         * @throws NullPointerException if null is used for any key
         */
        IKeyValueIterator<Bytes, byte[]> fetchAll(long from, long to);

        /**
         * Remove the record with the provided key. The key
         * should be a composite of the record key, and the timestamp information etc
         * as described by the {@link KeySchema}
         * @param key   the segmented key to Remove
         */
        void Remove(Bytes key);

        /**
         * Write a new value to the store with the provided key. The key
         * should be a composite of the record key, and the timestamp information etc
         * as described by the {@link KeySchema}
         * @param key
         * @param value
         */
        void put(Bytes key, byte[] value);

        /**
         * Get the record from the store with the given key. The key
         * should be a composite of the record key, and the timestamp information etc
         * as described by the {@link KeySchema}
         * @param key
         * @return
         */
        byte[] get(Bytes key);

{

        /**
         * Given a range of record keys and a time, construct a Segmented key that represents
         * the upper range of keys to search when performing range queries.
         * @see SessionKeySchema#upperRange
         * @see WindowKeySchema#upperRange
         * @param key
         * @param to
         * @return      The key that represents the upper range to search for in the store
         */
        Bytes upperRange(Bytes key, long to);

        /**
         * Given a range of record keys and a time, construct a Segmented key that represents
         * the lower range of keys to search when performing range queries.
         * @see SessionKeySchema#lowerRange
         * @see WindowKeySchema#lowerRange
         * @param key
         * @param from
         * @return      The key that represents the lower range to search for in the store
         */
        Bytes lowerRange(Bytes key, long from);

        /**
         * Given a range of fixed size record keys and a time, construct a Segmented key that represents
         * the upper range of keys to search when performing range queries.
         * @see SessionKeySchema#upperRange
         * @see WindowKeySchema#upperRange
         * @param key the last key in the range
         * @param to the last timestamp in the range
         * @return The key that represents the upper range to search for in the store
         */
        Bytes upperRangeFixedSize(Bytes key, long to);

        /**
         * Given a range of fixed size record keys and a time, construct a Segmented key that represents
         * the lower range of keys to search when performing range queries.
         * @see SessionKeySchema#lowerRange
         * @see WindowKeySchema#lowerRange
         * @param key the first key in the range
         * @param from the first timestamp in the range
         * @return      The key that represents the lower range to search for in the store
         */
        Bytes lowerRangeFixedSize(Bytes key, long from);

        /**
         * Extract the timestamp of the segment from the key. The key is a composite of
         * the record-key, any timestamps, plus any.Additional information.
         * @see SessionKeySchema#lowerRange
         * @see WindowKeySchema#lowerRange
         * @param key
         * @return
         */
        long segmentTimestamp(Bytes key);

        /**
         * Create an implementation of {@link HasNextCondition} that knows when
         * to stop iterating over the KeyValueSegments. Used during {@link SegmentedBytesStore#fetch(Bytes, Bytes, long, long)} operations
         * @param binaryKeyFrom the first key in the range
         * @param binaryKeyTo   the last key in the range
         * @param from          starting time range
         * @param to            ending time range
         * @return
         */
        HasNextCondition hasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to);

        /**
         * Used during {@link SegmentedBytesStore#fetch(Bytes, long, long)} operations to determine
         * which segments should be scanned.
         * @param segments
         * @param from
         * @param to
         * @return  List of segments to search
         */
        List<S> segmentsToSearch<S>(Segments<S> segments, long from, long to);
    }
}
