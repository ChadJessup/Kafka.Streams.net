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
namespace Kafka.Streams.State.Internals
{


    using Kafka.Common.Utils.Bytes;
    using Kafka.Streams.KeyValue;
    using Kafka.Streams.State.IKeyValueIterator;
    using Kafka.Streams.State.WindowStoreIterator;



    /**
     * Merges two iterators. Assumes each of them is sorted by key
     *
     */
    class MergedSortedCacheWindowStoreIterator : AbstractMergedSortedCacheStoreIterator<long, long, byte[], byte[]> : WindowStoreIterator<byte[]>
    {


        MergedSortedCacheWindowStoreIterator(IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                             IKeyValueIterator<long, byte[]> storeIterator)
        {
            base(cacheIterator, storeIterator);
        }

        public override KeyValue<long, byte[]> deserializeStorePair(KeyValue<long, byte[]> pair)
        {
            return pair;
        }


        long deserializeCacheKey(Bytes cacheKey)
        {
            byte[] binaryKey = bytesFromCacheKey(cacheKey);
            return WindowKeySchema.extractStoreTimestamp(binaryKey);
        }


        byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
        {
            return cacheEntry.value();
        }

        public override long deserializeStoreKey(long key)
        {
            return key;
        }

        public override int compare(Bytes cacheKey, long storeKey)
        {
            byte[] binaryKey = bytesFromCacheKey(cacheKey);

            long cacheTimestamp = WindowKeySchema.extractStoreTimestamp(binaryKey);
            return cacheTimestamp.CompareTo(storeKey);
        }
    }
}