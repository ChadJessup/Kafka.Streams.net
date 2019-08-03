/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
package org.apache.kafka.streams.state.internals;

using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.WindowStoreIterator;

import static org.apache.kafka.streams.state.internals.SegmentedCacheFunction.bytesFromCacheKey;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 */
class MergedSortedCacheWindowStoreIterator : AbstractMergedSortedCacheStoreIterator<Long, Long, byte[], byte[]> : WindowStoreIterator<byte[]>
{


    MergedSortedCacheWindowStoreIterator(PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                         KeyValueIterator<Long, byte[]> storeIterator)
{
        super(cacheIterator, storeIterator);
    }

    public override KeyValue<Long, byte[]> deserializeStorePair(KeyValue<Long, byte[]> pair)
{
        return pair;
    }

    @Override
    Long deserializeCacheKey(Bytes cacheKey)
{
        byte[] binaryKey = bytesFromCacheKey(cacheKey);
        return WindowKeySchema.extractStoreTimestamp(binaryKey);
    }

    @Override
    byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
{
        return cacheEntry.value();
    }

    public override Long deserializeStoreKey(Long key)
{
        return key;
    }

    public override int compare(Bytes cacheKey, Long storeKey)
{
        byte[] binaryKey = bytesFromCacheKey(cacheKey);

        Long cacheTimestamp = WindowKeySchema.extractStoreTimestamp(binaryKey);
        return cacheTimestamp.compareTo(storeKey);
    }
}
