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
namespace Kafka.Streams.State.Internals;

using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.State.IKeyValueIterator;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 */
class MergedSortedCacheKeyValueBytesStoreIterator : AbstractMergedSortedCacheStoreIterator<Bytes, Bytes, byte[], byte[]>
{


    MergedSortedCacheKeyValueBytesStoreIterator(PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                                IKeyValueIterator<Bytes, byte[]> storeIterator)
{
        base(cacheIterator, storeIterator);
    }

    public override KeyValue<Bytes, byte[]> deserializeStorePair(KeyValue<Bytes, byte[]> pair)
{
        return pair;
    }

    
    Bytes deserializeCacheKey(Bytes cacheKey)
{
        return cacheKey;
    }

    
    byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
{
        return cacheEntry.value();
    }

    public override Bytes deserializeStoreKey(Bytes key)
{
        return key;
    }

    public override int compare(Bytes cacheKey, Bytes storeKey)
{
        return cacheKey.CompareTo(storeKey);
    }
}
