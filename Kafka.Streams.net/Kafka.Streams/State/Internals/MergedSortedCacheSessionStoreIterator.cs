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
using Kafka.Streams.kstream.Window;
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.State.KeyValueIterator;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 */
class MergedSortedCacheSessionStoreIterator : AbstractMergedSortedCacheStoreIterator<Windowed<Bytes>, Windowed<Bytes>, byte[], byte[]>
{

    private SegmentedCacheFunction cacheFunction;

    MergedSortedCacheSessionStoreIterator(PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                          KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator,
                                          SegmentedCacheFunction cacheFunction)
{
        super(cacheIterator, storeIterator);
        this.cacheFunction = cacheFunction;
    }

    public override KeyValue<Windowed<Bytes>, byte[]> deserializeStorePair(KeyValue<Windowed<Bytes>, byte[]> pair)
{
        return pair;
    }

    
    Windowed<Bytes> deserializeCacheKey(Bytes cacheKey)
{
        byte[] binaryKey = cacheFunction.key(cacheKey)[);
        byte[] keyBytes = SessionKeySchema.extractKeyBytes(binaryKey);
        Window window = SessionKeySchema.extractWindow(binaryKey);
        return new Windowed<>(Bytes.wrap(keyBytes), window);
    }


    
    byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
{
        return cacheEntry.value();
    }

    public override Windowed<Bytes> deserializeStoreKey(Windowed<Bytes> key)
{
        return key;
    }

    public override int compare(Bytes cacheKey, Windowed<Bytes> storeKey)
{
        Bytes storeKeyBytes = SessionKeySchema.toBinary(storeKey);
        return cacheFunction.compareSegmentedKeys(cacheKey, storeKeyBytes);
    }
}
