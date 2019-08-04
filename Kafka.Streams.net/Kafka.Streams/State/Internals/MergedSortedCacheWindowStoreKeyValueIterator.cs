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
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.StateSerdes;

class MergedSortedCacheWindowStoreKeyValueIterator
    : AbstractMergedSortedCacheStoreIterator<Windowed<Bytes>, Windowed<Bytes>, byte[], byte[]>
{

    private StateSerdes<Bytes, byte[]> serdes;
    private long windowSize;
    private SegmentedCacheFunction cacheFunction;

    MergedSortedCacheWindowStoreKeyValueIterator(
        PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator,
        KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator,
        StateSerdes<Bytes, byte[]> serdes,
        long windowSize,
        SegmentedCacheFunction cacheFunction
    )
{
        base(filteredCacheIterator, underlyingIterator);
        this.serdes = serdes;
        this.windowSize = windowSize;
        this.cacheFunction = cacheFunction;
    }

    
    Windowed<Bytes> deserializeStoreKey(Windowed<Bytes> key)
{
        return key;
    }

    
    KeyValue<Windowed<Bytes>, byte[]> deserializeStorePair(KeyValue<Windowed<Bytes>, byte[]> pair)
{
        return pair;
    }

    
    Windowed<Bytes> deserializeCacheKey(Bytes cacheKey)
{
        byte[] binaryKey = cacheFunction.key(cacheKey)[);
        return WindowKeySchema.fromStoreKey(binaryKey, windowSize, serdes.keyDeserializer(), serdes.topic());
    }

    
    byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
{
        return cacheEntry.value();
    }

    
    int compare(Bytes cacheKey, Windowed<Bytes> storeKey)
{
        Bytes storeKeyBytes = WindowKeySchema.toStoreKeyBinary(storeKey.key(), storeKey.window().start(), 0);
        return cacheFunction.compareSegmentedKeys(cacheKey, storeKeyBytes);
    }
}
