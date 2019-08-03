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
using Kafka.Streams.State.internals.SegmentedBytesStore.KeySchema;

import java.nio.ByteBuffer;

class SegmentedCacheFunction : CacheFunction
{

    private static int SEGMENT_ID_BYTES = 8;

    private KeySchema keySchema;
    private long segmentInterval;

    SegmentedCacheFunction(KeySchema keySchema, long segmentInterval)
{
        this.keySchema = keySchema;
        this.segmentInterval = segmentInterval;
    }

    public override Bytes key(Bytes cacheKey)
{
        return Bytes.wrap(bytesFromCacheKey(cacheKey));
    }

    public override Bytes cacheKey(Bytes key)
{
        return cacheKey(key, segmentId(key));
    }

    Bytes cacheKey(Bytes key, long segmentId)
{
        byte[] keyBytes = key.get();
        ByteBuffer buf = ByteBuffer.allocate(SEGMENT_ID_BYTES + keyBytes.length);
        buf.putLong(segmentId).put(keyBytes);
        return Bytes.wrap(buf.array());
    }

    static byte[] bytesFromCacheKey(Bytes cacheKey)
{
        byte[] binaryKey = new byte[cacheKey.get().length - SEGMENT_ID_BYTES];
        System.arraycopy(cacheKey.get(), SEGMENT_ID_BYTES, binaryKey, 0, binaryKey.length);
        return binaryKey;
    }

    public long segmentId(Bytes key)
{
        return segmentId(keySchema.segmentTimestamp(key));
    }

    long segmentId(long timestamp)
{
        return timestamp / segmentInterval;
    }

    long getSegmentInterval()
{
        return segmentInterval;
    }

    int compareSegmentedKeys(Bytes cacheKey, Bytes storeKey)
{
        long storeSegmentId = segmentId(storeKey);
        long cacheSegmentId = ByteBuffer.wrap(cacheKey.get()).getLong();

        int segmentCompare = Long.compare(cacheSegmentId, storeSegmentId);
        if (segmentCompare == 0)
{
            byte[] cacheKeyBytes = cacheKey.get();
            byte[] storeKeyBytes = storeKey.get();
            return Bytes.BYTES_LEXICO_COMPARATOR.compare(
                cacheKeyBytes, SEGMENT_ID_BYTES, cacheKeyBytes.length - SEGMENT_ID_BYTES,
                storeKeyBytes, 0, storeKeyBytes.length
            );
        } else
{
            return segmentCompare;
        }
    }
}
