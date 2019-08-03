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

using Kafka.Common.serialization.Deserializer;
using Kafka.Common.serialization.Serializer;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.kstream.Window;
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.kstream.internals.TimeWindow;
using Kafka.Streams.State.StateSerdes;

import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowKeySchema : RocksDBSegmentedBytesStore.KeySchema
{

    private static Logger LOG = LoggerFactory.getLogger(WindowKeySchema.class);

    private static int SEQNUM_SIZE = 4;
    private static int TIMESTAMP_SIZE = 8;
    private static int SUFFIX_SIZE = TIMESTAMP_SIZE + SEQNUM_SIZE;
    private static byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

    public override Bytes upperRange(Bytes key, long to)
{
        byte[] maxSuffix = ByteBuffer.allocate(SUFFIX_SIZE)
            .putLong(to)
            .putInt(Integer.MAX_VALUE)
            .array();

        return OrderedBytes.upperRange(key, maxSuffix);
    }

    public override Bytes lowerRange(Bytes key, long from)
{
        return OrderedBytes.lowerRange(key, MIN_SUFFIX);
    }

    public override Bytes lowerRangeFixedSize(Bytes key, long from)
{
        return WindowKeySchema.toStoreKeyBinary(key, Math.max(0, from), 0);
    }

    public override Bytes upperRangeFixedSize(Bytes key, long to)
{
        return WindowKeySchema.toStoreKeyBinary(key, to, Integer.MAX_VALUE);
    }

    public override long segmentTimestamp(Bytes key)
{
        return WindowKeySchema.extractStoreTimestamp(key.get());
    }

    public override HasNextCondition hasNextCondition(Bytes binaryKeyFrom,
                                             Bytes binaryKeyTo,
                                             long from,
                                             long to)
{
        return iterator ->
{
            while (iterator.hasNext())
{
                Bytes bytes = iterator.peekNextKey();
                Bytes keyBytes = Bytes.wrap(WindowKeySchema.extractStoreKeyBytes(bytes.get()));
                long time = WindowKeySchema.extractStoreTimestamp(bytes.get());
                if ((binaryKeyFrom == null || keyBytes.compareTo(binaryKeyFrom) >= 0)
                    && (binaryKeyTo == null || keyBytes.compareTo(binaryKeyTo) <= 0)
                    && time >= from
                    && time <= to)
{
                    return true;
                }
                iterator.next();
            }
            return false;
        };
    }

    public override <S : Segment> List<S> segmentsToSearch(Segments<S> segments,
                                                        long from,
                                                        long to)
{
        return segments.segments(from, to);
    }

    /**
     * Safely construct a time window of the given size,
     * taking care of bounding endMs to Long.MAX_VALUE if necessary
     */
    static TimeWindow timeWindowForSize(long startMs,
                                        long windowSize)
{
        long endMs = startMs + windowSize;

        if (endMs < 0)
{
            LOG.warn("Warning: window end time was truncated to Long.MAX");
            endMs = Long.MAX_VALUE;
        }
        return new TimeWindow(startMs, endMs);
    }

    // for pipe serdes

    public static <K> byte[] toBinary(Windowed<K> timeKey,
                                      Serializer<K> serializer,
                                      string topic)
{
        byte[] bytes = serializer.serialize(topic, timeKey.key());
        ByteBuffer buf = ByteBuffer.allocate(bytes.length + TIMESTAMP_SIZE);
        buf.put(bytes);
        buf.putLong(timeKey.window().start());

        return buf.array();
    }

    public static <K> Windowed<K> from(byte[] binaryKey,
                                       long windowSize,
                                       Deserializer<K> deserializer,
                                       string topic)
{
        byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
        K key = deserializer.deserialize(topic, bytes);
        Window window = extractWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    private static Window extractWindow(byte[] binaryKey,
                                        long windowSize)
{
        ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE);
        return timeWindowForSize(start, windowSize);
    }

    // for store serdes

    public static Bytes toStoreKeyBinary(Bytes key,
                                         long timestamp,
                                         int seqnum)
{
        byte[] serializedKey = key.get();
        return toStoreKeyBinary(serializedKey, timestamp, seqnum);
    }

    public static <K> Bytes toStoreKeyBinary(K key,
                                             long timestamp,
                                             int seqnum,
                                             StateSerdes<K, ?> serdes)
{
        byte[] serializedKey = serdes.rawKey(key);
        return toStoreKeyBinary(serializedKey, timestamp, seqnum);
    }

    public static Bytes toStoreKeyBinary(Windowed<Bytes> timeKey,
                                         int seqnum)
{
        byte[] bytes = timeKey.key().get();
        return toStoreKeyBinary(bytes, timeKey.window().start(), seqnum);
    }

    public static <K> Bytes toStoreKeyBinary(Windowed<K> timeKey,
                                             int seqnum,
                                             StateSerdes<K, ?> serdes)
{
        byte[] serializedKey = serdes.rawKey(timeKey.key());
        return toStoreKeyBinary(serializedKey, timeKey.window().start(), seqnum);
    }

    // package private for testing
    static Bytes toStoreKeyBinary(byte[] serializedKey,
                                  long timestamp,
                                  int seqnum)
{
        ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + TIMESTAMP_SIZE + SEQNUM_SIZE);
        buf.put(serializedKey);
        buf.putLong(timestamp);
        buf.putInt(seqnum);

        return Bytes.wrap(buf.array());
    }

    static byte[] extractStoreKeyBytes(byte[] binaryKey)
{
        byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
        return bytes;
    }

    static <K> K extractStoreKey(byte[] binaryKey,
                                 StateSerdes<K, ?> serdes)
{
        byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
        return serdes.keyFrom(bytes);
    }

    static long extractStoreTimestamp(byte[] binaryKey)
{
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
    }

    static int extractStoreSequence(byte[] binaryKey)
{
        return ByteBuffer.wrap(binaryKey).getInt(binaryKey.length - SEQNUM_SIZE);
    }

    public static <K> Windowed<K> fromStoreKey(byte[] binaryKey,
                                               long windowSize,
                                               Deserializer<K> deserializer,
                                               string topic)
{
        K key = deserializer.deserialize(topic, extractStoreKeyBytes(binaryKey));
        Window window = extractStoreWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    public static <K> Windowed<K> fromStoreKey(Windowed<Bytes> windowedKey,
                                               Deserializer<K> deserializer,
                                               string topic)
{
        K key = deserializer.deserialize(topic, windowedKey.key().get());
        return new Windowed<>(key, windowedKey.window());
    }

    public static Windowed<Bytes> fromStoreBytesKey(byte[] binaryKey,
                                                    long windowSize)
{
        Bytes key = Bytes.wrap(extractStoreKeyBytes(binaryKey));
        Window window = extractStoreWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    static Window extractStoreWindow(byte[] binaryKey,
                                     long windowSize)
{
        ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
        return timeWindowForSize(start, windowSize);
    }
}
