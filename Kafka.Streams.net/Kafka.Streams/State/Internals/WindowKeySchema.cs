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

using Kafka.Common.serialization.Deserializer;
using Kafka.Common.serialization.Serializer;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KStream.Window;
using Kafka.Streams.KStream.Windowed;
using Kafka.Streams.KStream.Internals.TimeWindow;
using Kafka.Streams.State.StateSerdes;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

public class WindowKeySchema : RocksDBSegmentedBytesStore.KeySchema
{

    private static ILogger LOG = new LoggerFactory().CreateLogger<WindowKeySchema>();

    private static int SEQNUM_SIZE = 4;
    private static int TIMESTAMP_SIZE = 8;
    private static int SUFFIX_SIZE = TIMESTAMP_SIZE + SEQNUM_SIZE;
    private static byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

    public override Bytes upperRange(Bytes key, long to)
    {
        byte[] maxSuffix = ByteBuffer.allocate(SUFFIX_SIZE)
            .putLong(to)
            .putInt(int.MaxValue)
            .array();

        return OrderedBytes.upperRange(key, maxSuffix);
    }

    public override Bytes lowerRange(Bytes key, long from)
    {
        return OrderedBytes.lowerRange(key, MIN_SUFFIX);
    }

    public override Bytes lowerRangeFixedSize(Bytes key, long from)
    {
        return WindowKeySchema.toStoreKeyBinary(key, Math.Max(0, from), 0);
    }

    public override Bytes upperRangeFixedSize(Bytes key, long to)
    {
        return WindowKeySchema.toStoreKeyBinary(key, to, int.MaxValue);
    }

    public override long segmentTimestamp(Bytes key)
    {
        return WindowKeySchema.extractStoreTimestamp(key());
    }

    public override HasNextCondition hasNextCondition(Bytes binaryKeyFrom,
                                             Bytes binaryKeyTo,
                                             long from,
                                             long to)
    {
        //        return iterator=>
        //{
        //            while (iterator.hasNext())
        //            {
        //                Bytes bytes = iterator.peekNextKey();
        //                Bytes keyBytes = Bytes.wrap(WindowKeySchema.extractStoreKeyBytes(bytes()));
        //                long time = WindowKeySchema.extractStoreTimestamp(bytes());
        //                if ((binaryKeyFrom == null || keyBytes.CompareTo(binaryKeyFrom) >= 0)
        //                    && (binaryKeyTo == null || keyBytes.CompareTo(binaryKeyTo) <= 0)
        //                    && time >= from
        //                    && time <= to)
        //                {
        //                    return true;
        //                }
        //                iterator.next();
        //            }
        //            return false;
        //        };
    }

    public override List<S> segmentsToSearch<S>(
        Segments<S> segments,
        long from,
        long to)
    {
        return segments.segments(from, to);
    }

    /**
     * Safely construct a time window of the given size,
     * taking care of bounding endMs to long.MaxValue if necessary
     */
    static TimeWindow timeWindowForSize(long startMs,
                                        long windowSize)
    {
        long endMs = startMs + windowSize;

        if (endMs < 0)
        {
            LOG.LogWarning("Warning: window end time was truncated to long.MAX");
            endMs = long.MaxValue;
        }
        return new TimeWindow(startMs, endMs);
    }

    // for pipe serdes

    public static byte[] toBinary(Windowed<K> timeKey,
                                      ISerializer<K> serializer,
                                      string topic)
    {
        byte[] bytes = serializer.Serialize(topic, timeKey.key());
        ByteBuffer buf = ByteBuffer.allocate(bytes.Length + TIMESTAMP_SIZE);
        buf.Add(bytes);
        buf.putLong(timeKey.window().start());

        return buf.array();
    }

    public static Windowed<K> from(byte[] binaryKey,
                                       long windowSize,
                                       IDeserializer<K> deserializer,
                                       string topic)
    {
        byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.Length);
        K key = deserializer.Deserialize(topic, bytes);
        Window window = extractWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    private static Window extractWindow(byte[] binaryKey,
                                        long windowSize)
    {
        ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
        long start = buffer.getLong(binaryKey.Length - TIMESTAMP_SIZE);
        return timeWindowForSize(start, windowSize);
    }

    // for store serdes

    public static Bytes toStoreKeyBinary(Bytes key,
                                         long timestamp,
                                         int seqnum)
    {
        byte[] serializedKey = key[];
        return toStoreKeyBinary(serializedKey, timestamp, seqnum);
    }

    public static Bytes toStoreKeyBinary(K key,
                                             long timestamp,
                                             int seqnum,
                                             StateSerdes<K, object> serdes)
    {
        byte[] serializedKey = serdes.rawKey(key);
        return toStoreKeyBinary(serializedKey, timestamp, seqnum);
    }

    public static Bytes toStoreKeyBinary(Windowed<Bytes> timeKey,
                                         int seqnum)
    {
        byte[] bytes = timeKey.key()[];
        return toStoreKeyBinary(bytes, timeKey.window().start(), seqnum);
    }

    public static Bytes toStoreKeyBinary(Windowed<K> timeKey,
                                             int seqnum,
                                             StateSerdes<K, object> serdes)
    {
        byte[] serializedKey = serdes.rawKey(timeKey.key());
        return toStoreKeyBinary(serializedKey, timeKey.window().start(), seqnum);
    }

    // package private for testing
    static Bytes toStoreKeyBinary(byte[] serializedKey,
                                  long timestamp,
                                  int seqnum)
    {
        ByteBuffer buf = ByteBuffer.allocate(serializedKey.Length + TIMESTAMP_SIZE + SEQNUM_SIZE);
        buf.Add(serializedKey);
        buf.putLong(timestamp);
        buf.putInt(seqnum);

        return Bytes.wrap(buf.array());
    }

    static byte[] extractStoreKeyBytes(byte[] binaryKey)
    {
        byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.Length);
        return bytes;
    }

    static K extractStoreKey(byte[] binaryKey,
                                 StateSerdes<K, object> serdes)
    {
        byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE];
        System.arraycopy(binaryKey, 0, bytes, 0, bytes.Length);
        return serdes.keyFrom(bytes);
    }

    static long extractStoreTimestamp(byte[] binaryKey)
    {
        return ByteBuffer.wrap(binaryKey).getLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
    }

    static int extractStoreSequence(byte[] binaryKey)
    {
        return ByteBuffer.wrap(binaryKey).getInt(binaryKey.Length - SEQNUM_SIZE);
    }

    public static Windowed<K> fromStoreKey(byte[] binaryKey,
                                               long windowSize,
                                               IDeserializer<K> deserializer,
                                               string topic)
    {
        K key = deserializer.Deserialize(topic, extractStoreKeyBytes(binaryKey));
        Window window = extractStoreWindow(binaryKey, windowSize);
        return new Windowed<>(key, window);
    }

    public static Windowed<K> fromStoreKey(Windowed<Bytes> windowedKey,
                                               IDeserializer<K> deserializer,
                                               string topic)
    {
        K key = deserializer.Deserialize(topic, windowedKey.key()());
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
        long start = buffer.getLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
        return timeWindowForSize(start, windowSize);
    }
}
