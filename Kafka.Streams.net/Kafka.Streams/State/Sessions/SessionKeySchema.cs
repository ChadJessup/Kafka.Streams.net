
//using Confluent.Kafka;
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Sessions
//{
//    public class SessionKeySchema : ISegmentedBytesStore.KeySchema
//    {

//        private static int TIMESTAMP_SIZE = 8;
//        private static int SUFFIX_SIZE = 2 * TIMESTAMP_SIZE;
//        private static byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

//        public override Bytes upperRangeFixedSize(Bytes key, long to)
//        {
//            Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(to, long.MaxValue));
//            return SessionKeySchema.toBinary(sessionKey);
//        }

//        public override Bytes lowerRangeFixedSize(Bytes key, long from)
//        {
//            Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(0, Math.Max(0, from)));
//            return SessionKeySchema.toBinary(sessionKey);
//        }

//        public override Bytes upperRange(Bytes key, long to)
//        {
//            byte[] maxSuffix = ByteBuffer.allocate(SUFFIX_SIZE)
//                // the end timestamp can be as large as possible as long as it's larger than start time
//                .putLong(long.MaxValue)
//                // this is the start timestamp
//                .putLong(to)
//                .array();
//            return OrderedBytes.upperRange(key, maxSuffix);
//        }

//        public override Bytes lowerRange(Bytes key, long from)
//        {
//            return OrderedBytes.lowerRange(key, MIN_SUFFIX);
//        }

//        public override long segmentTimestamp(Bytes key)
//        {
//            return SessionKeySchema.extractEndTimestamp(key());
//        }

//        public override HasNextCondition hasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to)
//        {
//            //        return iterator=>
//            //{
//            //            while (iterator.hasNext())
//            //            {
//            //                Bytes bytes = iterator.peekNextKey();
//            //                Windowed<Bytes> windowedKey = SessionKeySchema.from(bytes);
//            //                if ((binaryKeyFrom == null || windowedKey.key().CompareTo(binaryKeyFrom) >= 0)
//            //                    && (binaryKeyTo == null || windowedKey.key().CompareTo(binaryKeyTo) <= 0)
//            //                    && windowedKey.window().end() >= from
//            //                    && windowedKey.window().start() <= to)
//            //                {
//            //                    return true;
//            //                }
//            //                iterator.next();
//            //            }
//            //            return false;
//            //        };
//        }

//        public override List<S> segmentsToSearch<S>(
//            Segments<S> segments,
//            long from,
//            long to)
//        {
//            return segments.segments(from, long.MaxValue);
//        }

//        private static K extractKey(byte[] binaryKey,
//                                        IDeserializer<K> deserializer,
//                                        string topic)
//        {
//            return deserializer.Deserialize(topic, extractKeyBytes(binaryKey));
//        }

//        static byte[] extractKeyBytes(byte[] binaryKey)
//        {
//            byte[] bytes = new byte[binaryKey.Length - 2 * TIMESTAMP_SIZE];
//            System.arraycopy(binaryKey, 0, bytes, 0, bytes.Length);
//            return bytes;
//        }

//        static long extractEndTimestamp(byte[] binaryKey)
//        {
//            return ByteBuffer.wrap(binaryKey).getLong(binaryKey.Length - 2 * TIMESTAMP_SIZE);
//        }

//        static long extractStartTimestamp(byte[] binaryKey)
//        {
//            return ByteBuffer.wrap(binaryKey).getLong(binaryKey.Length - TIMESTAMP_SIZE);
//        }

//        static Window extractWindow(byte[] binaryKey)
//        {
//            ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
//            long start = buffer.getLong(binaryKey.Length - TIMESTAMP_SIZE);
//            long end = buffer.getLong(binaryKey.Length - 2 * TIMESTAMP_SIZE);
//            return new SessionWindow(start, end);
//        }

//        public static Windowed<K> from(byte[] binaryKey,
//                                           IDeserializer<K> keyDeserializer,
//                                           string topic)
//        {
//            K key = extractKey(binaryKey, keyDeserializer, topic);
//            Window window = extractWindow(binaryKey);
//            return new Windowed<>(key, window);
//        }

//        public static Windowed<Bytes> from(Bytes bytesKey)
//        {
//            byte[] binaryKey = Array.Empty();
//            Window window = extractWindow(binaryKey);
//            return new Windowed<Bytes>(Bytes.wrap(extractKeyBytes(binaryKey)), window);
//        }

//        public static Windowed<K> from<K>(
//            Windowed<Bytes> keyBytes,
//            IDeserializer<K> keyDeserializer,
//            string topic)
//        {
//            K key = keyDeserializer.Deserialize(keyBytes.key, new SerializationContext(MessageComponentType.Key, topic);
//            return new Windowed<K>(key, keyBytes.window);
//        }

//        public static byte[] toBinary<K>(
//            Windowed<K> sessionKey,
//            ISerializer<K> serializer,
//            string topic)
//        {
//            byte[] bytes = serializer.Serialize(topic, sessionKey.key);
//            return toBinary(Bytes.wrap(bytes), sessionKey.window.start(), sessionKey.window.end())[];
//        }

//        public static Bytes toBinary(Windowed<Bytes> sessionKey)
//        {
//            return toBinary(sessionKey.key, sessionKey.window.start(), sessionKey.window.end());
//        }

//        public static Bytes toBinary(
//            Bytes key,
//            long startTime,
//            long endTime)
//        {
//            byte[] bytes = key[];
//            ByteBuffer buf = ByteBuffer.allocate(bytes.Length + 2 * TIMESTAMP_SIZE);
//            buf.Add(bytes);
//            buf.putLong(endTime);
//            buf.putLong(startTime);
//            return Bytes.wrap(buf.array());
//        }
//    }
//}