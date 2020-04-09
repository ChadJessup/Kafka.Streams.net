using Confluent.Kafka;
using Kafka.Common.Utils;
using Kafka.Streams.Internals;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Sessions
{
    public class SessionKeySchema : IKeySchema
    {
        private static int TIMESTAMP_SIZE = 8;
        private static int SUFFIX_SIZE = 2 * TIMESTAMP_SIZE;
        private static byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

        public Bytes UpperRangeFixedSize(Bytes key, long to)
        {
            Windowed<Bytes> sessionKey = new Windowed<Bytes>(key, new SessionWindow(to, long.MaxValue));
            return SessionKeySchema.ToBinary(sessionKey);
        }

        public Bytes LowerRangeFixedSize(Bytes key, long from)
        {
            Windowed<Bytes> sessionKey = new Windowed<Bytes>(key, new SessionWindow(0, Math.Max(0, from)));
            return SessionKeySchema.ToBinary(sessionKey);
        }

        public Bytes UpperRange(Bytes key, long to)
        {
            byte[] maxSuffix = new ByteBuffer().Allocate(SUFFIX_SIZE)
                // the end timestamp can be as large as possible as long as it's larger than start time
                .PutLong(long.MaxValue)
                // this is the start timestamp
                .PutLong(to)
                .Array();
            return OrderedBytes.UpperRange(key, maxSuffix);
        }

        public Bytes LowerRange(Bytes key, long from)
        {
            return OrderedBytes.LowerRange(key, MIN_SUFFIX);
        }

        public long SegmentTimestamp(Bytes key)
        {
            return SessionKeySchema.ExtractEndTimestamp(key.Get());
        }

        //public override HasNextCondition hasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to)
        //{
        //    //        return iterator=>
        //    //{
        //    //            while (iterator.HasNext())
        //    //            {
        //    //                Bytes bytes = iterator.PeekNextKey();
        //    //                Windowed<Bytes> windowedKey = SessionKeySchema.from(bytes);
        //    //                if ((binaryKeyFrom == null || windowedKey.key().CompareTo(binaryKeyFrom) >= 0)
        //    //                    && (binaryKeyTo == null || windowedKey.key().CompareTo(binaryKeyTo) <= 0)
        //    //                    && windowedKey.window().end() >= from
        //    //                    && windowedKey.window().start() <= to)
        //    //                {
        //    //                    return true;
        //    //                }
        //    //                iterator.MoveNext();
        //    //            }
        //    //            return false;
        //    //        };
        //}

        public List<S> SegmentsToSearch<S>(ISegments<S> segments, long from, long to)
             where S : ISegment
        {
            if (segments is null)
            {
                throw new ArgumentNullException(nameof(segments));
            }

            return segments.GetSegments(from, long.MaxValue);
        }

        private static K ExtractKey<K>(
            byte[] binaryKey,
            IDeserializer<K> deserializer,
            string topic)
        {
            return deserializer.Deserialize(topic, ExtractKeyBytes(binaryKey), isKey: true);
        }

        public static byte[] ExtractKeyBytes(byte[] binaryKey)
        {
            byte[] bytes = new byte[binaryKey.Length - 2 * TIMESTAMP_SIZE];
            Array.Copy(binaryKey, 0, bytes, 0, bytes.Length);

            return bytes;
        }

        static long ExtractEndTimestamp(byte[] binaryKey)
        {
            return new ByteBuffer()
                .Wrap(binaryKey)
                .GetLong(binaryKey.Length - 2 * TIMESTAMP_SIZE);
        }

        static long ExtractStartTimestamp(byte[] binaryKey)
        {
            return new ByteBuffer()
                .Wrap(binaryKey)
                .GetLong(binaryKey.Length - TIMESTAMP_SIZE);
        }

        public static Window ExtractWindow(byte[] binaryKey)
        {
            ByteBuffer buffer = new ByteBuffer().Wrap(binaryKey);
            long start = buffer.GetLong(binaryKey.Length - TIMESTAMP_SIZE);
            long end = buffer.GetLong(binaryKey.Length - 2 * TIMESTAMP_SIZE);
            
            return new SessionWindow(start, end);
        }

        public static Windowed<K> From<K>(
            byte[] binaryKey,
            IDeserializer<K> keyDeserializer,
            string topic)
        {
            K key = ExtractKey(binaryKey, keyDeserializer, topic);
            Window window = ExtractWindow(binaryKey);

            return new Windowed<K>(key, window);
        }

        public static Windowed<Bytes> From(Bytes bytesKey)
        {
            byte[] binaryKey = Array.Empty<byte>();
            Window window = ExtractWindow(binaryKey);
            return new Windowed<Bytes>(Bytes.Wrap(ExtractKeyBytes(binaryKey)), window);
        }

        public static Windowed<K> From<K>(
            Windowed<Bytes> keyBytes,
            IDeserializer<K> keyDeserializer,
            string topic)
        {
            K key = keyDeserializer.Deserialize(topic, keyBytes.Key, isKey: true);
            return new Windowed<K>(key, keyBytes.window);
        }

        public static byte[] ToBinary<K>(
            Windowed<K> sessionKey,
            ISerializer<K> serializer,
            string topic)
        {
            byte[] bytes = serializer.Serialize(topic, sessionKey.Key, isKey: true);
            return ToBinary(
                    Bytes.Wrap(bytes),
                    sessionKey.window.Start(),
                    sessionKey.window.End())
                .Get();
        }

        public static Bytes ToBinary(Windowed<Bytes> sessionKey)
        {
            return ToBinary(sessionKey.Key, sessionKey.window.Start(), sessionKey.window.End());
        }

        public static Bytes ToBinary(
            Bytes key,
            long startTime,
            long endTime)
        {
            byte[] bytes = key.Get();
            ByteBuffer buf = new ByteBuffer()
                .Allocate(bytes.Length + 2 * TIMESTAMP_SIZE);

            buf.Add(bytes);
            buf.PutLong(endTime);
            buf.PutLong(startTime);
            return Bytes.Wrap(buf.Array());
        }

        public bool HasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to)
        {
            throw new NotImplementedException();
        }
    }
}
