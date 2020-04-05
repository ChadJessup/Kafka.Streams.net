using Confluent.Kafka;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using System;

namespace Kafka.Streams.State.Windowed
{
    public class WindowKeySchema
    {
        private const int SEQNUM_SIZE = 4;
        private const int TIMESTAMP_SIZE = 8;
        private const int SUFFIX_SIZE = TIMESTAMP_SIZE + SEQNUM_SIZE;
        private readonly static byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

        static TimeWindow TimeWindowForSize(long startMs, long windowSize)
        {
            long endMs = startMs + windowSize;

            if (endMs < 0)
            {
                //LOG.LogWarning("Warning: window end time was truncated to long.MAX");
                endMs = long.MaxValue;
            }
            return new TimeWindow(startMs, endMs);
        }

        // for pipe serdes
        public static byte[] ToBinary<K>(
            Windowed<K> timeKey,
            ISerializer<K> serializer,
            string topic)
        {
            byte[] bytes = serializer.Serialize(topic, timeKey.Key, isKey: true);
            ByteBuffer buf = new ByteBuffer().Allocate(bytes.Length + TIMESTAMP_SIZE);
            buf.Add(bytes);
            buf.PutLong(timeKey.window.Start());

            return buf.Array();
        }

        public static Windowed<K> From<K>(
            byte[] binaryKey,
            long windowSize,
            IDeserializer<K> deserializer,
            string topic)
        {
            byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE];
            Array.Copy(binaryKey, 0, bytes, 0, bytes.Length);
            K key = deserializer.Deserialize(topic, bytes, isKey: true);
            Window window = ExtractWindow(binaryKey, windowSize);

            return new Windowed<K>(key, window);
        }

        private static Window ExtractWindow(
            byte[] binaryKey,
            long windowSize)
        {
            ByteBuffer buffer = new ByteBuffer().Wrap(binaryKey);
            long start = buffer.GetLong(binaryKey.Length - TIMESTAMP_SIZE);

            return TimeWindowForSize(start, windowSize);
        }

        // for store serdes
        public static Bytes ToStoreKeyBinary(
            Bytes key,
            long timestamp,
            int seqnum)
        {
            byte[] serializedKey = key.Get();
            return ToStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static Bytes ToStoreKeyBinary<K>(
            K key,
            long timestamp,
            int seqnum,
            StateSerdes<K, object> serdes)
        {
            byte[] serializedKey = serdes.RawKey(key);
            return ToStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static Bytes ToStoreKeyBinary(
            Windowed<Bytes> timeKey,
            int seqnum)
        {
            byte[] bytes = timeKey.Key.Get();
            return ToStoreKeyBinary(bytes, timeKey.window.Start(), seqnum);
        }

        public static Bytes ToStoreKeyBinary<K>(
            Windowed<K> timeKey,
            int seqnum,
            StateSerdes<K, object> serdes)
        {
            byte[] serializedKey = serdes.RawKey(timeKey.Key);

            return ToStoreKeyBinary(serializedKey, timeKey.window.Start(), seqnum);
        }

        // package private for testing
        static Bytes ToStoreKeyBinary(
            byte[] serializedKey,
            long timestamp,
            int seqnum)
        {
            ByteBuffer buf = new ByteBuffer().Allocate(serializedKey.Length + TIMESTAMP_SIZE + SEQNUM_SIZE);
            buf.Add(serializedKey);
            buf.PutLong(timestamp);
            buf.PutInt(seqnum);

            return Bytes.Wrap(buf.Array());
        }

        static byte[] ExtractStoreKeyBytes(byte[] binaryKey)
        {
            byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE];
            Array.Copy(binaryKey, 0, bytes, 0, bytes.Length);
            return bytes;
        }

        static K ExtractStoreKey<K>(
            byte[] binaryKey,
            StateSerdes<K, object> serdes)
        {
            byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE];
            Array.Copy(binaryKey, 0, bytes, 0, bytes.Length);
            return serdes.KeyFrom(bytes);
        }

        public static long ExtractStoreTimestamp(byte[] binaryKey)
        {
            return new ByteBuffer().Wrap(binaryKey).GetLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
        }

        static int ExtractStoreSequence(byte[] binaryKey)
        {
            return new ByteBuffer().Wrap(binaryKey).GetInt(binaryKey.Length - SEQNUM_SIZE);
        }

        public static Windowed<K> FromStoreKey<K>(
            byte[] binaryKey,
            long windowSize,
            IDeserializer<K> deserializer,
            string topic)
        {
            K key = deserializer.Deserialize(topic, ExtractStoreKeyBytes(binaryKey), isKey: true);
            var window = ExtractStoreWindow(binaryKey, windowSize);

            return new Windowed<K>(key, window);
        }

        public static Windowed<K> FromStoreKey<K>(
            Windowed<Bytes> windowedKey,
            IDeserializer<K> deserializer,
            string topic)
        {
            var key = deserializer.Deserialize(topic, windowedKey.Key.Get(), isKey: true);
            return new Windowed<K>(key, windowedKey.window);
        }

        public static Windowed<Bytes> FromStoreBytesKey(
            byte[] binaryKey,
            long windowSize)
        {
            Bytes key = Bytes.Wrap(ExtractStoreKeyBytes(binaryKey));
            var window = ExtractStoreWindow(binaryKey, windowSize);
            return new Windowed<Bytes>(key, window);
        }

        static Window ExtractStoreWindow(
            byte[] binaryKey,
            long windowSize)
        {
            ByteBuffer buffer = new ByteBuffer().Wrap(binaryKey);
            long start = buffer.GetLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
            return TimeWindowForSize(start, windowSize);
        }
    }
}
