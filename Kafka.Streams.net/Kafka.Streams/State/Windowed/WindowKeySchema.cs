using Confluent.Kafka;
using Kafka.Common.Extensions;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using System;

namespace Kafka.Streams.State.Windowed
{
    public static class WindowKeySchema
    {
        private const int SEQNUM_SIZE = 4;
        private const int TIMESTAMP_SIZE = 8;
        private const int SUFFIX_SIZE = TIMESTAMP_SIZE + SEQNUM_SIZE;
        private readonly static byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

        private static TimeWindow TimeWindowForSize(DateTime start, TimeSpan windowSize)
        {
            var end = start + windowSize;

            if (end.Ticks < 0)
            {
                //LOG.LogWarning("Warning: window end time was truncated to long.MAX");
                end = DateTime.MaxValue;
            }
            return new TimeWindow(start, end);
        }

        // for pipe serdes
        public static byte[] ToBinary<K>(
            IWindowed<K> timeKey,
            ISerializer<K> serializer,
            string topic)
        {
            byte[] bytes = serializer.Serialize(topic, timeKey.Key, isKey: true);
            ByteBuffer buf = new ByteBuffer().Allocate(bytes.Length + TIMESTAMP_SIZE);
            buf.Add(bytes);
            buf.PutLong(timeKey.Window.Start());

            return buf.Array();
        }

        public static IWindowed<K> From<K>(
            byte[] binaryKey,
            TimeSpan windowSize,
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
            TimeSpan windowSize)
        {
            ByteBuffer buffer = new ByteBuffer().Wrap(binaryKey);
            DateTime start = Timestamp.UnixTimestampMsToDateTime(buffer.GetLong(binaryKey.Length - TIMESTAMP_SIZE));

            return TimeWindowForSize(start, windowSize);
        }

        // for store serdes
        public static Bytes ToStoreKeyBinary(
            Bytes key,
            DateTime timestamp,
            int seqnum)
        {
            byte[] serializedKey = key.Get();
            return ToStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static Bytes ToStoreKeyBinary<K>(
            K key,
            DateTime timestamp,
            int seqnum,
            IStateSerdes<K, object> serdes)
        {
            byte[] serializedKey = serdes.RawKey(key);
            return ToStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static Bytes ToStoreKeyBinary(
            IWindowed<Bytes> timeKey,
            int seqnum)
        {
            byte[] bytes = timeKey.Key.Get();
            return ToStoreKeyBinary(bytes, timeKey.Window.StartTime, seqnum);
        }

        public static Bytes ToStoreKeyBinary<K>(
            IWindowed<K> timeKey,
            int seqnum,
            IStateSerdes<K, object> serdes)
        {
            byte[] serializedKey = serdes.RawKey(timeKey.Key);

            return ToStoreKeyBinary(serializedKey, timeKey.Window.StartTime, seqnum);
        }

        // package private for testing
        private static Bytes ToStoreKeyBinary(
            byte[] serializedKey,
            DateTime timestamp,
            int seqnum)
        {
            ByteBuffer buf = new ByteBuffer().Allocate(serializedKey.Length + TIMESTAMP_SIZE + SEQNUM_SIZE);
            buf.Add(serializedKey);
            buf.PutLong(timestamp.ToEpochMilliseconds());
            buf.PutInt(seqnum);

            return Bytes.Wrap(buf.Array());
        }

        private static byte[] ExtractStoreKeyBytes(byte[] binaryKey)
        {
            byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE];
            Array.Copy(binaryKey, 0, bytes, 0, bytes.Length);
            return bytes;
        }

        private static K ExtractStoreKey<K>(
            byte[] binaryKey,
            IStateSerdes<K, object> serdes)
        {
            byte[] bytes = new byte[binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE];
            Array.Copy(binaryKey, 0, bytes, 0, bytes.Length);
            return serdes.KeyFrom(bytes);
        }

        public static DateTime ExtractStoreTimestamp(byte[] binaryKey)
        {
            return Timestamp.UnixTimestampMsToDateTime(new ByteBuffer()
                .Wrap(binaryKey)
                .GetLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE));
        }

        private static int ExtractStoreSequence(byte[] binaryKey)
        {
            return new ByteBuffer().Wrap(binaryKey).GetInt(binaryKey.Length - SEQNUM_SIZE);
        }

        public static IWindowed<K> FromStoreKey<K>(
            byte[] binaryKey,
            TimeSpan windowSize,
            IDeserializer<K> deserializer,
            string topic)
        {
            K key = deserializer.Deserialize(topic, ExtractStoreKeyBytes(binaryKey), isKey: true);
            var window = ExtractStoreWindow(binaryKey, windowSize);

            return new Windowed<K>(key, window);
        }

        public static IWindowed<K> FromStoreKey<K>(
            IWindowed<Bytes> windowedKey,
            IDeserializer<K> deserializer,
            string topic)
        {
            var key = deserializer.Deserialize(topic, windowedKey.Key.Get(), isKey: true);
            return new Windowed<K>(key, windowedKey.Window);
        }

        public static IWindowed<Bytes> FromStoreBytesKey(
            byte[] binaryKey,
            TimeSpan windowSize)
        {
            Bytes key = Bytes.Wrap(ExtractStoreKeyBytes(binaryKey));
            var window = ExtractStoreWindow(binaryKey, windowSize);
            return new Windowed<Bytes>(key, window);
        }

        private static Window ExtractStoreWindow(byte[] binaryKey, TimeSpan windowSize)
        {
            ByteBuffer buffer = new ByteBuffer().Wrap(binaryKey);
            DateTime start = Timestamp.UnixTimestampMsToDateTime(buffer.GetLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE));

            return TimeWindowForSize(start, windowSize);
        }
    }
}
