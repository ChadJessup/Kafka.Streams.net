using System;
using Confluent.Kafka;
using Kafka.Common.Extensions;
using Kafka.Streams.Internals;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State.Internals
{
    public class SegmentedCacheFunction : ICacheFunction
    {
        internal static int SEGMENT_ID_BYTES = 8;

        private IKeySchema keySchema;
        private TimeSpan segmentInterval;

        public SegmentedCacheFunction(IKeySchema keySchema, TimeSpan segmentInterval)
        {
            this.keySchema = keySchema;
            this.segmentInterval = segmentInterval;
        }

        public Bytes Key(Bytes cacheKey)
        {
            return Bytes.Wrap(BytesFromCacheKey(cacheKey));
        }

        public Bytes CacheKey(Bytes key)
        {
            return this.CacheKey(key, this.SegmentId(key));
        }

        private Bytes CacheKey(Bytes key, long segmentId)
        {
            byte[] keyBytes = key.Get();
            ByteBuffer buf = new ByteBuffer().Allocate(SEGMENT_ID_BYTES + keyBytes.Length);
            buf.PutLong(segmentId).Add(keyBytes);

            return Bytes.Wrap(buf.Array());
        }

        private static byte[] BytesFromCacheKey(Bytes cacheKey)
        {
            byte[] binaryKey = new byte[cacheKey.Get().Length - SEGMENT_ID_BYTES];
            Array.Copy(cacheKey.Get(), SEGMENT_ID_BYTES, binaryKey, 0, binaryKey.Length);

            return binaryKey;
        }

        public long SegmentId(Bytes key)
        {
            return this.SegmentId(this.keySchema.SegmentTimestamp(key));
        }

        private long SegmentId(DateTime timestamp)
        {
            return timestamp.Ticks / this.segmentInterval.Ticks;
        }

        public int CompareSegmentedKeys(Bytes cacheKey, Bytes storeKey)
        {
            long storeSegmentId = this.SegmentId(storeKey);
            long cacheSegmentId = new ByteBuffer().Wrap(cacheKey.Get()).GetLong();

            int segmentCompare = cacheSegmentId.CompareTo(storeSegmentId);
            if (segmentCompare == 0)
            {
                byte[] cacheKeyBytes = cacheKey.Get();
                byte[] storeKeyBytes = storeKey.Get();

                return Bytes.BYTES_LEXICO_COMPARATOR.Compare(
                    cacheKeyBytes, SEGMENT_ID_BYTES, cacheKeyBytes.Length - SEGMENT_ID_BYTES,
                    storeKeyBytes, 0, storeKeyBytes.Length);
            }
            else
            {
                return segmentCompare;
            }
        }
    }
}
