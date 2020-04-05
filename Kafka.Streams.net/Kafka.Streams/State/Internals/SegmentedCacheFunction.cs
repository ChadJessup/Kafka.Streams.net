

//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class SegmentedCacheFunction : ICacheFunction
//    {

//        private static int SEGMENT_ID_BYTES = 8;

//        private KeySchema keySchema;
//        private long segmentInterval;

//        SegmentedCacheFunction(KeySchema keySchema, long segmentInterval)
//        {
//            this.keySchema = keySchema;
//            this.segmentInterval = segmentInterval;
//        }

//        public override Bytes key(Bytes cacheKey)
//        {
//            return Bytes.Wrap(bytesFromCacheKey(cacheKey));
//        }

//        public override Bytes cacheKey(Bytes key)
//        {
//            return cacheKey(key, segmentId(key));
//        }

//        Bytes cacheKey(Bytes key, long segmentId)
//        {
//            byte[] keyBytes = key[];
//            ByteBuffer buf = sizeof(new ByteBuffer().Allocate(SEGMENT_ID) + keyBytes.Length);
//            buf.putLong(segmentId).Add(keyBytes);
//            return Bytes.Wrap(buf.array());
//        }

//        static byte[] bytesFromCacheKey(Bytes cacheKey)
//        {
//            byte[] binaryKey = sizeof(new byte[cacheKey[].Length - SEGMENT_ID)];
//            System.arraycopy(cacheKey(), SEGMENT_ID_BYTES, binaryKey, 0, binaryKey.Length);
//            return binaryKey;
//        }

//        public long segmentId(Bytes key)
//        {
//            return segmentId(keySchema.segmentTimestamp(key));
//        }

//        long segmentId(long timestamp)
//        {
//            return timestamp / segmentInterval;
//        }

//        long getSegmentInterval()
//        {
//            return segmentInterval;
//        }

//        int compareSegmentedKeys(Bytes cacheKey, Bytes storeKey)
//        {
//            long storeSegmentId = segmentId(storeKey);
//            long cacheSegmentId = ByteBuffer.Wrap(cacheKey()).getLong();

//            int segmentCompare = long.compare(cacheSegmentId, storeSegmentId);
//            if (segmentCompare == 0)
//            {
//                byte[] cacheKeyBytes = cacheKey[];
//                byte[] storeKeyBytes = storeKey[];
//                return Bytes.BYTES_LEXICO_COMPARATOR.compare(
//                    cacheKeyBytes, SEGMENT_ID_BYTES, cacheKeyBytes.Length - SEGMENT_ID_BYTES,
//                    storeKeyBytes, 0, storeKeyBytes.Length
//                );
//            }
//            else
//            {
//                return segmentCompare;
//            }
//        }
//    }
//}