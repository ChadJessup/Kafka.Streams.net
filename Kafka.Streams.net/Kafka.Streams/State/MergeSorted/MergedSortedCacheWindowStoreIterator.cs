using System;
using System.Collections.Generic;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.State.MergeSorted
{
    public class MergedSortedCacheWindowStoreIterator
        : AbstractMergedSortedCacheStoreIterator<DateTime, DateTime, byte[], byte[]>,
        IWindowStoreIterator<byte[]>
    {
        public MergedSortedCacheWindowStoreIterator(
            KafkaStreamsContext context,
            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
            IKeyValueIterator<DateTime, byte[]> storeIterator)
            : base(context, cacheIterator, storeIterator)
        {
        }

        public override KeyValuePair<DateTime, byte[]> DeserializeStorePair(KeyValuePair<DateTime, byte[]> pair)
        {
            return pair;
        }

        public override DateTime DeserializeCacheKey(Bytes cacheKey)
        {
            if (cacheKey is null)
            {
                throw new ArgumentNullException(nameof(cacheKey));
            }

            byte[] binaryKey = BytesFromCacheKey(cacheKey);

            return WindowKeySchema.ExtractStoreTimestamp(binaryKey);
        }

        public override byte[] DeserializeCacheValue(LRUCacheEntry cacheEntry)
        {
            if (cacheEntry is null)
            {
                throw new ArgumentNullException(nameof(cacheEntry));
            }

            return cacheEntry.Value();
        }

        public override DateTime DeserializeStoreKey(DateTime key)
        {
            return key;
        }

        public override int Compare(Bytes cacheKey, DateTime storeKey)
        {
            if (cacheKey is null)
            {
                throw new ArgumentNullException(nameof(cacheKey));
            }

            byte[] binaryKey = BytesFromCacheKey(cacheKey);

            var cacheTimestamp = WindowKeySchema.ExtractStoreTimestamp(binaryKey);
            return cacheTimestamp.CompareTo(storeKey);
        }

        private static byte[] BytesFromCacheKey(Bytes cacheKey)
        {
            byte[] binaryKey = new byte[cacheKey.Get().Length - SegmentedCacheFunction.SEGMENT_ID_BYTES];
            Array.Copy(cacheKey.Get(), SegmentedCacheFunction.SEGMENT_ID_BYTES, binaryKey, 0, binaryKey.Length);

            return binaryKey;
        }
    }
}
