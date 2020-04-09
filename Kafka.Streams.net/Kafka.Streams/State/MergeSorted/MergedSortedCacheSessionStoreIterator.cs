using System.Collections.Generic;
using Kafka.Streams.KStream;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;

namespace Kafka.Streams.State.MergeSorted
{
    public class MergedSortedCacheSessionStoreIterator : AbstractMergedSortedCacheStoreIterator<Windowed<Bytes>, Windowed<Bytes>, byte[], byte[]>
    {
        private SegmentedCacheFunction cacheFunction;

        public MergedSortedCacheSessionStoreIterator(
            KafkaStreamsContext context,
            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
            IKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator,
            SegmentedCacheFunction cacheFunction)
            : base(context, cacheIterator, storeIterator)
        {
            this.cacheFunction = cacheFunction;
        }

        public override KeyValuePair<Windowed<Bytes>, byte[]> DeserializeStorePair(KeyValuePair<Windowed<Bytes>, byte[]> pair)
        {
            return pair;
        }

        public override Windowed<Bytes> DeserializeCacheKey(Bytes cacheKey)
        {
            byte[] binaryKey = cacheFunction.Key(cacheKey).Get();
            byte[] keyBytes = SessionKeySchema.ExtractKeyBytes(binaryKey);
            Window window = SessionKeySchema.ExtractWindow(binaryKey);

            return new Windowed<Bytes>(Bytes.Wrap(keyBytes), window);
        }

        public override byte[] DeserializeCacheValue(LRUCacheEntry cacheEntry)
        {
            return cacheEntry.Value();
        }

        public override Windowed<Bytes> DeserializeStoreKey(Windowed<Bytes> key)
        {
            return key;
        }

        public override int Compare(Bytes cacheKey, Windowed<Bytes> storeKey)
        {
            Bytes storeKeyBytes = SessionKeySchema.ToBinary(storeKey);
            return cacheFunction.CompareSegmentedKeys(cacheKey, storeKeyBytes);
        }
    }
}
