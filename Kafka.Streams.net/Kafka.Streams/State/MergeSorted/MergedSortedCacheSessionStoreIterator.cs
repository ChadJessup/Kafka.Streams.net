using System.Collections.Generic;
using Kafka.Streams.KStream;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;

namespace Kafka.Streams.State.MergeSorted
{
    public class MergedSortedCacheSessionStoreIterator : AbstractMergedSortedCacheStoreIterator<IWindowed<Bytes>, IWindowed<Bytes>, byte[], byte[]>
    {
        private SegmentedCacheFunction cacheFunction;

        public MergedSortedCacheSessionStoreIterator(
            KafkaStreamsContext context,
            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
            IKeyValueIterator<IWindowed<Bytes>, byte[]> storeIterator,
            SegmentedCacheFunction cacheFunction)
            : base(context, cacheIterator, storeIterator)
        {
            this.cacheFunction = cacheFunction;
        }

        public override KeyValuePair<IWindowed<Bytes>, byte[]> DeserializeStorePair(KeyValuePair<IWindowed<Bytes>, byte[]> pair)
        {
            return pair;
        }

        public override IWindowed<Bytes> DeserializeCacheKey(Bytes cacheKey)
        {
            byte[] binaryKey = this.cacheFunction.Key(cacheKey).Get();
            byte[] keyBytes = SessionKeySchema.ExtractKeyBytes(binaryKey);
            Window window = SessionKeySchema.ExtractWindow(binaryKey);

            return new Windowed2<Bytes>(Bytes.Wrap(keyBytes), window);
        }

        public override byte[] DeserializeCacheValue(LRUCacheEntry cacheEntry)
        {
            return cacheEntry.Value();
        }

        public override IWindowed<Bytes> DeserializeStoreKey(IWindowed<Bytes> key)
        {
            return key;
        }

        public override int Compare(Bytes cacheKey, IWindowed<Bytes> storeKey)
        {
            Bytes storeKeyBytes = SessionKeySchema.ToBinary(storeKey);
            return this.cacheFunction.CompareSegmentedKeys(cacheKey, storeKeyBytes);
        }
    }
}
