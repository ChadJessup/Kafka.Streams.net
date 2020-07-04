using System;
using System.Collections.Generic;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.State.Internals
{
    public class MergedSortedCacheWindowStoreKeyValueIterator
        : AbstractMergedSortedCacheStoreIterator<IWindowed<Bytes>, IWindowed<Bytes>, byte[], byte[]>
    {
        private readonly IStateSerdes<Bytes, byte[]> serdes;
        private readonly TimeSpan windowSize;
        private readonly SegmentedCacheFunction cacheFunction;

        public MergedSortedCacheWindowStoreKeyValueIterator(
            KafkaStreamsContext context,
            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator,
            IKeyValueIterator<IWindowed<Bytes>, byte[]> underlyingIterator,
            IStateSerdes<Bytes, byte[]> serdes,
            TimeSpan windowSize,
            SegmentedCacheFunction cacheFunction)
            : base(context, filteredCacheIterator, underlyingIterator)
        {
            this.serdes = serdes;
            this.windowSize = windowSize;
            this.cacheFunction = cacheFunction;
        }

        public override IWindowed<Bytes> DeserializeStoreKey(IWindowed<Bytes> key)
        {
            return key;
        }

        public override KeyValuePair<IWindowed<Bytes>, byte[]> DeserializeStorePair(KeyValuePair<IWindowed<Bytes>, byte[]> pair)
        {
            return pair;
        }


        public override IWindowed<Bytes> DeserializeCacheKey(Bytes cacheKey)
        {
            byte[] binaryKey = this.cacheFunction.Key(cacheKey).Get();

            return WindowKeySchema.FromStoreKey(
                binaryKey,
                this.windowSize,
                this.serdes.KeyDeserializer(),
                this.serdes.Topic);
        }

        public override byte[] DeserializeCacheValue(LRUCacheEntry cacheEntry)
        {
            return cacheEntry.Value();
        }

        public override int Compare(Bytes cacheKey, IWindowed<Bytes> storeKey)
        {
            Bytes storeKeyBytes = WindowKeySchema.ToStoreKeyBinary(
                storeKey.Key,
                storeKey.Window.StartTime,
                0);

            return this.cacheFunction.CompareSegmentedKeys(cacheKey, storeKeyBytes);
        }
    }
}
