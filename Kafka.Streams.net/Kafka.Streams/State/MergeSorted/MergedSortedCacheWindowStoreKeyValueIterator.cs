

//namespace Kafka.Streams.State.Internals
//{


//    using Kafka.Common.Utils.Bytes;
//    using Kafka.Streams.KeyValuePair;
//    using Kafka.Streams.KStream.Windowed;
//    using Kafka.Streams.State.IKeyValueIterator;
//    using Kafka.Streams.State.StateSerdes;

//    class MergedSortedCacheWindowStoreKeyValueIterator
//        : AbstractMergedSortedCacheStoreIterator<IWindowed<Bytes>, IWindowed<Bytes>, byte[], byte[]>
//    {

//        private StateSerdes<Bytes, byte[]> serdes;
//        private long windowSize;
//        private SegmentedCacheFunction cacheFunction;

//        MergedSortedCacheWindowStoreKeyValueIterator(
//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator,
//            IKeyValueIterator<IWindowed<Bytes>, byte[]> underlyingIterator,
//            StateSerdes<Bytes, byte[]> serdes,
//            long windowSize,
//            SegmentedCacheFunction cacheFunction
//        )
//        {
//            base(filteredCacheIterator, underlyingIterator);
//            this.serdes = serdes;
//            this.windowSize = windowSize;
//            this.cacheFunction = cacheFunction;
//        }


//        IWindowed<Bytes> deserializeStoreKey(IWindowed<Bytes> key)
//        {
//            return key;
//        }


//        KeyValuePair<IWindowed<Bytes>, byte[]> deserializeStorePair(KeyValuePair<IWindowed<Bytes>, byte[]> pair)
//        {
//            return pair;
//        }


//        IWindowed<Bytes> deserializeCacheKey(Bytes cacheKey)
//        {
//            byte[] binaryKey = cacheFunction.key(cacheKey)[];
//            return WindowKeySchema.fromStoreKey(binaryKey, windowSize, serdes.keyDeserializer(), serdes.Topic);
//        }


//        byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
//        {
//            return cacheEntry.value();
//        }


//        int compare(Bytes cacheKey, IWindowed<Bytes> storeKey)
//        {
//            Bytes storeKeyBytes = WindowKeySchema.toStoreKeyBinary(storeKey.key(), storeKey.window().start(), 0);
//            return cacheFunction.compareSegmentedKeys(cacheKey, storeKeyBytes);
//        }
//    }
//}