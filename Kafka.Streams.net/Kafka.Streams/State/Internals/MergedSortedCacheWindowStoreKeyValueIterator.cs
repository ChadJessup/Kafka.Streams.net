

//namespace Kafka.Streams.State.Internals
//{


//    using Kafka.Common.Utils.Bytes;
//    using Kafka.Streams.KeyValue;
//    using Kafka.Streams.KStream.Windowed;
//    using Kafka.Streams.State.IKeyValueIterator;
//    using Kafka.Streams.State.StateSerdes;

//    class MergedSortedCacheWindowStoreKeyValueIterator
//        : AbstractMergedSortedCacheStoreIterator<Windowed<Bytes>, Windowed<Bytes>, byte[], byte[]>
//    {

//        private StateSerdes<Bytes, byte[]> serdes;
//        private long windowSize;
//        private SegmentedCacheFunction cacheFunction;

//        MergedSortedCacheWindowStoreKeyValueIterator(
//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator,
//            IKeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator,
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


//        Windowed<Bytes> deserializeStoreKey(Windowed<Bytes> key)
//        {
//            return key;
//        }


//        KeyValue<Windowed<Bytes>, byte[]> deserializeStorePair(KeyValue<Windowed<Bytes>, byte[]> pair)
//        {
//            return pair;
//        }


//        Windowed<Bytes> deserializeCacheKey(Bytes cacheKey)
//        {
//            byte[] binaryKey = cacheFunction.key(cacheKey)[];
//            return WindowKeySchema.fromStoreKey(binaryKey, windowSize, serdes.keyDeserializer(), serdes.Topic);
//        }


//        byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
//        {
//            return cacheEntry.value();
//        }


//        int compare(Bytes cacheKey, Windowed<Bytes> storeKey)
//        {
//            Bytes storeKeyBytes = WindowKeySchema.toStoreKeyBinary(storeKey.key(), storeKey.window().start(), 0);
//            return cacheFunction.compareSegmentedKeys(cacheKey, storeKeyBytes);
//        }
//    }
//}