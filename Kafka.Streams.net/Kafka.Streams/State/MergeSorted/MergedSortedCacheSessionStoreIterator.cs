
//    class MergedSortedCacheSessionStoreIterator : AbstractMergedSortedCacheStoreIterator<Windowed<Bytes>, Windowed<Bytes>, byte[], byte[]>
//    {

//        private SegmentedCacheFunction cacheFunction;

//        MergedSortedCacheSessionStoreIterator(IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
//                                              IKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator,
//                                              SegmentedCacheFunction cacheFunction)
//        {
//            base(cacheIterator, storeIterator);
//            this.cacheFunction = cacheFunction;
//        }

//        public override KeyValuePair<Windowed<Bytes>, byte[]> deserializeStorePair(KeyValuePair<Windowed<Bytes>, byte[]> pair)
//        {
//            return pair;
//        }


//        Windowed<Bytes> deserializeCacheKey(Bytes cacheKey)
//        {
//            byte[] binaryKey = cacheFunction.key(cacheKey)[];
//            byte[] keyBytes = SessionKeySchema.extractKeyBytes(binaryKey);
//            Window window = SessionKeySchema.extractWindow(binaryKey);
//            return new Windowed<>(Bytes.wrap(keyBytes), window);
//        }



//        byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
//        {
//            return cacheEntry.value();
//        }

//        public override Windowed<Bytes> deserializeStoreKey(Windowed<Bytes> key)
//        {
//            return key;
//        }

//        public override int compare(Bytes cacheKey, Windowed<Bytes> storeKey)
//        {
//            Bytes storeKeyBytes = SessionKeySchema.toBinary(storeKey);
//            return cacheFunction.compareSegmentedKeys(cacheKey, storeKeyBytes);
//        }
//    }
//}