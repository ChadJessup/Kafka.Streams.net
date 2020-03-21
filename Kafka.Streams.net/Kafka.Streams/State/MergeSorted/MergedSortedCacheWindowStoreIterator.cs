
//    public class MergedSortedCacheWindowStoreIterator : AbstractMergedSortedCacheStoreIterator<long, long, byte[], byte[]>
//        , IWindowStoreIterator<byte[]>
//    {
//        public MergedSortedCacheWindowStoreIterator(IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
//                                             IKeyValueIterator<long, byte[]> storeIterator)
//            : base(cacheIterator, storeIterator)
//        {
//        }

//        public KeyValuePair<long, byte[]> deserializeStorePair(KeyValuePair<long, byte[]> pair)
//        {
//            return pair;
//        }


//        long deserializeCacheKey(Bytes cacheKey)
//        {
//            byte[] binaryKey = bytesFromCacheKey(cacheKey);
//            return WindowKeySchema.extractStoreTimestamp(binaryKey);
//        }


//        byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
//        {
//            return cacheEntry.value();
//        }

//        public long deserializeStoreKey(long key)
//        {
//            return key;
//        }

//        public int compare(Bytes cacheKey, long storeKey)
//        {
//            byte[] binaryKey = bytesFromCacheKey(cacheKey);

//            long cacheTimestamp = WindowKeySchema.extractStoreTimestamp(binaryKey);
//            return cacheTimestamp.CompareTo(storeKey);
//        }
//    }
//}