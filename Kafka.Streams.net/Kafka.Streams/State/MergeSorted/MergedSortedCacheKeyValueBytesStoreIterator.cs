
//    public class MergedSortedCacheKeyValueBytesStoreIterator : AbstractMergedSortedCacheStoreIterator<Bytes, Bytes, byte[], byte[]>
//    {


//        MergedSortedCacheKeyValueBytesStoreIterator(IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
//                                                    IKeyValueIterator<Bytes, byte[]> storeIterator)
//        {
//            base(cacheIterator, storeIterator);
//        }

//        public override KeyValuePair<Bytes, byte[]> deserializeStorePair(KeyValuePair<Bytes, byte[]> pair)
//        {
//            return pair;
//        }


//        Bytes deserializeCacheKey(Bytes cacheKey)
//        {
//            return cacheKey;
//        }


//        byte[] deserializeCacheValue(LRUCacheEntry cacheEntry)
//        {
//            return cacheEntry.value();
//        }

//        public override Bytes deserializeStoreKey(Bytes key)
//        {
//            return key;
//        }

//        public override int compare(Bytes cacheKey, Bytes storeKey)
//        {
//            return cacheKey.CompareTo(storeKey);
//        }
//    }
//}