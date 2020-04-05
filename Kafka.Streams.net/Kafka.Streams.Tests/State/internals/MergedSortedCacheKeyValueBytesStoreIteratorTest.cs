///*






// *

// *





// */
















//public class MergedSortedCacheKeyValueBytesStoreIteratorTest {

//    private string namespace = "0.0-one";
//    private StateSerdes<byte[], byte[]> serdes =  new StateSerdes<>("dummy", Serdes.ByteArray(), Serdes.ByteArray());
//    private IKeyValueStore<Bytes, byte[]> store;
//    private ThreadCache cache;

    
//    public void SetUp() {// throws Exception
//        store = new InMemoryKeyValueStore(namespace);
//        cache = new ThreadCache(new LogContext("testCache "), 10000L, new MockStreamsMetrics(new Metrics()));
//    }

//    [Fact]
//    public void ShouldIterateOverRange() {// throws Exception
//        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
//        for (int i = 0; i < bytes.Length; i += 2) {
//            store.put(Bytes.Wrap(bytes[i]), bytes[i]);
//            cache.put(namespace, Bytes.Wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
//        }

//        Bytes from = Bytes.Wrap(new byte[]{2});
//        Bytes to = Bytes.Wrap(new byte[]{9});
//        IKeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", store.Range(from, to));
//        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.Range(namespace, from, to);

//        MergedSortedCacheKeyValueBytesStoreIterator iterator = new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
//        byte[][] values = new byte[8][];
//        int index = 0;
//        int bytesIndex = 2;
//        while (iterator.hasNext()) {
//            byte[] value = iterator.MoveNext().value;
//            values[index++] = value;
//            assertArrayEquals(bytes[bytesIndex++], value);
//        }
//        iterator.close();
//    }


//    [Fact]
//    public void ShouldSkipLargerDeletedCacheValue() {// throws Exception
//        byte[][] bytes = {{0}, {1}};
//        store.put(Bytes.Wrap(bytes[0]), bytes[0]);
//        cache.put(namespace, Bytes.Wrap(bytes[1]), new LRUCacheEntry(null));
//        MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
//        assertArrayEquals(bytes[0], iterator.MoveNext().key.Get());
//        Assert.False(iterator.hasNext());
//    }

//    [Fact]
//    public void ShouldSkipSmallerDeletedCachedValue() {// throws Exception
//        byte[][] bytes = {{0}, {1}};
//        cache.put(namespace, Bytes.Wrap(bytes[0]), new LRUCacheEntry(null));
//        store.put(Bytes.Wrap(bytes[1]), bytes[1]);
//        MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
//        assertArrayEquals(bytes[1], iterator.MoveNext().key.Get());
//        Assert.False(iterator.hasNext());
//    }

//    [Fact]
//    public void ShouldIgnoreIfDeletedInCacheButExistsInStore() {// throws Exception
//        byte[][] bytes = {{0}};
//        cache.put(namespace, Bytes.Wrap(bytes[0]), new LRUCacheEntry(null));
//        store.put(Bytes.Wrap(bytes[0]), bytes[0]);
//        MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
//        Assert.False(iterator.hasNext());
//    }

//    [Fact]
//    public void ShouldNotHaveNextIfAllCachedItemsDeleted() {// throws Exception
//        byte[][] bytes = {{0}, {1}, {2}};
//        foreach (byte[] aByte in bytes) {
//            Bytes aBytes = Bytes.Wrap(aByte);
//            store.put(aBytes, aByte);
//            cache.put(namespace, aBytes, new LRUCacheEntry(null));
//        }
//        Assert.False(createIterator().hasNext());
//    }

//    [Fact]
//    public void ShouldNotHaveNextIfOnlyCacheItemsAndAllDeleted() {// throws Exception
//        byte[][] bytes = {{0}, {1}, {2}};
//        foreach (byte[] aByte in bytes) {
//            cache.put(namespace, Bytes.Wrap(aByte), new LRUCacheEntry(null));
//        }
//        Assert.False(createIterator().hasNext());
//    }

//    [Fact]
//    public void ShouldSkipAllDeletedFromCache() {// throws Exception
//        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
//        foreach (byte[] aByte in bytes) {
//            Bytes aBytes = Bytes.Wrap(aByte);
//            store.put(aBytes, aByte);
//            cache.put(namespace, aBytes, new LRUCacheEntry(aByte));
//        }
//        cache.put(namespace, Bytes.Wrap(bytes[1]), new LRUCacheEntry(null));
//        cache.put(namespace, Bytes.Wrap(bytes[2]), new LRUCacheEntry(null));
//        cache.put(namespace, Bytes.Wrap(bytes[3]), new LRUCacheEntry(null));
//        cache.put(namespace, Bytes.Wrap(bytes[8]), new LRUCacheEntry(null));
//        cache.put(namespace, Bytes.Wrap(bytes[11]), new LRUCacheEntry(null));

//        MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
//        assertArrayEquals(bytes[0], iterator.MoveNext().key.Get());
//        assertArrayEquals(bytes[4], iterator.MoveNext().key.Get());
//        assertArrayEquals(bytes[5], iterator.MoveNext().key.Get());
//        assertArrayEquals(bytes[6], iterator.MoveNext().key.Get());
//        assertArrayEquals(bytes[7], iterator.MoveNext().key.Get());
//        assertArrayEquals(bytes[9], iterator.MoveNext().key.Get());
//        assertArrayEquals(bytes[10], iterator.MoveNext().key.Get());
//        Assert.False(iterator.hasNext());

//    }

//    [Fact]
//    public void ShouldPeekNextKey() {// throws Exception
//        IKeyValueStore<Bytes, byte[]> kv = new InMemoryKeyValueStore("one");
//        ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1000000L, new MockStreamsMetrics(new Metrics()));
//        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
//        for (int i = 0; i < bytes.Length - 1; i += 2) {
//            kv.put(Bytes.Wrap(bytes[i]), bytes[i]);
//            cache.put(namespace, Bytes.Wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
//        }

//        Bytes from = Bytes.Wrap(new byte[]{2});
//        Bytes to = Bytes.Wrap(new byte[]{9});
//        IKeyValueIterator<Bytes, byte[]> storeIterator = kv.Range(from, to);
//        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.Range(namespace, from, to);

//        MergedSortedCacheKeyValueBytesStoreIterator iterator =
//                new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator,
//                                                                storeIterator
//                );
//        byte[][] values = new byte[8][];
//        int index = 0;
//        int bytesIndex = 2;
//        while (iterator.hasNext()) {
//            byte[] keys = iterator.peekNextKey().Get();
//            values[index++] = keys;
//            assertArrayEquals(bytes[bytesIndex++], keys);
//            iterator.MoveNext();
//        }
//        iterator.close();
//    }

//    private MergedSortedCacheKeyValueBytesStoreIterator CreateIterator() {
//        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(namespace);
//        IKeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", store.all());
//        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
//    }
//}