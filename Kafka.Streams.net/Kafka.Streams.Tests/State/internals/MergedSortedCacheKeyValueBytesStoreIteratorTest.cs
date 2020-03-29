/*






 *

 *





 */
















public class MergedSortedCacheKeyValueBytesStoreIteratorTest {

    private string namespace = "0.0-one";
    private StateSerdes<byte[], byte[]> serdes =  new StateSerdes<>("dummy", Serdes.ByteArray(), Serdes.ByteArray());
    private KeyValueStore<Bytes, byte[]> store;
    private ThreadCache cache;

    
    public void setUp() {// throws Exception
        store = new InMemoryKeyValueStore(namespace);
        cache = new ThreadCache(new LogContext("testCache "), 10000L, new MockStreamsMetrics(new Metrics()));
    }

    [Xunit.Fact]
    public void shouldIterateOverRange() {// throws Exception
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
        for (int i = 0; i < bytes.Length; i += 2) {
            store.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, Bytes.wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
        }

        Bytes from = Bytes.wrap(new byte[]{2});
        Bytes to = Bytes.wrap(new byte[]{9});
        KeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", store.range(from, to));
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, from, to);

        MergedSortedCacheKeyValueBytesStoreIterator iterator = new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
        byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 2;
        while (iterator.hasNext()) {
            byte[] value = iterator.next().value;
            values[index++] = value;
            assertArrayEquals(bytes[bytesIndex++], value);
        }
        iterator.close();
    }


    [Xunit.Fact]
    public void shouldSkipLargerDeletedCacheValue() {// throws Exception
        byte[][] bytes = {{0}, {1}};
        store.put(Bytes.wrap(bytes[0]), bytes[0]);
        cache.put(namespace, Bytes.wrap(bytes[1]), new LRUCacheEntry(null));
        MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
        assertArrayEquals(bytes[0], iterator.next().key.get());
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void shouldSkipSmallerDeletedCachedValue() {// throws Exception
        byte[][] bytes = {{0}, {1}};
        cache.put(namespace, Bytes.wrap(bytes[0]), new LRUCacheEntry(null));
        store.put(Bytes.wrap(bytes[1]), bytes[1]);
        MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
        assertArrayEquals(bytes[1], iterator.next().key.get());
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void shouldIgnoreIfDeletedInCacheButExistsInStore() {// throws Exception
        byte[][] bytes = {{0}};
        cache.put(namespace, Bytes.wrap(bytes[0]), new LRUCacheEntry(null));
        store.put(Bytes.wrap(bytes[0]), bytes[0]);
        MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void shouldNotHaveNextIfAllCachedItemsDeleted() {// throws Exception
        byte[][] bytes = {{0}, {1}, {2}};
        foreach (byte[] aByte in bytes) {
            Bytes aBytes = Bytes.wrap(aByte);
            store.put(aBytes, aByte);
            cache.put(namespace, aBytes, new LRUCacheEntry(null));
        }
        Assert.False(createIterator().hasNext());
    }

    [Xunit.Fact]
    public void shouldNotHaveNextIfOnlyCacheItemsAndAllDeleted() {// throws Exception
        byte[][] bytes = {{0}, {1}, {2}};
        foreach (byte[] aByte in bytes) {
            cache.put(namespace, Bytes.wrap(aByte), new LRUCacheEntry(null));
        }
        Assert.False(createIterator().hasNext());
    }

    [Xunit.Fact]
    public void shouldSkipAllDeletedFromCache() {// throws Exception
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}};
        foreach (byte[] aByte in bytes) {
            Bytes aBytes = Bytes.wrap(aByte);
            store.put(aBytes, aByte);
            cache.put(namespace, aBytes, new LRUCacheEntry(aByte));
        }
        cache.put(namespace, Bytes.wrap(bytes[1]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[2]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[3]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[8]), new LRUCacheEntry(null));
        cache.put(namespace, Bytes.wrap(bytes[11]), new LRUCacheEntry(null));

        MergedSortedCacheKeyValueBytesStoreIterator iterator = createIterator();
        assertArrayEquals(bytes[0], iterator.next().key.get());
        assertArrayEquals(bytes[4], iterator.next().key.get());
        assertArrayEquals(bytes[5], iterator.next().key.get());
        assertArrayEquals(bytes[6], iterator.next().key.get());
        assertArrayEquals(bytes[7], iterator.next().key.get());
        assertArrayEquals(bytes[9], iterator.next().key.get());
        assertArrayEquals(bytes[10], iterator.next().key.get());
        Assert.False(iterator.hasNext());

    }

    [Xunit.Fact]
    public void shouldPeekNextKey() {// throws Exception
        KeyValueStore<Bytes, byte[]> kv = new InMemoryKeyValueStore("one");
        ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1000000L, new MockStreamsMetrics(new Metrics()));
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
        for (int i = 0; i < bytes.Length - 1; i += 2) {
            kv.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, Bytes.wrap(bytes[i + 1]), new LRUCacheEntry(bytes[i + 1]));
        }

        Bytes from = Bytes.wrap(new byte[]{2});
        Bytes to = Bytes.wrap(new byte[]{9});
        KeyValueIterator<Bytes, byte[]> storeIterator = kv.range(from, to);
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, from, to);

        MergedSortedCacheKeyValueBytesStoreIterator iterator =
                new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator,
                                                                storeIterator
                );
        byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 2;
        while (iterator.hasNext()) {
            byte[] keys = iterator.peekNextKey().get();
            values[index++] = keys;
            assertArrayEquals(bytes[bytesIndex++], keys);
            iterator.next();
        }
        iterator.close();
    }

    private MergedSortedCacheKeyValueBytesStoreIterator createIterator() {
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(namespace);
        KeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", store.all());
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
    }
}