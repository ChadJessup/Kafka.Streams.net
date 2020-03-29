/*






 *

 *





 */





















public class MergedSortedCacheWrappedWindowStoreIteratorTest {

    private static SegmentedCacheFunction SINGLE_SEGMENT_CACHE_FUNCTION = new SegmentedCacheFunction(null, -1) {
        
        public long segmentId(Bytes key) {
            return 0;
        }
    };

    private List<KeyValuePair<long, byte[]>> windowStoreKvPairs = new ArrayList<>();
    private ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1000000L,  new MockStreamsMetrics(new Metrics()));
    private string namespace = "0.0-one";
    private StateSerdes<string, string> stateSerdes = new StateSerdes<>("foo", Serdes.String(), Serdes.String());

    [Xunit.Fact]
    public void shouldIterateOverValueFromBothIterators() {
        List<KeyValuePair<long, byte[]>> expectedKvPairs = new ArrayList<>();
        for (long t = 0; t < 100; t += 20) {
            byte[] v1Bytes = string.valueOf(t).getBytes();
            KeyValuePair<long, byte[]> v1 = KeyValuePair.Create(t, v1Bytes);
            windowStoreKvPairs.add(v1);
            expectedKvPairs.add(KeyValuePair.Create(t, v1Bytes));
            Bytes keyBytes = WindowKeySchema.toStoreKeyBinary("a", t + 10, 0, stateSerdes);
            byte[] valBytes = string.valueOf(t + 10).getBytes();
            expectedKvPairs.add(KeyValuePair.Create(t + 10, valBytes));
            cache.put(namespace, SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(keyBytes), new LRUCacheEntry(valBytes));
        }

        Bytes fromBytes = WindowKeySchema.toStoreKeyBinary("a", 0, 0, stateSerdes);
        Bytes toBytes = WindowKeySchema.toStoreKeyBinary("a", 100, 0, stateSerdes);
        KeyValueIterator<long, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(windowStoreKvPairs.iterator()));

        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(
            namespace, SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(fromBytes), SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(toBytes)
        );

        MergedSortedCacheWindowStoreIterator iterator = new MergedSortedCacheWindowStoreIterator(
            cacheIterator, storeIterator
        );
        int index = 0;
        while (iterator.hasNext()) {
            KeyValuePair<long, byte[]> next = iterator.next();
            KeyValuePair<long, byte[]> expected = expectedKvPairs.get(index++);
            assertArrayEquals(expected.value, next.value);
            Assert.Equal(expected.key, next.key);
        }
        iterator.close();
    }

    [Xunit.Fact]
    public void shouldPeekNextStoreKey() {
        windowStoreKvPairs.add(KeyValuePair.Create(10L, "a".getBytes()));
        cache.put(namespace, SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(WindowKeySchema.toStoreKeyBinary("a", 0, 0, stateSerdes)), new LRUCacheEntry("b".getBytes()));
        Bytes fromBytes = WindowKeySchema.toStoreKeyBinary("a", 0, 0, stateSerdes);
        Bytes toBytes = WindowKeySchema.toStoreKeyBinary("a", 100, 0, stateSerdes);
        KeyValueIterator<long, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(windowStoreKvPairs.iterator()));
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(
            namespace, SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(fromBytes), SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(toBytes)
        );
        MergedSortedCacheWindowStoreIterator iterator = new MergedSortedCacheWindowStoreIterator(
            cacheIterator, storeIterator
        );
        Assert.Equal(iterator.peekNextKey(), (0L));
        iterator.next();
        Assert.Equal(iterator.peekNextKey(), (10L));
        iterator.close();
    }

    [Xunit.Fact]
    public void shouldPeekNextCacheKey() {
        windowStoreKvPairs.add(KeyValuePair.Create(0L, "a".getBytes()));
        cache.put(namespace, SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(WindowKeySchema.toStoreKeyBinary("a", 10L, 0, stateSerdes)), new LRUCacheEntry("b".getBytes()));
        Bytes fromBytes = WindowKeySchema.toStoreKeyBinary("a", 0, 0, stateSerdes);
        Bytes toBytes = WindowKeySchema.toStoreKeyBinary("a", 100, 0, stateSerdes);
        KeyValueIterator<long, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(windowStoreKvPairs.iterator()));
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(fromBytes), SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(toBytes));
        MergedSortedCacheWindowStoreIterator iterator = new MergedSortedCacheWindowStoreIterator(cacheIterator, storeIterator);
        Assert.Equal(iterator.peekNextKey(), (0L));
        iterator.next();
        Assert.Equal(iterator.peekNextKey(), (10L));
        iterator.close();
    }
}
