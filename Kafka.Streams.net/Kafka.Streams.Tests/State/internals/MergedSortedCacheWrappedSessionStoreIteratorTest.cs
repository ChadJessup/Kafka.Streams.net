/*






 *

 *





 */

















public class MergedSortedCacheWrappedSessionStoreIteratorTest {

    private static SegmentedCacheFunction SINGLE_SEGMENT_CACHE_FUNCTION = new SegmentedCacheFunction(null, -1) {
        
        public long segmentId(Bytes key) {
            return 0;
        }
    };

    private Bytes storeKey = Bytes.wrap("a".getBytes());
    private Bytes cacheKey = Bytes.wrap("b".getBytes());

    private SessionWindow storeWindow = new SessionWindow(0, 1);
    private Iterator<KeyValuePair<Windowed<Bytes>, byte[]>> storeKvs = Collections.singleton(
            KeyValuePair.Create(new Windowed<>(storeKey, storeWindow), storeKey.get())).iterator();
    private SessionWindow cacheWindow = new SessionWindow(10, 20);
    private Iterator<KeyValuePair<Bytes, LRUCacheEntry>> cacheKvs = Collections.singleton(
        KeyValuePair.Create(
            SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(SessionKeySchema.toBinary(new Windowed<>(cacheKey, cacheWindow))),
            new LRUCacheEntry(cacheKey.get())
        )).iterator();

    [Xunit.Fact]
    public void shouldHaveNextFromStore() {
        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator());
        Assert.True(mergeIterator.hasNext());
    }

    [Xunit.Fact]
    public void shouldGetNextFromStore() {
        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator());
        Assert.Equal(mergeIterator.next(), (KeyValuePair.Create(new Windowed<>(storeKey, storeWindow), storeKey.get())));
    }

    [Xunit.Fact]
    public void shouldPeekNextKeyFromStore() {
        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator());
        Assert.Equal(mergeIterator.peekNextKey(), (new Windowed<>(storeKey, storeWindow)));
    }

    [Xunit.Fact]
    public void shouldHaveNextFromCache() {
        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs);
        Assert.True(mergeIterator.hasNext());
    }

    [Xunit.Fact]
    public void shouldGetNextFromCache() {
        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs);
        Assert.Equal(mergeIterator.next(), (KeyValuePair.Create(new Windowed<>(cacheKey, cacheWindow), cacheKey.get())));
    }

    [Xunit.Fact]
    public void shouldPeekNextKeyFromCache() {
        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs);
        Assert.Equal(mergeIterator.peekNextKey(), (new Windowed<>(cacheKey, cacheWindow)));
    }

    [Xunit.Fact]
    public void shouldIterateBothStoreAndCache() {
        MergedSortedCacheSessionStoreIterator iterator = createIterator(storeKvs, cacheKvs);
        Assert.Equal(iterator.next(), (KeyValuePair.Create(new Windowed<>(storeKey, storeWindow), storeKey.get())));
        Assert.Equal(iterator.next(), (KeyValuePair.Create(new Windowed<>(cacheKey, cacheWindow), cacheKey.get())));
        Assert.False(iterator.hasNext());
    }

    private MergedSortedCacheSessionStoreIterator createIterator(Iterator<KeyValuePair<Windowed<Bytes>, byte[]>> storeKvs,
                                                                 Iterator<KeyValuePair<Bytes, LRUCacheEntry>> cacheKvs) {
        DelegatingPeekingKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator =
            new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(storeKvs));

        PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator =
            new DelegatingPeekingKeyValueIterator<>("cache", new KeyValueIteratorStub<>(cacheKvs));
        return new MergedSortedCacheSessionStoreIterator(cacheIterator, storeIterator, SINGLE_SEGMENT_CACHE_FUNCTION);
    }

}
