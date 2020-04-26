//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

















//    public class MergedSortedCacheWrappedSessionStoreIteratorTest
//    {

//        private static SegmentedCacheFunction SINGLE_SEGMENT_CACHE_FUNCTION = new SegmentedCacheFunction(null, -1)
//        {


//        public long SegmentId(Bytes key)
//        {
//            return 0;
//        }
//    };

//    private Bytes storeKey = Bytes.Wrap("a".getBytes());
//    private Bytes cacheKey = Bytes.Wrap("b".getBytes());

//    private SessionWindow storeWindow = new SessionWindow(0, 1);
//    private Iterator<KeyValuePair<IWindowed<Bytes>, byte[]>> storeKvs = Collections.singleton(
//            KeyValuePair.Create(new Windowed<>(storeKey, storeWindow), storeKey.Get())).iterator();
//    private SessionWindow cacheWindow = new SessionWindow(10, 20);
//    private Iterator<KeyValuePair<Bytes, LRUCacheEntry>> cacheKvs = Collections.singleton(
//        KeyValuePair.Create(
//            SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(SessionKeySchema.toBinary(new Windowed<>(cacheKey, cacheWindow))),
//            new LRUCacheEntry(cacheKey.Get())
//        )).iterator();

//    [Fact]
//    public void ShouldHaveNextFromStore()
//    {
//        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator());
//        Assert.True(mergeIterator.MoveNext());
//    }

//    [Fact]
//    public void ShouldGetNextFromStore()
//    {
//        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator());
//        Assert.Equal(mergeIterator.MoveNext(), (KeyValuePair.Create(new Windowed<>(storeKey, storeWindow), storeKey.Get())));
//    }

//    [Fact]
//    public void ShouldPeekNextKeyFromStore()
//    {
//        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(storeKvs, Collections.emptyIterator());
//        Assert.Equal(mergeIterator.PeekNextKey(), (new Windowed<>(storeKey, storeWindow)));
//    }

//    [Fact]
//    public void ShouldHaveNextFromCache()
//    {
//        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs);
//        Assert.True(mergeIterator.MoveNext());
//    }

//    [Fact]
//    public void ShouldGetNextFromCache()
//    {
//        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs);
//        Assert.Equal(mergeIterator.MoveNext(), (KeyValuePair.Create(new Windowed<>(cacheKey, cacheWindow), cacheKey.Get())));
//    }

//    [Fact]
//    public void ShouldPeekNextKeyFromCache()
//    {
//        MergedSortedCacheSessionStoreIterator mergeIterator = createIterator(Collections.emptyIterator(), cacheKvs);
//        Assert.Equal(mergeIterator.PeekNextKey(), (new Windowed<>(cacheKey, cacheWindow)));
//    }

//    [Fact]
//    public void ShouldIterateBothStoreAndCache()
//    {
//        MergedSortedCacheSessionStoreIterator iterator = createIterator(storeKvs, cacheKvs);
//        Assert.Equal(iterator.MoveNext(), (KeyValuePair.Create(new Windowed<>(storeKey, storeWindow), storeKey.Get())));
//        Assert.Equal(iterator.MoveNext(), (KeyValuePair.Create(new Windowed<>(cacheKey, cacheWindow), cacheKey.Get())));
//        Assert.False(iterator.MoveNext());
//    }

//    private MergedSortedCacheSessionStoreIterator CreateIterator(Iterator<KeyValuePair<IWindowed<Bytes>, byte[]>> storeKvs,
//                                                                 Iterator<KeyValuePair<Bytes, LRUCacheEntry>> cacheKvs)
//    {
//        DelegatingPeekingKeyValueIterator<IWindowed<Bytes>, byte[]> storeIterator =
//            new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(storeKvs));

//        PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator =
//            new DelegatingPeekingKeyValueIterator<>("cache", new KeyValueIteratorStub<>(cacheKvs));
//        return new MergedSortedCacheSessionStoreIterator(cacheIterator, storeIterator, SINGLE_SEGMENT_CACHE_FUNCTION);
//    }

//}
//}
///*






//*

//*





//*/



























