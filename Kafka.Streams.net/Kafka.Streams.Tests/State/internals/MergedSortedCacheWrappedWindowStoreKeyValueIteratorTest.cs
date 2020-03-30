namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */





















    public class MergedSortedCacheWrappedWindowStoreKeyValueIteratorTest
    {
        private static SegmentedCacheFunction SINGLE_SEGMENT_CACHE_FUNCTION = new SegmentedCacheFunction(null, -1)
        {


        public long SegmentId(Bytes key)
        {
            return 0;
        }
    };
    private static int WINDOW_SIZE = 10;

    private string storeKey = "a";
    private string cacheKey = "b";

    private TimeWindow storeWindow = new TimeWindow(0, 1);
    private Iterator<KeyValuePair<Windowed<Bytes>, byte[]>> storeKvs = Collections.singleton(
        KeyValuePair.Create(new Windowed<>(Bytes.wrap(storeKey.getBytes()), storeWindow), storeKey.getBytes())).iterator();
    private TimeWindow cacheWindow = new TimeWindow(10, 20);
    private Iterator<KeyValuePair<Bytes, LRUCacheEntry>> cacheKvs = Collections.singleton(
        KeyValuePair.Create(
            SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(WindowKeySchema.toStoreKeyBinary(
                    new Windowed<>(cacheKey, cacheWindow), 0, new StateSerdes<>("dummy", Serdes.String(), Serdes.ByteArray()))
            ),
            new LRUCacheEntry(cacheKey.getBytes())
        )).iterator();
    private Deserializer<string> deserializer = Serdes.String().deserializer();

    [Xunit.Fact]
    public void ShouldHaveNextFromStore()
    {
        MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(storeKvs, Collections.emptyIterator());
        Assert.True(mergeIterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldGetNextFromStore()
    {
        MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(storeKvs, Collections.emptyIterator());
        Assert.Equal(convertKeyValuePair(mergeIterator.next()), (KeyValuePair.Create(new Windowed<>(storeKey, storeWindow), storeKey)));
    }

    [Xunit.Fact]
    public void ShouldPeekNextKeyFromStore()
    {
        MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(storeKvs, Collections.emptyIterator());
        Assert.Equal(convertWindowedKey(mergeIterator.peekNextKey()), (new Windowed<>(storeKey, storeWindow)));
    }

    [Xunit.Fact]
    public void ShouldHaveNextFromCache()
    {
        MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(Collections.emptyIterator(), cacheKvs);
        Assert.True(mergeIterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldGetNextFromCache()
    {
        MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(Collections.emptyIterator(), cacheKvs);
        Assert.Equal(convertKeyValuePair(mergeIterator.next()), (KeyValuePair.Create(new Windowed<>(cacheKey, cacheWindow), cacheKey)));
    }

    [Xunit.Fact]
    public void ShouldPeekNextKeyFromCache()
    {
        MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(Collections.emptyIterator(), cacheKvs);
        Assert.Equal(convertWindowedKey(mergeIterator.peekNextKey()), (new Windowed<>(cacheKey, cacheWindow)));
    }

    [Xunit.Fact]
    public void ShouldIterateBothStoreAndCache()
    {
        MergedSortedCacheWindowStoreKeyValueIterator iterator = createIterator(storeKvs, cacheKvs);
        Assert.Equal(convertKeyValuePair(iterator.next()), (KeyValuePair.Create(new Windowed<>(storeKey, storeWindow), storeKey)));
        Assert.Equal(convertKeyValuePair(iterator.next()), (KeyValuePair.Create(new Windowed<>(cacheKey, cacheWindow), cacheKey)));
        Assert.False(iterator.hasNext());
    }

    private KeyValuePair<Windowed<string>, string> ConvertKeyValuePair(KeyValuePair<Windowed<Bytes>, byte[]> next)
    {
        string value = deserializer.deserialize("", next.value);
        return KeyValuePair.Create(convertWindowedKey(next.key), value);
    }

    private Windowed<string> ConvertWindowedKey(Windowed<Bytes> bytesWindowed)
    {
        string key = deserializer.deserialize("", bytesWindowed.Key.get());
        return new Windowed<>(key, bytesWindowed.window());
    }


    private MergedSortedCacheWindowStoreKeyValueIterator CreateIterator(
        Iterator<KeyValuePair<Windowed<Bytes>, byte[]>> storeKvs,
        Iterator<KeyValuePair<Bytes, LRUCacheEntry>> cacheKvs
    )
    {
        DelegatingPeekingKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator =
            new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(storeKvs));

        PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator =
            new DelegatingPeekingKeyValueIterator<>("cache", new KeyValueIteratorStub<>(cacheKvs));
        return new MergedSortedCacheWindowStoreKeyValueIterator(
            cacheIterator,
            storeIterator,
            new StateSerdes<>("name", Serdes.Bytes(), Serdes.ByteArray()),
            WINDOW_SIZE,
            SINGLE_SEGMENT_CACHE_FUNCTION
        );
    }
}
}
/*






*

*





*/


































