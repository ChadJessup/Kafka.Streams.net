//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */




















//    public class FilteredCacheIteratorTest
//    {

//        private static CacheFunction IDENTITY_FUNCTION = new CacheFunction()
//        {


//        public Bytes Key(Bytes cacheKey)
//        {
//            return cacheKey;
//        }


//        public Bytes CacheKey(Bytes key)
//        {
//            return key;
//        }
//    };


//    private IKeyValueStore<Bytes, LRUCacheEntry> store = new GenericInMemoryKeyValueStore<>("my-store");
//    private KeyValuePair<Bytes, LRUCacheEntry> firstEntry = KeyValuePair.Create(Bytes.Wrap("a".getBytes()),
//                                                                            new LRUCacheEntry("1".getBytes()));
//    private List<KeyValuePair<Bytes, LRUCacheEntry>> entries = asList(
//            firstEntry,
//            KeyValuePair.Create(Bytes.Wrap("b".getBytes()),
//                          new LRUCacheEntry("2".getBytes())),
//            KeyValuePair.Create(Bytes.Wrap("c".getBytes()),
//                          new LRUCacheEntry("3".getBytes())));

//    private FilteredCacheIterator allIterator;
//    private FilteredCacheIterator firstEntryIterator;


//    public void Before()
//    {
//        store.putAll(entries);
//        HasNextCondition allCondition = new HasNextCondition()
//        {


//            public bool hasNext(IKeyValueIterator<Bytes, ?> iterator)
//        {
//            return iterator.HasNext();
//        }
//    };
//    allIterator = new FilteredCacheIterator(
//            new DelegatingPeekingKeyValueIterator<>("",
//                                                    store.all()), allCondition, IDENTITY_FUNCTION);

//        HasNextCondition firstEntryCondition = new HasNextCondition()
//        {


//            public bool HasNext(IKeyValueIterator<Bytes, ?> iterator)
//    {
//        return iterator.HasNext() && iterator.PeekNextKey().equals(firstEntry.key);
//    }
//        };
//        firstEntryIterator = new FilteredCacheIterator(
//                new DelegatingPeekingKeyValueIterator<>("",
//                                                        store.all()), firstEntryCondition, IDENTITY_FUNCTION);

//    }

//    [Fact]
//    public void ShouldAllowEntryMatchingHasNextCondition()
//    {
//        List<KeyValuePair<Bytes, LRUCacheEntry>> keyValues = toList(allIterator);
//        Assert.Equal(keyValues, (entries));
//    }

//    [Fact]
//    public void ShouldPeekNextKey()
//    {
//        while (allIterator.HasNext())
//        {
//            Bytes nextKey = allIterator.PeekNextKey();
//            KeyValuePair<Bytes, LRUCacheEntry> next = allIterator.MoveNext();
//            Assert.Equal(next.key, (nextKey));
//        }
//    }

//    [Fact]
//    public void ShouldPeekNext()
//    {
//        while (allIterator.HasNext())
//        {
//            KeyValuePair<Bytes, LRUCacheEntry> peeked = allIterator.PeekNext();
//            KeyValuePair<Bytes, LRUCacheEntry> next = allIterator.MoveNext();
//            Assert.Equal(peeked, (next));
//        }
//    }

//    [Fact]
//    public void ShouldNotHaveNextIfHasNextConditionNotMet()
//    {
//        Assert.True(firstEntryIterator.HasNext());
//        firstEntryIterator.MoveNext();
//        Assert.False(firstEntryIterator.HasNext());
//    }

//    [Fact]
//    public void ShouldFilterEntriesNotMatchingHasNextCondition()
//    {
//        List<KeyValuePair<Bytes, LRUCacheEntry>> keyValues = toList(firstEntryIterator);
//        Assert.Equal(keyValues, (asList(firstEntry)));
//    }

//    [Fact]// (expected = UnsupportedOperationException)
//    public void ShouldThrowUnsupportedOperationExeceptionOnRemove()
//    {
//        allIterator.remove();
//    }

//}
//}
///*






//*

//*





//*/

































