/*






 *

 *





 */




















public class FilteredCacheIteratorTest {

    private static CacheFunction IDENTITY_FUNCTION = new CacheFunction() {
        
        public Bytes Key(Bytes cacheKey) {
            return cacheKey;
        }

        
        public Bytes CacheKey(Bytes key) {
            return key;
        }
    };

    
    private KeyValueStore<Bytes, LRUCacheEntry> store = new GenericInMemoryKeyValueStore<>("my-store");
    private KeyValuePair<Bytes, LRUCacheEntry> firstEntry = KeyValuePair.Create(Bytes.wrap("a".getBytes()),
                                                                            new LRUCacheEntry("1".getBytes()));
    private List<KeyValuePair<Bytes, LRUCacheEntry>> entries = asList(
            firstEntry,
            KeyValuePair.Create(Bytes.wrap("b".getBytes()),
                          new LRUCacheEntry("2".getBytes())),
            KeyValuePair.Create(Bytes.wrap("c".getBytes()),
                          new LRUCacheEntry("3".getBytes())));

    private FilteredCacheIterator allIterator;
    private FilteredCacheIterator firstEntryIterator;

    
    public void Before() {
        store.putAll(entries);
        HasNextCondition allCondition = new HasNextCondition() {
            
            public bool hasNext(KeyValueIterator<Bytes, ?> iterator) {
                return iterator.hasNext();
            }
        };
        allIterator = new FilteredCacheIterator(
            new DelegatingPeekingKeyValueIterator<>("",
                                                    store.all()), allCondition, IDENTITY_FUNCTION);

        HasNextCondition firstEntryCondition = new HasNextCondition() {
            
            public bool HasNext(KeyValueIterator<Bytes, ?> iterator) {
                return iterator.hasNext() && iterator.peekNextKey().equals(firstEntry.key);
            }
        };
        firstEntryIterator = new FilteredCacheIterator(
                new DelegatingPeekingKeyValueIterator<>("",
                                                        store.all()), firstEntryCondition, IDENTITY_FUNCTION);

    }

    [Xunit.Fact]
    public void ShouldAllowEntryMatchingHasNextCondition() {
        List<KeyValuePair<Bytes, LRUCacheEntry>> keyValues = toList(allIterator);
        Assert.Equal(keyValues, (entries));
    }

    [Xunit.Fact]
    public void ShouldPeekNextKey() {
        while (allIterator.hasNext()) {
            Bytes nextKey = allIterator.peekNextKey();
            KeyValuePair<Bytes, LRUCacheEntry> next = allIterator.next();
            Assert.Equal(next.key, (nextKey));
        }
    }

    [Xunit.Fact]
    public void ShouldPeekNext() {
        while (allIterator.hasNext()) {
            KeyValuePair<Bytes, LRUCacheEntry> peeked = allIterator.peekNext();
            KeyValuePair<Bytes, LRUCacheEntry> next = allIterator.next();
            Assert.Equal(peeked, (next));
        }
    }

    [Xunit.Fact]
    public void ShouldNotHaveNextIfHasNextConditionNotMet() {
        Assert.True(firstEntryIterator.hasNext());
        firstEntryIterator.next();
        Assert.False(firstEntryIterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldFilterEntriesNotMatchingHasNextCondition() {
        List<KeyValuePair<Bytes, LRUCacheEntry>> keyValues = toList(firstEntryIterator);
        Assert.Equal(keyValues, (asList(firstEntry)));
    }

    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void ShouldThrowUnsupportedOperationExeceptionOnRemove() {
        allIterator.remove();
    }

}
