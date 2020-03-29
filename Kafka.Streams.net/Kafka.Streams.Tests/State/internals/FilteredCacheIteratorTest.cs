/*






 *

 *





 */




















public class FilteredCacheIteratorTest {

    private static CacheFunction IDENTITY_FUNCTION = new CacheFunction() {
        
        public Bytes key(Bytes cacheKey) {
            return cacheKey;
        }

        
        public Bytes cacheKey(Bytes key) {
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

    
    public void before() {
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
            
            public bool hasNext(KeyValueIterator<Bytes, ?> iterator) {
                return iterator.hasNext() && iterator.peekNextKey().equals(firstEntry.key);
            }
        };
        firstEntryIterator = new FilteredCacheIterator(
                new DelegatingPeekingKeyValueIterator<>("",
                                                        store.all()), firstEntryCondition, IDENTITY_FUNCTION);

    }

    [Xunit.Fact]
    public void shouldAllowEntryMatchingHasNextCondition() {
        List<KeyValuePair<Bytes, LRUCacheEntry>> keyValues = toList(allIterator);
        Assert.Equal(keyValues, (entries));
    }

    [Xunit.Fact]
    public void shouldPeekNextKey() {
        while (allIterator.hasNext()) {
            Bytes nextKey = allIterator.peekNextKey();
            KeyValuePair<Bytes, LRUCacheEntry> next = allIterator.next();
            Assert.Equal(next.key, (nextKey));
        }
    }

    [Xunit.Fact]
    public void shouldPeekNext() {
        while (allIterator.hasNext()) {
            KeyValuePair<Bytes, LRUCacheEntry> peeked = allIterator.peekNext();
            KeyValuePair<Bytes, LRUCacheEntry> next = allIterator.next();
            Assert.Equal(peeked, (next));
        }
    }

    [Xunit.Fact]
    public void shouldNotHaveNextIfHasNextConditionNotMet() {
        Assert.True(firstEntryIterator.hasNext());
        firstEntryIterator.next();
        Assert.False(firstEntryIterator.hasNext());
    }

    [Xunit.Fact]
    public void shouldFilterEntriesNotMatchingHasNextCondition() {
        List<KeyValuePair<Bytes, LRUCacheEntry>> keyValues = toList(firstEntryIterator);
        Assert.Equal(keyValues, (asList(firstEntry)));
    }

    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void shouldThrowUnsupportedOperationExeceptionOnRemove() {
        allIterator.remove();
    }

}
