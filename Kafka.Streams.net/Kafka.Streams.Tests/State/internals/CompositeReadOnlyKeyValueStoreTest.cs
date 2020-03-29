/*






 *

 *





 */





























public class CompositeReadOnlyKeyValueStoreTest {

    private string storeName = "my-store";
    private string storeNameA = "my-storeA";
    private StateStoreProviderStub stubProviderTwo;
    private KeyValueStore<string, string> stubOneUnderlying;
    private KeyValueStore<string, string> otherUnderlyingStore;
    private CompositeReadOnlyKeyValueStore<string, string> theStore;

    
    public void before() {
        StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
        stubProviderTwo = new StateStoreProviderStub(false);

        stubOneUnderlying = newStoreInstance();
        stubProviderOne.addStore(storeName, stubOneUnderlying);
        otherUnderlyingStore = newStoreInstance();
        stubProviderOne.addStore("other-store", otherUnderlyingStore);

        theStore = new CompositeReadOnlyKeyValueStore<>(
            new WrappingStoreProvider(Array.<StateStoreProvider>asList(stubProviderOne, stubProviderTwo)),
                                        QueryableStoreTypes.<string, string>keyValueStore(),
                                        storeName);
    }

    private KeyValueStore<string, string> newStoreInstance() {
        KeyValueStore<string, string> store = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName),
                Serdes.String(),
                Serdes.String())
                .build();

        store.init(new InternalMockProcessorContext(new StateSerdes<>(ProcessorStateManager.storeChangelogTopic("appId", storeName), Serdes.String(), Serdes.String()),
                                                    new NoOpRecordCollector()),
                store);

        return store;
    }

    [Xunit.Fact]
    public void shouldReturnNullIfKeyDoesntExist() {
        assertNull(theStore.get("whatever"));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        theStore.get(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        theStore.range(null, "to");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        theStore.range("from", null);
    }

    [Xunit.Fact]
    public void shouldReturnValueIfExists() {
        stubOneUnderlying.put("key", "value");
        Assert.Equal("value", theStore.get("key"));
    }

    [Xunit.Fact]
    public void shouldNotGetValuesFromOtherStores() {
        otherUnderlyingStore.put("otherKey", "otherValue");
        assertNull(theStore.get("otherKey"));
    }

    [Xunit.Fact]
    public void shouldThrowNoSuchElementExceptionWhileNext() {
        stubOneUnderlying.put("a", "1");
        KeyValueIterator<string, string> keyValueIterator = theStore.range("a", "b");
        keyValueIterator.next();
        try {
            keyValueIterator.next();
            Assert.True(false, "Should have thrown NoSuchElementException with next()");
        } catch (NoSuchElementException e) { }
    }

    [Xunit.Fact]
    public void shouldThrowNoSuchElementExceptionWhilePeekNext() {
        stubOneUnderlying.put("a", "1");
        KeyValueIterator<string, string> keyValueIterator = theStore.range("a", "b");
        keyValueIterator.next();
        try {
            keyValueIterator.peekNextKey();
            Assert.True(false, "Should have thrown NoSuchElementException with peekNextKey()");
        } catch (NoSuchElementException e) { }
    }

    [Xunit.Fact]
    public void shouldThrowUnsupportedOperationExceptionWhileRemove() {
        KeyValueIterator<string, string> keyValueIterator = theStore.all();
        try {
            keyValueIterator.remove();
            Assert.True(false, "Should have thrown UnsupportedOperationException");
        } catch (UnsupportedOperationException e) { }
    }

    [Xunit.Fact]
    public void shouldThrowUnsupportedOperationExceptionWhileRange() {
        stubOneUnderlying.put("a", "1");
        stubOneUnderlying.put("b", "1");
        KeyValueIterator<string, string> keyValueIterator = theStore.range("a", "b");
        try {
            keyValueIterator.remove();
            Assert.True(false, "Should have thrown UnsupportedOperationException");
        } catch (UnsupportedOperationException e) { }
    }

    [Xunit.Fact]
    public void shouldFindValueForKeyWhenMultiStores() {
        KeyValueStore<string, string> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        cache.put("key-two", "key-two-value");
        stubOneUnderlying.put("key-one", "key-one-value");

        Assert.Equal("key-two-value", theStore.get("key-two"));
        Assert.Equal("key-one-value", theStore.get("key-one"));
    }

    [Xunit.Fact]
    public void shouldSupportRange() {
        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("c", "c");

        List<KeyValuePair<string, string>> results = toList(theStore.range("a", "b"));
        Assert.True(results.Contains(new KeyValuePair<>("a", "a")));
        Assert.True(results.Contains(new KeyValuePair<>("b", "b")));
        Assert.Equal(2, results.Count);
    }

    [Xunit.Fact]
    public void shouldSupportRangeAcrossMultipleKVStores() {
        KeyValueStore<string, string> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        List<KeyValuePair<string, string>> results = toList(theStore.range("a", "e"));
        Assert.True(results.Contains(new KeyValuePair<>("a", "a")));
        Assert.True(results.Contains(new KeyValuePair<>("b", "b")));
        Assert.True(results.Contains(new KeyValuePair<>("c", "c")));
        Assert.True(results.Contains(new KeyValuePair<>("d", "d")));
        Assert.Equal(4, results.Count);
    }

    [Xunit.Fact]
    public void shouldSupportAllAcrossMultipleStores() {
        KeyValueStore<string, string> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        List<KeyValuePair<string, string>> results = toList(theStore.all());
        Assert.True(results.Contains(new KeyValuePair<>("a", "a")));
        Assert.True(results.Contains(new KeyValuePair<>("b", "b")));
        Assert.True(results.Contains(new KeyValuePair<>("c", "c")));
        Assert.True(results.Contains(new KeyValuePair<>("d", "d")));
        Assert.True(results.Contains(new KeyValuePair<>("x", "x")));
        Assert.True(results.Contains(new KeyValuePair<>("z", "z")));
        Assert.Equal(6, results.Count);
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowInvalidStoreExceptionDuringRebalance() {
        rebalancing().get("anything");
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowInvalidStoreExceptionOnApproximateNumEntriesDuringRebalance() {
        rebalancing().approximateNumEntries();
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowInvalidStoreExceptionOnRangeDuringRebalance() {
        rebalancing().range("anything", "something");
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowInvalidStoreExceptionOnAllDuringRebalance() {
        rebalancing().all();
    }

    [Xunit.Fact]
    public void shouldGetApproximateEntriesAcrossAllStores() {
        KeyValueStore<string, string> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        Assert.Equal(6, theStore.approximateNumEntries());
    }

    [Xunit.Fact]
    public void shouldReturnLongMaxValueOnOverflow() {
        stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<object, object>() {
            
            public long approximateNumEntries() {
                return long.MaxValue;
            }
        });

        stubOneUnderlying.put("overflow", "me");
        Assert.Equal(long.MaxValue, theStore.approximateNumEntries());
    }

    [Xunit.Fact]
    public void shouldReturnLongMaxValueOnUnderflow() {
        stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<object, object>() {
            
            public long approximateNumEntries() {
                return long.MaxValue;
            }
        });
        stubProviderTwo.addStore(storeNameA, new NoOpReadOnlyStore<object, object>() {
            
            public long approximateNumEntries() {
                return long.MaxValue;
            }
        });

        Assert.Equal(long.MaxValue, theStore.approximateNumEntries());
    }

    private CompositeReadOnlyKeyValueStore<object, object> rebalancing() {
        return new CompositeReadOnlyKeyValueStore<>(new WrappingStoreProvider(Collections.<StateStoreProvider>singletonList(new StateStoreProviderStub(true))),
                QueryableStoreTypes.keyValueStore(), storeName);
    }

}