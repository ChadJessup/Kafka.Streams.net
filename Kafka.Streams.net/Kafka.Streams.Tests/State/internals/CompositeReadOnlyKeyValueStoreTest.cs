/*






 *

 *





 */





























public class CompositeReadOnlyKeyValueStoreTest {

    private readonly string storeName = "my-store";
    private readonly string storeNameA = "my-storeA";
    private StateStoreProviderStub stubProviderTwo;
    private KeyValueStore<string, string> stubOneUnderlying;
    private KeyValueStore<string, string> otherUnderlyingStore;
    private CompositeReadOnlyKeyValueStore<string, string> theStore;

    
    public void Before() {
        StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
        stubProviderTwo = new StateStoreProviderStub(false);

        stubOneUnderlying = NewStoreInstance();
        stubProviderOne.addStore(storeName, stubOneUnderlying);
        otherUnderlyingStore = NewStoreInstance();
        stubProviderOne.addStore("other-store", otherUnderlyingStore);

        theStore = new CompositeReadOnlyKeyValueStore<>(
            new WrappingStoreProvider(Array.<StateStoreProvider>asList(stubProviderOne, stubProviderTwo)),
                                        QueryableStoreTypes.<string, string>keyValueStore(),
                                        storeName);
    }

    private KeyValueStore<string, string> NewStoreInstance() {
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
    public void ShouldReturnNullIfKeyDoesntExist() {
        assertNull(theStore.get("whatever"));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnGetNullKey() {
        theStore.get(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnRangeNullFromKey() {
        theStore.range(null, "to");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnRangeNullToKey() {
        theStore.range("from", null);
    }

    [Xunit.Fact]
    public void ShouldReturnValueIfExists() {
        stubOneUnderlying.put("key", "value");
        Assert.Equal("value", theStore.get("key"));
    }

    [Xunit.Fact]
    public void ShouldNotGetValuesFromOtherStores() {
        otherUnderlyingStore.put("otherKey", "otherValue");
        assertNull(theStore.get("otherKey"));
    }

    [Xunit.Fact]
    public void ShouldThrowNoSuchElementExceptionWhileNext() {
        stubOneUnderlying.put("a", "1");
        KeyValueIterator<string, string> keyValueIterator = theStore.range("a", "b");
        keyValueIterator.next();
        try {
            keyValueIterator.next();
            Assert.True(false, "Should have thrown NoSuchElementException with next()");
        } catch (NoSuchElementException e) { }
    }

    [Xunit.Fact]
    public void ShouldThrowNoSuchElementExceptionWhilePeekNext() {
        stubOneUnderlying.put("a", "1");
        KeyValueIterator<string, string> keyValueIterator = theStore.range("a", "b");
        keyValueIterator.next();
        try {
            keyValueIterator.peekNextKey();
            Assert.True(false, "Should have thrown NoSuchElementException with peekNextKey()");
        } catch (NoSuchElementException e) { }
    }

    [Xunit.Fact]
    public void ShouldThrowUnsupportedOperationExceptionWhileRemove() {
        KeyValueIterator<string, string> keyValueIterator = theStore.all();
        try {
            keyValueIterator.remove();
            Assert.True(false, "Should have thrown UnsupportedOperationException");
        } catch (UnsupportedOperationException e) { }
    }

    [Xunit.Fact]
    public void ShouldThrowUnsupportedOperationExceptionWhileRange() {
        stubOneUnderlying.put("a", "1");
        stubOneUnderlying.put("b", "1");
        KeyValueIterator<string, string> keyValueIterator = theStore.range("a", "b");
        try {
            keyValueIterator.remove();
            Assert.True(false, "Should have thrown UnsupportedOperationException");
        } catch (UnsupportedOperationException e) { }
    }

    [Xunit.Fact]
    public void ShouldFindValueForKeyWhenMultiStores() {
        KeyValueStore<string, string> cache = NewStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        cache.put("key-two", "key-two-value");
        stubOneUnderlying.put("key-one", "key-one-value");

        Assert.Equal("key-two-value", theStore.get("key-two"));
        Assert.Equal("key-one-value", theStore.get("key-one"));
    }

    [Xunit.Fact]
    public void ShouldSupportRange() {
        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("c", "c");

        List<KeyValuePair<string, string>> results = toList(theStore.range("a", "b"));
        Assert.True(results.Contains(new KeyValuePair<>("a", "a")));
        Assert.True(results.Contains(new KeyValuePair<>("b", "b")));
        Assert.Equal(2, results.Count);
    }

    [Xunit.Fact]
    public void ShouldSupportRangeAcrossMultipleKVStores() {
        KeyValueStore<string, string> cache = NewStoreInstance();
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
    public void ShouldSupportAllAcrossMultipleStores() {
        KeyValueStore<string, string> cache = NewStoreInstance();
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
    public void ShouldThrowInvalidStoreExceptionDuringRebalance() {
        rebalancing().get("anything");
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowInvalidStoreExceptionOnApproximateNumEntriesDuringRebalance() {
        rebalancing().approximateNumEntries();
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowInvalidStoreExceptionOnRangeDuringRebalance() {
        rebalancing().range("anything", "something");
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowInvalidStoreExceptionOnAllDuringRebalance() {
        rebalancing().all();
    }

    [Xunit.Fact]
    public void ShouldGetApproximateEntriesAcrossAllStores() {
        KeyValueStore<string, string> cache = NewStoreInstance();
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
    public void ShouldReturnLongMaxValueOnOverflow() {
        stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<object, object>() {
            
            public long approximateNumEntries() {
                return long.MaxValue;
            }
        });

        stubOneUnderlying.put("overflow", "me");
        Assert.Equal(long.MaxValue, theStore.approximateNumEntries());
    }

    [Xunit.Fact]
    public void ShouldReturnLongMaxValueOnUnderflow() {
        stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<object, object>() {
            
            public long approximateNumEntries() {
                return long.MaxValue;
            }
        });
        stubProviderTwo.addStore(storeNameA, new NoOpReadOnlyStore<object, object>() {
            
            public long ApproximateNumEntries() {
                return long.MaxValue;
            }
        });

        Assert.Equal(long.MaxValue, theStore.approximateNumEntries());
    }

    private CompositeReadOnlyKeyValueStore<object, object> Rebalancing() {
        return new CompositeReadOnlyKeyValueStore<>(new WrappingStoreProvider(Collections.<StateStoreProvider>singletonList(new StateStoreProviderStub(true))),
                QueryableStoreTypes.keyValueStore(), storeName);
    }

}