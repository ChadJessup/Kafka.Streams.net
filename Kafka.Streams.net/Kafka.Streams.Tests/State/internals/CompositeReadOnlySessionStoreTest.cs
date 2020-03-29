/*






 *

 *





 */

























public class CompositeReadOnlySessionStoreTest {

    private string storeName = "session-store";
    private StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
    private StateStoreProviderStub stubProviderTwo = new StateStoreProviderStub(false);
    private ReadOnlySessionStoreStub<string, long> underlyingSessionStore = new ReadOnlySessionStoreStub<>();
    private ReadOnlySessionStoreStub<string, long> otherUnderlyingStore = new ReadOnlySessionStoreStub<>();
    private CompositeReadOnlySessionStore<string, long> sessionStore;

    
    public void Before() {
        stubProviderOne.addStore(storeName, underlyingSessionStore);
        stubProviderOne.addStore("other-session-store", otherUnderlyingStore);


        sessionStore = new CompositeReadOnlySessionStore<>(
                new WrappingStoreProvider(Array.<StateStoreProvider>asList(stubProviderOne, stubProviderTwo)),
                QueryableStoreTypes.<string, long>sessionStore(), storeName);
    }

    [Xunit.Fact]
    public void ShouldFetchResulstFromUnderlyingSessionStore() {
        underlyingSessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        underlyingSessionStore.put(new Windowed<>("a", new SessionWindow(10, 10)), 2L);

        List<KeyValuePair<Windowed<string>, long>> results = toList(sessionStore.fetch("a"));
        Assert.Equal(Array.asList(KeyValuePair.Create(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
                                   KeyValuePair.Create(new Windowed<>("a", new SessionWindow(10, 10)), 2L)),
                     results);
    }

    [Xunit.Fact]
    public void ShouldReturnEmptyIteratorIfNoData() {
        KeyValueIterator<Windowed<string>, long> result = sessionStore.fetch("b");
        Assert.False(result.hasNext());
    }

    [Xunit.Fact]
    public void ShouldFindValueForKeyWhenMultiStores() {
        ReadOnlySessionStoreStub<string, long> secondUnderlying = new
                ReadOnlySessionStoreStub<>();
        stubProviderTwo.addStore(storeName, secondUnderlying);

        Windowed<string> keyOne = new Windowed<>("key-one", new SessionWindow(0, 0));
        Windowed<string> keyTwo = new Windowed<>("key-two", new SessionWindow(0, 0));
        underlyingSessionStore.put(keyOne, 0L);
        secondUnderlying.put(keyTwo, 10L);

        List<KeyValuePair<Windowed<string>, long>> keyOneResults = toList(sessionStore.fetch("key-one"));
        List<KeyValuePair<Windowed<string>, long>> keyTwoResults = toList(sessionStore.fetch("key-two"));

        Assert.Equal(Collections.singletonList(KeyValuePair.Create(keyOne, 0L)), keyOneResults);
        Assert.Equal(Collections.singletonList(KeyValuePair.Create(keyTwo, 10L)), keyTwoResults);
    }

    [Xunit.Fact]
    public void ShouldNotGetValueFromOtherStores() {
        Windowed<string> expectedKey = new Windowed<>("foo", new SessionWindow(0, 0));
        otherUnderlyingStore.put(new Windowed<>("foo", new SessionWindow(10, 10)), 10L);
        underlyingSessionStore.put(expectedKey, 1L);

        KeyValueIterator<Windowed<string>, long> result = sessionStore.fetch("foo");
        Assert.Equal(KeyValuePair.Create(expectedKey, 1L), result.next());
        Assert.False(result.hasNext());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowInvalidStateStoreExceptionOnRebalance() {
        CompositeReadOnlySessionStore<string, string> store =
            new CompositeReadOnlySessionStore<>(
                new StateStoreProviderStub(true),
                QueryableStoreTypes.sessionStore(),
                "whateva");

        store.fetch("a");
    }

    [Xunit.Fact]
    public void ShouldThrowInvalidStateStoreExceptionIfSessionFetchThrows() {
        underlyingSessionStore.setOpen(false);
        try {
            sessionStore.fetch("key");
            Assert.True(false, "Should have thrown InvalidStateStoreException with session store");
        } catch (InvalidStateStoreException e) { }
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionIfFetchingNullKey() {
        sessionStore.fetch(null);
    }

    [Xunit.Fact]
    public void ShouldFetchKeyRangeAcrossStores() {
        ReadOnlySessionStoreStub<string, long> secondUnderlying = new
                ReadOnlySessionStoreStub<>();
        stubProviderTwo.addStore(storeName, secondUnderlying);
        underlyingSessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 0L);
        secondUnderlying.put(new Windowed<>("b", new SessionWindow(0, 0)), 10L);
        List<KeyValuePair<Windowed<string>, long>> results = StreamsTestUtils.toList(sessionStore.fetch("a", "b"));
        Assert.Equal(results.Count, (2));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNPEIfKeyIsNull() {
        underlyingSessionStore.fetch(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNPEIfFromKeyIsNull() {
        underlyingSessionStore.fetch(null, "a");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNPEIfToKeyIsNull() {
        underlyingSessionStore.fetch("a", null);
    }
}
