/*






 *

 *





 */

















public class QueryableStoreProviderTest {

    private string keyValueStore = "key-value";
    private string windowStore = "window-store";
    private QueryableStoreProvider storeProvider;
    private HashDictionary<string, StateStore> globalStateStores;

    
    public void before() {
        StateStoreProviderStub theStoreProvider = new StateStoreProviderStub(false);
        theStoreProvider.addStore(keyValueStore, new NoOpReadOnlyStore<>());
        theStoreProvider.addStore(windowStore, new NoOpWindowStore());
        globalStateStores = new HashMap<>();
        storeProvider =
            new QueryableStoreProvider(
                    Collections.<StateStoreProvider>singletonList(theStoreProvider), new GlobalStateStoreProvider(globalStateStores));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowExceptionIfKVStoreDoesntExist() {
        storeProvider.getStore("not-a-store", QueryableStoreTypes.keyValueStore());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowExceptionIfWindowStoreDoesntExist() {
        storeProvider.getStore("not-a-store", QueryableStoreTypes.windowStore());
    }

    [Xunit.Fact]
    public void shouldReturnKVStoreWhenItExists() {
        assertNotNull(storeProvider.getStore(keyValueStore, QueryableStoreTypes.keyValueStore()));
    }

    [Xunit.Fact]
    public void shouldReturnWindowStoreWhenItExists() {
        assertNotNull(storeProvider.getStore(windowStore, QueryableStoreTypes.windowStore()));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowExceptionWhenLookingForWindowStoreWithDifferentType() {
        storeProvider.getStore(windowStore, QueryableStoreTypes.keyValueStore());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowExceptionWhenLookingForKVStoreWithDifferentType() {
        storeProvider.getStore(keyValueStore, QueryableStoreTypes.windowStore());
    }

    [Xunit.Fact]
    public void shouldFindGlobalStores() {
        globalStateStores.put("global", new NoOpReadOnlyStore<>());
        assertNotNull(storeProvider.getStore("global", QueryableStoreTypes.keyValueStore()));
    }


}