/*






 *

 *





 */

















public class QueryableStoreProviderTest {

    private readonly string keyValueStore = "key-value";
    private readonly string windowStore = "window-store";
    private QueryableStoreProvider storeProvider;
    private HashDictionary<string, StateStore> globalStateStores;

    
    public void Before() {
        StateStoreProviderStub theStoreProvider = new StateStoreProviderStub(false);
        theStoreProvider.addStore(keyValueStore, new NoOpReadOnlyStore<>());
        theStoreProvider.addStore(windowStore, new NoOpWindowStore());
        globalStateStores = new HashMap<>();
        storeProvider =
            new QueryableStoreProvider(
                    Collections.<StateStoreProvider>singletonList(theStoreProvider), new GlobalStateStoreProvider(globalStateStores));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowExceptionIfKVStoreDoesntExist() {
        storeProvider.getStore("not-a-store", QueryableStoreTypes.keyValueStore());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowExceptionIfWindowStoreDoesntExist() {
        storeProvider.getStore("not-a-store", QueryableStoreTypes.windowStore());
    }

    [Xunit.Fact]
    public void ShouldReturnKVStoreWhenItExists() {
        assertNotNull(storeProvider.getStore(keyValueStore, QueryableStoreTypes.keyValueStore()));
    }

    [Xunit.Fact]
    public void ShouldReturnWindowStoreWhenItExists() {
        assertNotNull(storeProvider.getStore(windowStore, QueryableStoreTypes.windowStore()));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowExceptionWhenLookingForWindowStoreWithDifferentType() {
        storeProvider.getStore(windowStore, QueryableStoreTypes.keyValueStore());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowExceptionWhenLookingForKVStoreWithDifferentType() {
        storeProvider.getStore(keyValueStore, QueryableStoreTypes.windowStore());
    }

    [Xunit.Fact]
    public void ShouldFindGlobalStores() {
        globalStateStores.put("global", new NoOpReadOnlyStore<>());
        assertNotNull(storeProvider.getStore("global", QueryableStoreTypes.keyValueStore()));
    }


}