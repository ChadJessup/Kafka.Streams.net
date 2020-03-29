/*






 *

 *





 */














public class InMemoryKeyValueStoreTest : AbstractKeyValueStoreTest {

    
    
    protected KeyValueStore<K, V> CreateKeyValueStore<K, V>(ProcessorContext context) {
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("my-store"),
                (Serde<K>) context.keySerde(),
                (Serde<V>) context.valueSerde());

        StateStore store = storeBuilder.build();
        store.init(context, store);
        return (KeyValueStore<K, V>) store;
    }

    [Xunit.Fact]
    public void ShouldRemoveKeysWithNullValues() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");
        driver.addEntryToRestoreLog(0, null);

        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());

        Assert.Equal(3, driver.sizeOf(store));

        Assert.Equal(store.get(0), nullValue());
    }
}
