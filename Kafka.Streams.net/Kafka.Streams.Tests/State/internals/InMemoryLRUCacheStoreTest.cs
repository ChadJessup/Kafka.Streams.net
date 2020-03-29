/*






 *

 *





 */




















public class InMemoryLRUCacheStoreTest : AbstractKeyValueStoreTest {

    
    
    protected KeyValueStore<K, V> createKeyValueStore<K, V>(ProcessorContext context) {

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.lruMap("my-store", 10),
                (Serde<K>) context.keySerde(),
                (Serde<V>) context.valueSerde());

        StateStore store = storeBuilder.build();
        store.init(context, store);

        return (KeyValueStore<K, V>) store;
    }

    [Xunit.Fact]
    public void shouldPutAllKeyValuePairs() {
        List<KeyValuePair<int, string>> kvPairs = Array.asList(KeyValuePair.Create(1, "1"),
                KeyValuePair.Create(2, "2"),
                KeyValuePair.Create(3, "3"));

        store.putAll(kvPairs);

        Assert.Equal(store.approximateNumEntries(), (3L));

        foreach (KeyValuePair<int, string> kvPair in kvPairs) {
            Assert.Equal(store.get(kvPair.key), (kvPair.value));
        }
    }

    [Xunit.Fact]
    public void shouldUpdateValuesForExistingKeysOnPutAll() {
        List<KeyValuePair<int, string>> kvPairs = Array.asList(KeyValuePair.Create(1, "1"),
                KeyValuePair.Create(2, "2"),
                KeyValuePair.Create(3, "3"));

        store.putAll(kvPairs);
        

        List<KeyValuePair<int, string>> updatedKvPairs = Array.asList(KeyValuePair.Create(1, "ONE"),
                KeyValuePair.Create(2, "TWO"),
                KeyValuePair.Create(3, "THREE"));

        store.putAll(updatedKvPairs);

        Assert.Equal(store.approximateNumEntries(), (3L));
        
        foreach (KeyValuePair<int, string> kvPair in updatedKvPairs) {
            Assert.Equal(store.get(kvPair.key), (kvPair.value));
        }
    }

    [Xunit.Fact]
    public void testEvict() {
        // Create the test driver ...
        store.put(0, "zero");
        store.put(1, "one");
        store.put(2, "two");
        store.put(3, "three");
        store.put(4, "four");
        store.put(5, "five");
        store.put(6, "six");
        store.put(7, "seven");
        store.put(8, "eight");
        store.put(9, "nine");
        Assert.Equal(10, driver.sizeOf(store));

        store.put(10, "ten");
        store.flush();
        Assert.Equal(10, driver.sizeOf(store));
        Assert.True(driver.flushedEntryRemoved(0));
        Assert.Equal(1, driver.numFlushedEntryRemoved());

        store.delete(1);
        store.flush();
        Assert.Equal(9, driver.sizeOf(store));
        Assert.True(driver.flushedEntryRemoved(0));
        Assert.True(driver.flushedEntryRemoved(1));
        Assert.Equal(2, driver.numFlushedEntryRemoved());

        store.put(11, "eleven");
        store.flush();
        Assert.Equal(10, driver.sizeOf(store));
        Assert.Equal(2, driver.numFlushedEntryRemoved());

        store.put(2, "two-again");
        store.flush();
        Assert.Equal(10, driver.sizeOf(store));
        Assert.Equal(2, driver.numFlushedEntryRemoved());

        store.put(12, "twelve");
        store.flush();
        Assert.Equal(10, driver.sizeOf(store));
        Assert.True(driver.flushedEntryRemoved(0));
        Assert.True(driver.flushedEntryRemoved(1));
        Assert.True(driver.flushedEntryRemoved(3));
        Assert.Equal(3, driver.numFlushedEntryRemoved());
    }

    [Xunit.Fact]
    public void testRestoreEvict() {
        store.close();
        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "zero");
        driver.addEntryToRestoreLog(1, "one");
        driver.addEntryToRestoreLog(2, "two");
        driver.addEntryToRestoreLog(3, "three");
        driver.addEntryToRestoreLog(4, "four");
        driver.addEntryToRestoreLog(5, "five");
        driver.addEntryToRestoreLog(6, "fix");
        driver.addEntryToRestoreLog(7, "seven");
        driver.addEntryToRestoreLog(8, "eight");
        driver.addEntryToRestoreLog(9, "nine");
        driver.addEntryToRestoreLog(10, "ten");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        store = createKeyValueStore(driver.context());
        context.restore(store.name(), driver.restoredEntries());
        // Verify that the store's changelog does not get more appends ...
        Assert.Equal(0, driver.numFlushedEntryStored());
        Assert.Equal(0, driver.numFlushedEntryRemoved());

        // and there are no other entries ...
        Assert.Equal(10, driver.sizeOf(store));
    }
}
