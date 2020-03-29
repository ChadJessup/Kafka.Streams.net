/*






 *

 *





 */





















public class RocksDBKeyValueStoreTest : AbstractKeyValueStoreTest {

    
    
    protected KeyValueStore<K, V> CreateKeyValueStore<K, V>(ProcessorContext context) {
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("my-store"),
                (Serde<K>) context.keySerde(),
                (Serde<V>) context.valueSerde());

        StateStore store = storeBuilder.build();
        store.init(context, store);
        return (KeyValueStore<K, V>) store;
    }

    public static class TheRocksDbConfigSetter : RocksDBConfigSetter {
        static bool called = false;

        
        public void SetConfig(string storeName, Options options, Dictionary<string, object> configs) {
            called = true;
        }
    }

    [Xunit.Fact]
    public void ShouldUseCustomRocksDbConfigSetter() {
        Assert.True(TheRocksDbConfigSetter.called);
    }

    [Xunit.Fact]
    public void ShouldPerformRangeQueriesWithCachingDisabled() {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        KeyValueIterator<int, string> range = store.range(1, 2);
        Assert.Equal("hi", range.next().value);
        Assert.Equal("goodbye", range.next().value);
        Assert.False(range.hasNext());
    }

    [Xunit.Fact]
    public void ShouldPerformAllQueriesWithCachingDisabled() {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        KeyValueIterator<int, string> range = store.all();
        Assert.Equal("hi", range.next().value);
        Assert.Equal("goodbye", range.next().value);
        Assert.False(range.hasNext());
    }

    [Xunit.Fact]
    public void ShouldCloseOpenIteratorsWhenStoreClosedAndThrowInvalidStateStoreOnHasNextAndNext() {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        KeyValueIterator<int, string> iteratorOne = store.range(1, 5);
        KeyValueIterator<int, string> iteratorTwo = store.range(1, 4);

        Assert.True(iteratorOne.hasNext());
        Assert.True(iteratorTwo.hasNext());

        store.close();

        try {
            iteratorOne.hasNext();
            Assert.True(false, "should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }

        try {
            iteratorOne.next();
            Assert.True(false, "should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }

        try {
            iteratorTwo.hasNext();
            Assert.True(false, "should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }

        try {
            iteratorTwo.next();
            Assert.True(false, "should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }
    }

}
