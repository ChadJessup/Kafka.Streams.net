/*






 *

 *





 */








































public class CachingKeyValueStoreTest : AbstractKeyValueStoreTest {

    private int maxCacheSizeBytes = 150;
    private InternalMockProcessorContext context;
    private CachingKeyValueStore store;
    private InMemoryKeyValueStore underlyingStore;
    private ThreadCache cache;
    private CacheFlushListenerStub<string, string> cacheFlushListener;
    private string topic;

    
    public void setUp() {
        string storeName = "store";
        underlyingStore = new InMemoryKeyValueStore(storeName);
        cacheFlushListener = new CacheFlushListenerStub<>(new StringDeserializer(), new StringDeserializer());
        store = new CachingKeyValueStore(underlyingStore);
        store.setFlushListener(cacheFlushListener, false);
        cache = new ThreadCache(new LogContext("testCache "), maxCacheSizeBytes, new MockStreamsMetrics(new Metrics()));
        context = new InternalMockProcessorContext(null, null, null, null, cache);
        topic = "topic";
        context.setRecordContext(new ProcessorRecordContext(10, 0, 0, topic, null));
        store.init(context, null);
    }

    
    public void after() {
        base.after();
    }

    
    
    protected KeyValueStore<K, V> createKeyValueStore<K, V>(ProcessorContext context) {
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("cache-store"),
                (Serde<K>) context.keySerde(),
                (Serde<V>) context.valueSerde())
                .withCachingEnabled();

        KeyValueStore<K, V> store = (KeyValueStore<K, V>) storeBuilder.build();
        store.init(context, store);
        return store;
    }

    [Xunit.Fact]
    public void shouldSetFlushListener() {
        Assert.True(store.setFlushListener(null, true));
        Assert.True(store.setFlushListener(null, false));
    }

    [Xunit.Fact]
    public void shouldAvoidFlushingDeletionsWithoutDirtyKeys() {
        int added = addItemsToCache();
        // all dirty entries should have been flushed
        Assert.Equal(added, underlyingStore.approximateNumEntries());
        Assert.Equal(added, cacheFlushListener.forwarded.Count);

        store.put(bytesKey("key"), bytesValue("value"));
        Assert.Equal(added, underlyingStore.approximateNumEntries());
        Assert.Equal(added, cacheFlushListener.forwarded.Count);

        store.put(bytesKey("key"), null);
        store.flush();
        Assert.Equal(added, underlyingStore.approximateNumEntries());
        Assert.Equal(added, cacheFlushListener.forwarded.Count);
    }

    [Xunit.Fact]
    public void shouldCloseAfterErrorWithFlush() {
        try {
            cache = EasyMock.niceMock(ThreadCache);
            context = new InternalMockProcessorContext(null, null, null, null, cache);
            context.setRecordContext(new ProcessorRecordContext(10, 0, 0, topic, null));
            store.init(context, null);
            cache.flush("0_0-store");
            EasyMock.expectLastCall().andThrow(new NullPointerException("Simulating an error on flush"));
            EasyMock.replay(cache);
            store.close();
        } catch (NullPointerException npe) {
            Assert.False(underlyingStore.isOpen());
        }
    }

    [Xunit.Fact]
    public void shouldPutGetToFromCache() {
        store.put(bytesKey("key"), bytesValue("value"));
        store.put(bytesKey("key2"), bytesValue("value2"));
        Assert.Equal(store.get(bytesKey("key")), (bytesValue("value")));
        Assert.Equal(store.get(bytesKey("key2")), (bytesValue("value2")));
        // nothing evicted so underlying store should be empty
        Assert.Equal(2, cache.Count);
        Assert.Equal(0, underlyingStore.approximateNumEntries());
    }

    private byte[] bytesValue(string value) {
        return value.getBytes();
    }

    private Bytes bytesKey(string key) {
        return Bytes.wrap(key.getBytes());
    }

    [Xunit.Fact]
    public void shouldFlushEvictedItemsIntoUnderlyingStore() {
        int added = addItemsToCache();
        // all dirty entries should have been flushed
        Assert.Equal(added, underlyingStore.approximateNumEntries());
        Assert.Equal(added, store.approximateNumEntries());
        assertNotNull(underlyingStore.get(Bytes.wrap("0".getBytes())));
    }

    [Xunit.Fact]
    public void shouldForwardDirtyItemToListenerWhenEvicted() {
        int numRecords = addItemsToCache();
        Assert.Equal(numRecords, cacheFlushListener.forwarded.Count);
    }

    [Xunit.Fact]
    public void shouldForwardDirtyItemsWhenFlushCalled() {
        store.put(bytesKey("1"), bytesValue("a"));
        store.flush();
        Assert.Equal("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
    }

    [Xunit.Fact]
    public void shouldForwardOldValuesWhenEnabled() {
        store.setFlushListener(cacheFlushListener, true);
        store.put(bytesKey("1"), bytesValue("a"));
        store.flush();
        Assert.Equal("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), bytesValue("b"));
        store.put(bytesKey("1"), bytesValue("c"));
        store.flush();
        Assert.Equal("c", cacheFlushListener.forwarded.get("1").newValue);
        Assert.Equal("a", cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1").newValue);
        Assert.Equal("c", cacheFlushListener.forwarded.get("1").oldValue);
        cacheFlushListener.forwarded.Clear();
        store.put(bytesKey("1"), bytesValue("a"));
        store.put(bytesKey("1"), bytesValue("b"));
        store.put(bytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1"));
        cacheFlushListener.forwarded.Clear();
    }

    [Xunit.Fact]
    public void shouldNotForwardOldValuesWhenDisabled() {
        store.put(bytesKey("1"), bytesValue("a"));
        store.flush();
        Assert.Equal("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), bytesValue("b"));
        store.flush();
        Assert.Equal("b", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(bytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        cacheFlushListener.forwarded.Clear();
        store.put(bytesKey("1"), bytesValue("a"));
        store.put(bytesKey("1"), bytesValue("b"));
        store.put(bytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1"));
        cacheFlushListener.forwarded.Clear();
    }

    [Xunit.Fact]
    public void shouldIterateAllStoredItems() {
        int items = addItemsToCache();
        KeyValueIterator<Bytes, byte[]> all = store.all();
        List<Bytes> results = new ArrayList<>();
        while (all.hasNext()) {
            results.add(all.next().key);
        }
        Assert.Equal(items, results.Count);
    }

    [Xunit.Fact]
    public void shouldIterateOverRange() {
        int items = addItemsToCache();
        KeyValueIterator<Bytes, byte[]> range = store.range(bytesKey(string.valueOf(0)), bytesKey(string.valueOf(items)));
        List<Bytes> results = new ArrayList<>();
        while (range.hasNext()) {
            results.add(range.next().key);
        }
        Assert.Equal(items, results.Count);
    }

    [Xunit.Fact]
    public void shouldDeleteItemsFromCache() {
        store.put(bytesKey("a"), bytesValue("a"));
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        Assert.False(store.range(bytesKey("a"), bytesKey("b")).hasNext());
        Assert.False(store.all().hasNext());
    }

    [Xunit.Fact]
    public void shouldNotShowItemsDeletedFromCacheButFlushedToStoreBeforeDelete() {
        store.put(bytesKey("a"), bytesValue("a"));
        store.flush();
        store.delete(bytesKey("a"));
        assertNull(store.get(bytesKey("a")));
        Assert.False(store.range(bytesKey("a"), bytesKey("b")).hasNext());
        Assert.False(store.all().hasNext());
    }

    [Xunit.Fact]
    public void shouldClearNamespaceCacheOnClose() {
        store.put(bytesKey("a"), bytesValue("a"));
        Assert.Equal(1, cache.Count);
        store.close();
        Assert.Equal(0, cache.Count);
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToGetFromClosedCachingStore() {
        store.close();
        store.get(bytesKey("a"));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToWriteToClosedCachingStore() {
        store.close();
        store.put(bytesKey("a"), bytesValue("a"));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToDoRangeQueryOnClosedCachingStore() {
        store.close();
        store.range(bytesKey("a"), bytesKey("b"));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToDoAllQueryOnClosedCachingStore() {
        store.close();
        store.all();
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToDoGetApproxSizeOnClosedCachingStore() {
        store.close();
        store.approximateNumEntries();
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToDoPutAllClosedCachingStore() {
        store.close();
        store.putAll(Collections.singletonList(KeyValuePair.Create(bytesKey("a"), bytesValue("a"))));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToDoPutIfAbsentClosedCachingStore() {
        store.close();
        store.putIfAbsent(bytesKey("b"), bytesValue("c"));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnPutWithNullKey() {
        store.put(null, bytesValue("c"));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnPutIfAbsentWithNullKey() {
        store.putIfAbsent(null, bytesValue("c"));
    }

    [Xunit.Fact]
    public void shouldThrowNullPointerExceptionOnPutAllWithNullKey() {
        List<KeyValuePair<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValuePair<>(null, bytesValue("a")));
        try {
            store.putAll(entries);
            Assert.True(false, "Should have thrown NullPointerException while putAll null key");
        } catch (NullPointerException expected) {
        }
    }

    [Xunit.Fact]
    public void shouldPutIfAbsent() {
        store.putIfAbsent(bytesKey("b"), bytesValue("2"));
        Assert.Equal(store.get(bytesKey("b")), (bytesValue("2")));

        store.putIfAbsent(bytesKey("b"), bytesValue("3"));
        Assert.Equal(store.get(bytesKey("b")), (bytesValue("2")));
    }

    [Xunit.Fact]
    public void shouldPutAll() {
        List<KeyValuePair<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValuePair<>(bytesKey("a"), bytesValue("1")));
        entries.add(new KeyValuePair<>(bytesKey("b"), bytesValue("2")));
        store.putAll(entries);
        Assert.Equal(store.get(bytesKey("a")), (bytesValue("1")));
        Assert.Equal(store.get(bytesKey("b")), (bytesValue("2")));
    }

    [Xunit.Fact]
    public void shouldReturnUnderlying() {
        Assert.Equal(underlyingStore, store.wrapped());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToDeleteFromClosedCachingStore() {
        store.close();
        store.delete(bytesKey("key"));
    }

    private int addItemsToCache() {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < maxCacheSizeBytes) {
            string kv = string.valueOf(i++);
            store.put(bytesKey(kv), bytesValue(kv));
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic);
        }
        return i;
    }

    public static class CacheFlushListenerStub<K, V> : CacheFlushListener<byte[], byte[]> {
        Deserializer<K> keyDeserializer;
        Deserializer<V> valueDesializer;
        Dictionary<K, Change<V>> forwarded = new HashMap<>();

        CacheFlushListenerStub(Deserializer<K> keyDeserializer,
                               Deserializer<V> valueDesializer) {
            this.keyDeserializer = keyDeserializer;
            this.valueDesializer = valueDesializer;
        }

        
        public void apply(byte[] key,
                          byte[] newValue,
                          byte[] oldValue,
                          long timestamp) {
            forwarded.put(
                keyDeserializer.deserialize(null, key),
                new Change<>(
                    valueDesializer.deserialize(null, newValue),
                    valueDesializer.deserialize(null, oldValue)));
        }
    }
}
