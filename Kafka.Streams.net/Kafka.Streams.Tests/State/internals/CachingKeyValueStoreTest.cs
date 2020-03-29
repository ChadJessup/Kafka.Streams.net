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

    
    public void SetUp() {
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

    
    public void After() {
        base.After();
    }

    
    
    protected KeyValueStore<K, V> CreateKeyValueStore<K, V>(ProcessorContext context) {
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
    public void ShouldSetFlushListener() {
        Assert.True(store.setFlushListener(null, true));
        Assert.True(store.setFlushListener(null, false));
    }

    [Xunit.Fact]
    public void ShouldAvoidFlushingDeletionsWithoutDirtyKeys() {
        int added = AddItemsToCache();
        // all dirty entries should have been flushed
        Assert.Equal(added, underlyingStore.approximateNumEntries());
        Assert.Equal(added, cacheFlushListener.forwarded.Count);

        store.put(BytesKey("key"), BytesValue("value"));
        Assert.Equal(added, underlyingStore.approximateNumEntries());
        Assert.Equal(added, cacheFlushListener.forwarded.Count);

        store.put(BytesKey("key"), null);
        store.flush();
        Assert.Equal(added, underlyingStore.approximateNumEntries());
        Assert.Equal(added, cacheFlushListener.forwarded.Count);
    }

    [Xunit.Fact]
    public void ShouldCloseAfterErrorWithFlush() {
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
    public void ShouldPutGetToFromCache() {
        store.put(BytesKey("key"), BytesValue("value"));
        store.put(BytesKey("key2"), BytesValue("value2"));
        Assert.Equal(store.get(BytesKey("key")), (BytesValue("value")));
        Assert.Equal(store.get(BytesKey("key2")), (BytesValue("value2")));
        // nothing evicted so underlying store should be empty
        Assert.Equal(2, cache.Count);
        Assert.Equal(0, underlyingStore.approximateNumEntries());
    }

    private byte[] BytesValue(string value) {
        return value.getBytes();
    }

    private Bytes BytesKey(string key) {
        return Bytes.wrap(key.getBytes());
    }

    [Xunit.Fact]
    public void ShouldFlushEvictedItemsIntoUnderlyingStore() {
        int added = AddItemsToCache();
        // all dirty entries should have been flushed
        Assert.Equal(added, underlyingStore.approximateNumEntries());
        Assert.Equal(added, store.approximateNumEntries());
        assertNotNull(underlyingStore.get(Bytes.wrap("0".getBytes())));
    }

    [Xunit.Fact]
    public void ShouldForwardDirtyItemToListenerWhenEvicted() {
        int numRecords = AddItemsToCache();
        Assert.Equal(numRecords, cacheFlushListener.forwarded.Count);
    }

    [Xunit.Fact]
    public void ShouldForwardDirtyItemsWhenFlushCalled() {
        store.put(BytesKey("1"), BytesValue("a"));
        store.flush();
        Assert.Equal("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
    }

    [Xunit.Fact]
    public void ShouldForwardOldValuesWhenEnabled() {
        store.setFlushListener(cacheFlushListener, true);
        store.put(BytesKey("1"), BytesValue("a"));
        store.flush();
        Assert.Equal("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(BytesKey("1"), BytesValue("b"));
        store.put(BytesKey("1"), BytesValue("c"));
        store.flush();
        Assert.Equal("c", cacheFlushListener.forwarded.get("1").newValue);
        Assert.Equal("a", cacheFlushListener.forwarded.get("1").oldValue);
        store.put(BytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1").newValue);
        Assert.Equal("c", cacheFlushListener.forwarded.get("1").oldValue);
        cacheFlushListener.forwarded.Clear();
        store.put(BytesKey("1"), BytesValue("a"));
        store.put(BytesKey("1"), BytesValue("b"));
        store.put(BytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1"));
        cacheFlushListener.forwarded.Clear();
    }

    [Xunit.Fact]
    public void ShouldNotForwardOldValuesWhenDisabled() {
        store.put(BytesKey("1"), BytesValue("a"));
        store.flush();
        Assert.Equal("a", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(BytesKey("1"), BytesValue("b"));
        store.flush();
        Assert.Equal("b", cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        store.put(BytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1").newValue);
        assertNull(cacheFlushListener.forwarded.get("1").oldValue);
        cacheFlushListener.forwarded.Clear();
        store.put(BytesKey("1"), BytesValue("a"));
        store.put(BytesKey("1"), BytesValue("b"));
        store.put(BytesKey("1"), null);
        store.flush();
        assertNull(cacheFlushListener.forwarded.get("1"));
        cacheFlushListener.forwarded.Clear();
    }

    [Xunit.Fact]
    public void ShouldIterateAllStoredItems() {
        int items = AddItemsToCache();
        KeyValueIterator<Bytes, byte[]> all = store.all();
        List<Bytes> results = new ArrayList<>();
        while (all.hasNext()) {
            results.add(all.next().key);
        }
        Assert.Equal(items, results.Count);
    }

    [Xunit.Fact]
    public void ShouldIterateOverRange() {
        int items = AddItemsToCache();
        KeyValueIterator<Bytes, byte[]> range = store.range(bytesKey(string.valueOf(0)), bytesKey(string.valueOf(items)));
        List<Bytes> results = new ArrayList<>();
        while (range.hasNext()) {
            results.add(range.next().key);
        }
        Assert.Equal(items, results.Count);
    }

    [Xunit.Fact]
    public void ShouldDeleteItemsFromCache() {
        store.put(BytesKey("a"), BytesValue("a"));
        store.delete(BytesKey("a"));
        assertNull(store.get(BytesKey("a")));
        Assert.False(store.range(BytesKey("a"), BytesKey("b")).hasNext());
        Assert.False(store.all().hasNext());
    }

    [Xunit.Fact]
    public void ShouldNotShowItemsDeletedFromCacheButFlushedToStoreBeforeDelete() {
        store.put(BytesKey("a"), BytesValue("a"));
        store.flush();
        store.delete(BytesKey("a"));
        assertNull(store.get(BytesKey("a")));
        Assert.False(store.range(BytesKey("a"), BytesKey("b")).hasNext());
        Assert.False(store.all().hasNext());
    }

    [Xunit.Fact]
    public void ShouldClearNamespaceCacheOnClose() {
        store.put(BytesKey("a"), BytesValue("a"));
        Assert.Equal(1, cache.Count);
        store.close();
        Assert.Equal(0, cache.Count);
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToGetFromClosedCachingStore() {
        store.close();
        store.get(BytesKey("a"));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToWriteToClosedCachingStore() {
        store.close();
        store.put(BytesKey("a"), BytesValue("a"));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToDoRangeQueryOnClosedCachingStore() {
        store.close();
        store.range(BytesKey("a"), BytesKey("b"));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToDoAllQueryOnClosedCachingStore() {
        store.close();
        store.all();
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToDoGetApproxSizeOnClosedCachingStore() {
        store.close();
        store.approximateNumEntries();
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToDoPutAllClosedCachingStore() {
        store.close();
        store.putAll(Collections.singletonList(KeyValuePair.Create(BytesKey("a"), BytesValue("a"))));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToDoPutIfAbsentClosedCachingStore() {
        store.close();
        store.putIfAbsent(BytesKey("b"), BytesValue("c"));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnPutWithNullKey() {
        store.put(null, BytesValue("c"));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnPutIfAbsentWithNullKey() {
        store.putIfAbsent(null, BytesValue("c"));
    }

    [Xunit.Fact]
    public void ShouldThrowNullPointerExceptionOnPutAllWithNullKey() {
        List<KeyValuePair<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValuePair<>(null, BytesValue("a")));
        try {
            store.putAll(entries);
            Assert.True(false, "Should have thrown NullPointerException while putAll null key");
        } catch (NullPointerException expected) {
        }
    }

    [Xunit.Fact]
    public void ShouldPutIfAbsent() {
        store.putIfAbsent(BytesKey("b"), BytesValue("2"));
        Assert.Equal(store.get(BytesKey("b")), (BytesValue("2")));

        store.putIfAbsent(BytesKey("b"), BytesValue("3"));
        Assert.Equal(store.get(BytesKey("b")), (BytesValue("2")));
    }

    [Xunit.Fact]
    public void ShouldPutAll() {
        List<KeyValuePair<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValuePair<>(BytesKey("a"), BytesValue("1")));
        entries.add(new KeyValuePair<>(BytesKey("b"), BytesValue("2")));
        store.putAll(entries);
        Assert.Equal(store.get(BytesKey("a")), (BytesValue("1")));
        Assert.Equal(store.get(BytesKey("b")), (BytesValue("2")));
    }

    [Xunit.Fact]
    public void ShouldReturnUnderlying() {
        Assert.Equal(underlyingStore, store.wrapped());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToDeleteFromClosedCachingStore() {
        store.close();
        store.delete(BytesKey("key"));
    }

    private int AddItemsToCache() {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < maxCacheSizeBytes) {
            string kv = string.valueOf(i++);
            store.put(BytesKey(kv), BytesValue(kv));
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

        
        public void Apply(byte[] key,
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
