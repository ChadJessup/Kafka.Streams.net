//using Confluent.Kafka;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State.Internals;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class CachingKeyValueStoreTest : AbstractKeyValueStoreTest
//    {
//        private readonly int maxCacheSizeBytes = 150;
//        private InternalMockProcessorContext context;
//        private CachingKeyValueStore store;
//        private InMemoryKeyValueStore underlyingStore;
//        private ThreadCache cache;
//        private CacheFlushListenerStub<string, string> cacheFlushListener;
//        private string topic;


//        public void SetUp()
//        {
//            string storeName = "store";
//            underlyingStore = new InMemoryKeyValueStore(storeName);
//            cacheFlushListener = new CacheFlushListenerStub<>(new Serdes.String().Deserializer(), new Serdes.String().Deserializer());
//            store = new CachingKeyValueStore(underlyingStore);
//            store.setFlushListener(cacheFlushListener, false);
//            cache = new ThreadCache(new LogContext("testCache "), maxCacheSizeBytes, new MockStreamsMetrics(new Metrics()));
//            context = new InternalMockProcessorContext(null, null, null, null, cache);
//            topic = "topic";
//            context.setRecordContext(new ProcessorRecordContext(10, 0, 0, topic, null));
//            store.Init(context, null);
//        }


//        public void After()
//        {
//            base.After();
//        }



//        protected IKeyValueStore<K, V> CreateKeyValueStore<K, V>(ProcessorContext context)
//        {
//            IStoreBuilder storeBuilder = Stores.KeyValueStoreBuilder(
//                    Stores.PersistentKeyValueStore("cache-store"),
//                    (Serde<K>)context.keySerde(),
//                    (Serde<V>)context.valueSerde())
//                    .withCachingEnabled();

//            IKeyValueStore<K, V> store = (IKeyValueStore<K, V>)storeBuilder.Build();
//            store.Init(context, store);
//            return store;
//        }

//        [Fact]
//        public void ShouldSetFlushListener()
//        {
//            Assert.True(store.setFlushListener(null, true));
//            Assert.True(store.setFlushListener(null, false));
//        }

//        [Fact]
//        public void ShouldAvoidFlushingDeletionsWithoutDirtyKeys()
//        {
//            int added = AddItemsToCache();
//            // all dirty entries should have been flushed
//            Assert.Equal(added, underlyingStore.approximateNumEntries);
//            Assert.Equal(added, cacheFlushListener.forwarded.Count);

//            store.put(BytesKey("key"), BytesValue("value"));
//            Assert.Equal(added, underlyingStore.approximateNumEntries);
//            Assert.Equal(added, cacheFlushListener.forwarded.Count);

//            store.put(BytesKey("key"), null);
//            store.flush();
//            Assert.Equal(added, underlyingStore.approximateNumEntries);
//            Assert.Equal(added, cacheFlushListener.forwarded.Count);
//        }

//        [Fact]
//        public void ShouldCloseAfterErrorWithFlush()
//        {
//            try
//            {
//                cache = EasyMock.niceMock(ThreadCache);
//                context = new InternalMockProcessorContext(null, null, null, null, cache);
//                context.setRecordContext(new ProcessorRecordContext(10, 0, 0, topic, null));
//                store.Init(context, null);
//                cache.flush("0_0-store");
//                EasyMock.expectLastCall().andThrow(new NullPointerException("Simulating an error on flush"));
//                EasyMock.replay(cache);
//                store.close();
//            }
//            catch (NullPointerException npe)
//            {
//                Assert.False(underlyingStore.isOpen());
//            }
//        }

//        [Fact]
//        public void ShouldPutGetToFromCache()
//        {
//            store.put(BytesKey("key"), BytesValue("value"));
//            store.put(BytesKey("key2"), BytesValue("value2"));
//            Assert.Equal(store.Get(BytesKey("key")), (BytesValue("value")));
//            Assert.Equal(store.Get(BytesKey("key2")), (BytesValue("value2")));
//            // nothing evicted so underlying store should be empty
//            Assert.Equal(2, cache.Count);
//            Assert.Equal(0, underlyingStore.approximateNumEntries);
//        }

//        private byte[] BytesValue(string value)
//        {
//            return value.getBytes();
//        }

//        private Bytes BytesKey(string key)
//        {
//            return Bytes.Wrap(key.getBytes());
//        }

//        [Fact]
//        public void ShouldFlushEvictedItemsIntoUnderlyingStore()
//        {
//            int added = AddItemsToCache();
//            // all dirty entries should have been flushed
//            Assert.Equal(added, underlyingStore.approximateNumEntries);
//            Assert.Equal(added, store.approximateNumEntries);
//            Assert.NotNull(underlyingStore.Get(Bytes.Wrap("0".getBytes())));
//        }

//        [Fact]
//        public void ShouldForwardDirtyItemToListenerWhenEvicted()
//        {
//            int numRecords = AddItemsToCache();
//            Assert.Equal(numRecords, cacheFlushListener.forwarded.Count);
//        }

//        [Fact]
//        public void ShouldForwardDirtyItemsWhenFlushCalled()
//        {
//            store.put(BytesKey("1"), BytesValue("a"));
//            store.flush();
//            Assert.Equal("a", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//        }

//        [Fact]
//        public void ShouldForwardOldValuesWhenEnabled()
//        {
//            store.setFlushListener(cacheFlushListener, true);
//            store.put(BytesKey("1"), BytesValue("a"));
//            store.flush();
//            Assert.Equal("a", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//            store.put(BytesKey("1"), BytesValue("b"));
//            store.put(BytesKey("1"), BytesValue("c"));
//            store.flush();
//            Assert.Equal("c", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Equal("a", cacheFlushListener.forwarded.Get("1").oldValue);
//            store.put(BytesKey("1"), null);
//            store.flush();
//            Assert.Null(cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Equal("c", cacheFlushListener.forwarded.Get("1").oldValue);
//            cacheFlushListener.forwarded.Clear();
//            store.put(BytesKey("1"), BytesValue("a"));
//            store.put(BytesKey("1"), BytesValue("b"));
//            store.put(BytesKey("1"), null);
//            store.flush();
//            Assert.Null(cacheFlushListener.forwarded.Get("1"));
//            cacheFlushListener.forwarded.Clear();
//        }

//        [Fact]
//        public void ShouldNotForwardOldValuesWhenDisabled()
//        {
//            store.put(BytesKey("1"), BytesValue("a"));
//            store.flush();
//            Assert.Equal("a", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//            store.put(BytesKey("1"), BytesValue("b"));
//            store.flush();
//            Assert.Equal("b", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//            store.put(BytesKey("1"), null);
//            store.flush();
//            Assert.Null(cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//            cacheFlushListener.forwarded.Clear();
//            store.put(BytesKey("1"), BytesValue("a"));
//            store.put(BytesKey("1"), BytesValue("b"));
//            store.put(BytesKey("1"), null);
//            store.flush();
//            Assert.Null(cacheFlushListener.forwarded.Get("1"));
//            cacheFlushListener.forwarded.Clear();
//        }

//        [Fact]
//        public void ShouldIterateAllStoredItems()
//        {
//            int items = AddItemsToCache();
//            IKeyValueIterator<Bytes, byte[]> all = store.all();
//            List<Bytes> results = new ArrayList<>();
//            while (all.HasNext())
//            {
//                results.Add(all.MoveNext().key);
//            }
//            Assert.Equal(items, results.Count);
//        }

//        [Fact]
//        public void ShouldIterateOverRange()
//        {
//            int items = AddItemsToCache();
//            IKeyValueIterator<Bytes, byte[]> range = store.Range(bytesKey(string.valueOf(0)), bytesKey(string.valueOf(items)));
//            List<Bytes> results = new ArrayList<>();
//            while (range.HasNext())
//            {
//                results.Add(range.MoveNext().key);
//            }
//            Assert.Equal(items, results.Count);
//        }

//        [Fact]
//        public void ShouldDeleteItemsFromCache()
//        {
//            store.put(BytesKey("a"), BytesValue("a"));
//            store.delete(BytesKey("a"));
//            Assert.Null(store.Get(BytesKey("a")));
//            Assert.False(store.Range(BytesKey("a"), BytesKey("b")).HasNext());
//            Assert.False(store.all().HasNext());
//        }

//        [Fact]
//        public void ShouldNotShowItemsDeletedFromCacheButFlushedToStoreBeforeDelete()
//        {
//            store.put(BytesKey("a"), BytesValue("a"));
//            store.flush();
//            store.delete(BytesKey("a"));
//            Assert.Null(store.Get(BytesKey("a")));
//            Assert.False(store.Range(BytesKey("a"), BytesKey("b")).HasNext());
//            Assert.False(store.all().HasNext());
//        }

//        [Fact]
//        public void ShouldClearNamespaceCacheOnClose()
//        {
//            store.put(BytesKey("a"), BytesValue("a"));
//            Assert.Equal(1, cache.Count);
//            store.close();
//            Assert.Equal(0, cache.Count);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToGetFromClosedCachingStore()
//        {
//            store.close();
//            store.Get(BytesKey("a"));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToWriteToClosedCachingStore()
//        {
//            store.close();
//            store.put(BytesKey("a"), BytesValue("a"));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoRangeQueryOnClosedCachingStore()
//        {
//            store.close();
//            store.Range(BytesKey("a"), BytesKey("b"));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoAllQueryOnClosedCachingStore()
//        {
//            store.close();
//            store.all();
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoGetApproxSizeOnClosedCachingStore()
//        {
//            store.close();
//            store.approximateNumEntries;
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoPutAllClosedCachingStore()
//        {
//            store.close();
//            store.putAll(Collections.singletonList(KeyValuePair.Create(BytesKey("a"), BytesValue("a"))));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoPutIfAbsentClosedCachingStore()
//        {
//            store.close();
//            store.putIfAbsent(BytesKey("b"), BytesValue("c"));
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnPutWithNullKey()
//        {
//            store.put(null, BytesValue("c"));
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnPutIfAbsentWithNullKey()
//        {
//            store.putIfAbsent(null, BytesValue("c"));
//        }

//        [Fact]
//        public void ShouldThrowNullPointerExceptionOnPutAllWithNullKey()
//        {
//            List<KeyValuePair<Bytes, byte[]>> entries = new List<KeyValuePair<Bytes, byte[]>>();
//            entries.Add(new KeyValuePair<Bytes, byte[]> (null, BytesValue("a")));
//            try
//            {
//                store.putAll(entries);
//                Assert.True(false, "Should have thrown NullPointerException while putAll null key");
//            }
//            catch (NullReferenceException expected)
//            {
//            }
//        }

//        [Fact]
//        public void ShouldPutIfAbsent()
//        {
//            store.putIfAbsent(BytesKey("b"), BytesValue("2"));
//            Assert.Equal(store.Get(BytesKey("b")), (BytesValue("2")));

//            store.putIfAbsent(BytesKey("b"), BytesValue("3"));
//            Assert.Equal(store.Get(BytesKey("b")), (BytesValue("2")));
//        }

//        [Fact]
//        public void ShouldPutAll()
//        {
//            List<KeyValuePair<Bytes, byte[]>> entries = new ArrayList<>();
//            entries.Add(new KeyValuePair<Bytes, byte[]>(BytesKey("a"), BytesValue("1")));
//            entries.Add(new KeyValuePair<Bytes, byte[]>(BytesKey("b"), BytesValue("2")));
//            store.putAll(entries);
//            Assert.Equal(store.Get(BytesKey("a")), (BytesValue("1")));
//            Assert.Equal(store.Get(BytesKey("b")), (BytesValue("2")));
//        }

//        [Fact]
//        public void ShouldReturnUnderlying()
//        {
//            Assert.Equal(underlyingStore, store.wrapped());
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDeleteFromClosedCachingStore()
//        {
//            store.close();
//            store.delete(BytesKey("key"));
//        }

//        private int AddItemsToCache()
//        {
//            int cachedSize = 0;
//            int i = 0;
//            while (cachedSize < maxCacheSizeBytes)
//            {
//                string kv = string.valueOf(i++);
//                store.put(BytesKey(kv), BytesValue(kv));
//                cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic);
//            }
//            return i;
//        }

//        public class CacheFlushListenerStub<K, V> : CacheFlushListener<byte[], byte[]>
//        {
//            IDeserializer<K> keyDeserializer;
//            IDeserializer<V> valueDesializer;
//            Dictionary<K, Change<V>> forwarded = new HashMap<>();

//            CacheFlushListenerStub(IDeserializer<K> keyDeserializer,
//                                   IDeserializer<V> valueDesializer)
//            {
//                this.keyDeserializer = keyDeserializer;
//                this.valueDesializer = valueDesializer;
//            }


//            public void Apply(byte[] key,
//                              byte[] newValue,
//                              byte[] oldValue,
//                              long timestamp)
//            {
//                forwarded.put(
//                    keyDeserializer.deserialize(null, key),
//                    new Change<>(
//                        valueDesializer.deserialize(null, newValue),
//                        valueDesializer.deserialize(null, oldValue)));
//            }
//        }
//    }
//}
