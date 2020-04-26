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
//            store.SetFlushListener(cacheFlushListener, false);
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
//            Assert.True(store.SetFlushListener(null, true));
//            Assert.True(store.SetFlushListener(null, false));
//        }

//        [Fact]
//        public void ShouldAvoidFlushingDeletionsWithoutDirtyKeys()
//        {
//            int added = AddItemsToCache();
//            // All dirty entries should have been flushed
//            Assert.Equal(added, underlyingStore.approximateNumEntries);
//            Assert.Equal(added, cacheFlushListener.forwarded.Count);

//            store.Put(BytesKey("key"), BytesValue("value"));
//            Assert.Equal(added, underlyingStore.approximateNumEntries);
//            Assert.Equal(added, cacheFlushListener.forwarded.Count);

//            store.Put(BytesKey("key"), null);
//            store.Flush();
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
//                cache.Flush("0_0-store");
//                EasyMock.expectLastCall().andThrow(new NullReferenceException("Simulating an error on Flush"));
//                EasyMock.replay(cache);
//                store.Close();
//            }
//            catch (NullReferenceException npe)
//            {
//                Assert.False(underlyingStore.IsOpen());
//            }
//        }

//        [Fact]
//        public void ShouldPutGetToFromCache()
//        {
//            store.Put(BytesKey("key"), BytesValue("value"));
//            store.Put(BytesKey("key2"), BytesValue("value2"));
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
//            // All dirty entries should have been flushed
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
//            store.Put(BytesKey("1"), BytesValue("a"));
//            store.Flush();
//            Assert.Equal("a", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//        }

//        [Fact]
//        public void ShouldForwardOldValuesWhenEnabled()
//        {
//            store.SetFlushListener(cacheFlushListener, true);
//            store.Put(BytesKey("1"), BytesValue("a"));
//            store.Flush();
//            Assert.Equal("a", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//            store.Put(BytesKey("1"), BytesValue("b"));
//            store.Put(BytesKey("1"), BytesValue("c"));
//            store.Flush();
//            Assert.Equal("c", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Equal("a", cacheFlushListener.forwarded.Get("1").oldValue);
//            store.Put(BytesKey("1"), null);
//            store.Flush();
//            Assert.Null(cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Equal("c", cacheFlushListener.forwarded.Get("1").oldValue);
//            cacheFlushListener.forwarded.Clear();
//            store.Put(BytesKey("1"), BytesValue("a"));
//            store.Put(BytesKey("1"), BytesValue("b"));
//            store.Put(BytesKey("1"), null);
//            store.Flush();
//            Assert.Null(cacheFlushListener.forwarded.Get("1"));
//            cacheFlushListener.forwarded.Clear();
//        }

//        [Fact]
//        public void ShouldNotForwardOldValuesWhenDisabled()
//        {
//            store.Put(BytesKey("1"), BytesValue("a"));
//            store.Flush();
//            Assert.Equal("a", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//            store.Put(BytesKey("1"), BytesValue("b"));
//            store.Flush();
//            Assert.Equal("b", cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//            store.Put(BytesKey("1"), null);
//            store.Flush();
//            Assert.Null(cacheFlushListener.forwarded.Get("1").newValue);
//            Assert.Null(cacheFlushListener.forwarded.Get("1").oldValue);
//            cacheFlushListener.forwarded.Clear();
//            store.Put(BytesKey("1"), BytesValue("a"));
//            store.Put(BytesKey("1"), BytesValue("b"));
//            store.Put(BytesKey("1"), null);
//            store.Flush();
//            Assert.Null(cacheFlushListener.forwarded.Get("1"));
//            cacheFlushListener.forwarded.Clear();
//        }

//        [Fact]
//        public void ShouldIterateAllStoredItems()
//        {
//            int items = AddItemsToCache();
//            IKeyValueIterator<Bytes, byte[]> All = store.All();
//            List<Bytes> results = new List<Bytes>();
//            while (All.MoveNext())
//            {
//                results.Add(All.MoveNext().Key);
//            }
//            Assert.Equal(items, results.Count);
//        }

//        [Fact]
//        public void ShouldIterateOverRange()
//        {
//            int items = AddItemsToCache();
//            IKeyValueIterator<Bytes, byte[]> range = store.Range(bytesKey(string.valueOf(0)), bytesKey(string.valueOf(items)));
//            List<Bytes> results = new List<Bytes>();
//            while (range.MoveNext())
//            {
//                results.Add(range.MoveNext().Key);
//            }
//            Assert.Equal(items, results.Count);
//        }

//        [Fact]
//        public void ShouldDeleteItemsFromCache()
//        {
//            store.Put(BytesKey("a"), BytesValue("a"));
//            store.Delete(BytesKey("a"));
//            Assert.Null(store.Get(BytesKey("a")));
//            Assert.False(store.Range(BytesKey("a"), BytesKey("b")).MoveNext());
//            Assert.False(store.All().MoveNext());
//        }

//        [Fact]
//        public void ShouldNotShowItemsDeletedFromCacheButFlushedToStoreBeforeDelete()
//        {
//            store.Put(BytesKey("a"), BytesValue("a"));
//            store.Flush();
//            store.Delete(BytesKey("a"));
//            Assert.Null(store.Get(BytesKey("a")));
//            Assert.False(store.Range(BytesKey("a"), BytesKey("b")).MoveNext());
//            Assert.False(store.All().MoveNext());
//        }

//        [Fact]
//        public void ShouldClearNamespaceCacheOnClose()
//        {
//            store.Put(BytesKey("a"), BytesValue("a"));
//            Assert.Equal(1, cache.Count);
//            store.Close();
//            Assert.Equal(0, cache.Count);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToGetFromClosedCachingStore()
//        {
//            store.Close();
//            store.Get(BytesKey("a"));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToWriteToClosedCachingStore()
//        {
//            store.Close();
//            store.Put(BytesKey("a"), BytesValue("a"));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoRangeQueryOnClosedCachingStore()
//        {
//            store.Close();
//            store.Range(BytesKey("a"), BytesKey("b"));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoAllQueryOnClosedCachingStore()
//        {
//            store.Close();
//            store.All();
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoGetApproxSizeOnClosedCachingStore()
//        {
//            store.Close();
//            store.approximateNumEntries;
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoPutAllClosedCachingStore()
//        {
//            store.Close();
//            store.PutAll(Collections.singletonList(KeyValuePair.Create(BytesKey("a"), BytesValue("a"))));
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowIfTryingToDoPutIfAbsentClosedCachingStore()
//        {
//            store.Close();
//            store.PutIfAbsent(BytesKey("b"), BytesValue("c"));
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNullPointerExceptionOnPutWithNullKey()
//        {
//            store.Put(null, BytesValue("c"));
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNullPointerExceptionOnPutIfAbsentWithNullKey()
//        {
//            store.PutIfAbsent(null, BytesValue("c"));
//        }

//        [Fact]
//        public void ShouldThrowNullPointerExceptionOnPutAllWithNullKey()
//        {
//            List<KeyValuePair<Bytes, byte[]>> entries = new List<KeyValuePair<Bytes, byte[]>>();
//            entries.Add(new KeyValuePair<Bytes, byte[]> (null, BytesValue("a")));
//            try
//            {
//                store.PutAll(entries);
//                Assert.True(false, "Should have thrown NullReferenceException while putAll null key");
//            }
//            catch (NullReferenceException expected)
//            {
//            }
//        }

//        [Fact]
//        public void ShouldPutIfAbsent()
//        {
//            store.PutIfAbsent(BytesKey("b"), BytesValue("2"));
//            Assert.Equal(store.Get(BytesKey("b")), (BytesValue("2")));

//            store.PutIfAbsent(BytesKey("b"), BytesValue("3"));
//            Assert.Equal(store.Get(BytesKey("b")), (BytesValue("2")));
//        }

//        [Fact]
//        public void ShouldPutAll()
//        {
//            List<KeyValuePair<Bytes, byte[]>> entries = new List<KeyValuePair<Bytes, byte[]>>();
//            entries.Add(new KeyValuePair<Bytes, byte[]>(BytesKey("a"), BytesValue("1")));
//            entries.Add(new KeyValuePair<Bytes, byte[]>(BytesKey("b"), BytesValue("2")));
//            store.PutAll(entries);
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
//            store.Close();
//            store.Delete(BytesKey("key"));
//        }

//        private int AddItemsToCache()
//        {
//            int cachedSize = 0;
//            int i = 0;
//            while (cachedSize < maxCacheSizeBytes)
//            {
//                string kv = string.valueOf(i++);
//                store.Put(BytesKey(kv), BytesValue(kv));
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
//                forwarded.Put(
//                    keyDeserializer.Deserialize(null, key),
//                    new Change<string>(
//                        valueDesializer.Deserialize(null, newValue),
//                        valueDesializer.Deserialize(null, oldValue)));
//            }
//        }
//    }
//}
