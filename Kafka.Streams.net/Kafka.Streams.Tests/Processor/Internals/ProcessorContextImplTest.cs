//using Kafka.Streams.KStream;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Sessions;
//using Kafka.Streams.State.TimeStamped;
//using Kafka.Streams.State.Window;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class ProcessorContextImplTest
//    {
//        private ProcessorContextImpl context;

//        private const string KEY = "key";
//        private const long VALUE = 42L;
//        private static ValueAndTimestamp<long> VALUE_AND_TIMESTAMP = ValueAndTimestamp.Make(42L, 21L);
//        private const string STORE_NAME = "underlying-store";

//        private bool flushExecuted;
//        private bool putExecuted;
//        private bool putWithTimestampExecuted;
//        private bool putIfAbsentExecuted;
//        private bool putAllExecuted;
//        private bool deleteExecuted;
//        private bool removeExecuted;

//        private IKeyValueIterator<string, long> rangeIter;
//        private IKeyValueIterator<string, ValueAndTimestamp<long>> timestampedRangeIter;
//        private IKeyValueIterator<string, long> allIter;
//        private IKeyValueIterator<string, ValueAndTimestamp<long>> timestampedAllIter;

//        private List<IKeyValueIterator<Windowed<string>, long>> iters = new ArrayList<>(7);
//        private List<IKeyValueIterator<Windowed<string>, ValueAndTimestamp<long>>> timestampedIters = new ArrayList<>(7);
//        private IWindowStoreIterator windowStoreIter;


//        public void Setup()
//        {
//            flushExecuted = false;
//            putExecuted = false;
//            putIfAbsentExecuted = false;
//            putAllExecuted = false;
//            deleteExecuted = false;
//            removeExecuted = false;

//            rangeIter = mock(IKeyValueIterator);
//            timestampedRangeIter = mock(IKeyValueIterator);
//            allIter = mock(IKeyValueIterator);
//            timestampedAllIter = mock(IKeyValueIterator);
//            windowStoreIter = mock(IWindowStoreIterator);

//            for (int i = 0; i < 7; i++)
//            {
//                iters.Add(i, mock(IKeyValueIterator));
//                timestampedIters.Add(i, mock(IKeyValueIterator));
//            }

//            StreamsConfig streamsConfig = mock(StreamsConfig);
//            expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("add-id");
//            expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
//            expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
//            replay(streamsConfig);

//            ProcessorStateManager stateManager = mock(ProcessorStateManager);

//            expect(stateManager.getGlobalStore("GlobalKeyValueStore")).andReturn(KeyValueStoreMock());
//            expect(stateManager.getGlobalStore("GlobalTimestampedKeyValueStore")).andReturn(TimestampedKeyValueStoreMock());
//            expect(stateManager.getGlobalStore("GlobalWindowStore")).andReturn(WindowStoreMock());
//            expect(stateManager.getGlobalStore("GlobalTimestampedWindowStore")).andReturn(TimestampedWindowStoreMock());
//            expect(stateManager.getGlobalStore("GlobalSessionStore")).andReturn(SessionStoreMock());
//            expect(stateManager.getGlobalStore(anyString())).andReturn(null);

//            expect(stateManager.getStore("LocalKeyValueStore")).andReturn(KeyValueStoreMock());
//            expect(stateManager.getStore("LocalTimestampedKeyValueStore")).andReturn(TimestampedKeyValueStoreMock());
//            expect(stateManager.getStore("LocalWindowStore")).andReturn(WindowStoreMock());
//            expect(stateManager.getStore("LocalTimestampedWindowStore")).andReturn(TimestampedWindowStoreMock());
//            expect(stateManager.getStore("LocalSessionStore")).andReturn(SessionStoreMock());

//            replay(stateManager);

//            context = new ProcessorContextImpl(
//                mock(TaskId),
//                mock(StreamTask),
//                streamsConfig,
//                mock(RecordCollector),
//                stateManager,
//                mock(StreamsMetricsImpl),
//                mock(ThreadCache)
//            );

//            context.setCurrentNode(new ProcessorNode<string, long>("fake", null,
//                new HashSet<>(asList(
//                    "LocalKeyValueStore",
//                    "LocalTimestampedKeyValueStore",
//                    "LocalWindowStore",
//                    "LocalTimestampedWindowStore",
//                    "LocalSessionStore"))));
//        }

//        [Xunit.Fact]
//        public void GlobalKeyValueStoreShouldBeReadOnly()
//        {
//            doTest("GlobalKeyValueStore", (Consumer<IKeyValueStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                checkThrowsUnsupportedOperation(store::flush, "flush()");
//                checkThrowsUnsupportedOperation(() => store.put("1", 1L), "put()");
//                checkThrowsUnsupportedOperation(() => store.putIfAbsent("1", 1L), "putIfAbsent()");
//                checkThrowsUnsupportedOperation(() => store.putAll(Collections.emptyList()), "putAll()");
//                checkThrowsUnsupportedOperation(() => store.delete("1"), "delete()");

//                Assert.Equal((long)VALUE, store.Get(KEY));
//                Assert.Equal(rangeIter, store.Range("one", "two"));
//                Assert.Equal(allIter, store.all());
//                Assert.Equal(VALUE, store.approximateNumEntries);
//            });
//        }

//        [Xunit.Fact]
//        public void GlobalTimestampedKeyValueStoreShouldBeReadOnly()
//        {
//            doTest("GlobalTimestampedKeyValueStore", (Consumer<ITimestampedKeyValueStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                checkThrowsUnsupportedOperation(store::flush, "flush()");
//                checkThrowsUnsupportedOperation(() => store.put("1", ValueAndTimestamp.Make(1L, 2L)), "put()");
//                checkThrowsUnsupportedOperation(() => store.putIfAbsent("1", ValueAndTimestamp.Make(1L, 2L)), "putIfAbsent()");
//                checkThrowsUnsupportedOperation(() => store.putAll(Collections.emptyList()), "putAll()");
//                checkThrowsUnsupportedOperation(() => store.delete("1"), "delete()");

//                Assert.Equal(VALUE_AND_TIMESTAMP, store.Get(KEY));
//                Assert.Equal(timestampedRangeIter, store.Range("one", "two"));
//                Assert.Equal(timestampedAllIter, store.all());
//                Assert.Equal(VALUE, store.approximateNumEntries);
//            });
//        }

//        [Xunit.Fact]
//        public void GlobalWindowStoreShouldBeReadOnly()
//        {
//            doTest("GlobalWindowStore", (Consumer<IWindowStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                checkThrowsUnsupportedOperation(store::flush, "flush()");
//                checkThrowsUnsupportedOperation(() => store.put("1", 1L, 1L), "put()");
//                checkThrowsUnsupportedOperation(() => store.put("1", 1L), "put()");

//                Assert.Equal(iters.Get(0), store.fetchAll(0L, 0L));
//                Assert.Equal(windowStoreIter, store.Fetch(KEY, 0L, 1L));
//                Assert.Equal(iters.Get(1), store.Fetch(KEY, KEY, 0L, 1L));
//                Assert.Equal((long)VALUE, store.Fetch(KEY, 1L));
//                Assert.Equal(iters.Get(2), store.all());
//            });
//        }

//        [Xunit.Fact]
//        public void GlobalTimestampedWindowStoreShouldBeReadOnly()
//        {
//            doTest("GlobalTimestampedWindowStore", (Consumer<ITimestampedWindowStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                checkThrowsUnsupportedOperation(store::flush, "flush()");
//                checkThrowsUnsupportedOperation(() => store.put("1", ValueAndTimestamp.Make(1L, 1L), 1L), "put() [with timestamp]");
//                checkThrowsUnsupportedOperation(() => store.put("1", ValueAndTimestamp.Make(1L, 1L)), "put() [no timestamp]");

//                Assert.Equal(timestampedIters.Get(0), store.fetchAll(0L, 0L));
//                Assert.Equal(windowStoreIter, store.Fetch(KEY, 0L, 1L));
//                Assert.Equal(timestampedIters.Get(1), store.Fetch(KEY, KEY, 0L, 1L));
//                Assert.Equal(VALUE_AND_TIMESTAMP, store.Fetch(KEY, 1L));
//                Assert.Equal(timestampedIters.Get(2), store.all());
//            });
//        }

//        [Xunit.Fact]
//        public void GlobalSessionStoreShouldBeReadOnly()
//        {
//            doTest("GlobalSessionStore", (Consumer<ISessionStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                checkThrowsUnsupportedOperation(store::flush, "flush()");
//                checkThrowsUnsupportedOperation(() => store.remove(null), "remove()");
//                checkThrowsUnsupportedOperation(() => store.put(null, null), "put()");

//                Assert.Equal(iters.Get(3), store.findSessions(KEY, 1L, 2L));
//                Assert.Equal(iters.Get(4), store.findSessions(KEY, KEY, 1L, 2L));
//                Assert.Equal(iters.Get(5), store.Fetch(KEY));
//                Assert.Equal(iters.Get(6), store.Fetch(KEY, KEY));
//            });
//        }

//        [Xunit.Fact]
//        public void LocalKeyValueStoreShouldNotAllowInitOrClose()
//        {
//            doTest("LocalKeyValueStore", (Consumer<IKeyValueStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                store.flush();
//                Assert.True(flushExecuted);

//                store.put("1", 1L);
//                Assert.True(putExecuted);

//                store.putIfAbsent("1", 1L);
//                Assert.True(putIfAbsentExecuted);

//                store.putAll(Collections.emptyList());
//                Assert.True(putAllExecuted);

//                store.delete("1");
//                Assert.True(deleteExecuted);

//                Assert.Equal((long)VALUE, store.Get(KEY));
//                Assert.Equal(rangeIter, store.Range("one", "two"));
//                Assert.Equal(allIter, store.all());
//                Assert.Equal(VALUE, store.approximateNumEntries);
//            });
//        }

//        [Xunit.Fact]
//        public void LocalTimestampedKeyValueStoreShouldNotAllowInitOrClose()
//        {
//            doTest("LocalTimestampedKeyValueStore", (Consumer<ITimestampedKeyValueStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                store.flush();
//                Assert.True(flushExecuted);

//                store.put("1", ValueAndTimestamp.Make(1L, 2L));
//                Assert.True(putExecuted);

//                store.putIfAbsent("1", ValueAndTimestamp.Make(1L, 2L));
//                Assert.True(putIfAbsentExecuted);

//                store.putAll(Collections.emptyList());
//                Assert.True(putAllExecuted);

//                store.delete("1");
//                Assert.True(deleteExecuted);

//                Assert.Equal(VALUE_AND_TIMESTAMP, store.Get(KEY));
//                Assert.Equal(timestampedRangeIter, store.Range("one", "two"));
//                Assert.Equal(timestampedAllIter, store.all());
//                Assert.Equal(VALUE, store.approximateNumEntries);
//            });
//        }

//        [Xunit.Fact]
//        public void LocalWindowStoreShouldNotAllowInitOrClose()
//        {
//            doTest("LocalWindowStore", (Consumer<IWindowStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                store.flush();
//                Assert.True(flushExecuted);

//                store.put("1", 1L);
//                Assert.True(putExecuted);

//                Assert.Equal(iters.Get(0), store.fetchAll(0L, 0L));
//                Assert.Equal(windowStoreIter, store.Fetch(KEY, 0L, 1L));
//                Assert.Equal(iters.Get(1), store.Fetch(KEY, KEY, 0L, 1L));
//                Assert.Equal((long)VALUE, store.Fetch(KEY, 1L));
//                Assert.Equal(iters.Get(2), store.all());
//            });
//        }

//        [Xunit.Fact]
//        public void LocalTimestampedWindowStoreShouldNotAllowInitOrClose()
//        {
//            doTest("LocalTimestampedWindowStore", (Consumer<ITimestampedWindowStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                store.flush();
//                Assert.True(flushExecuted);

//                store.put("1", ValueAndTimestamp.Make(1L, 1L));
//                Assert.True(putExecuted);

//                store.put("1", ValueAndTimestamp.Make(1L, 1L), 1L);
//                Assert.True(putWithTimestampExecuted);

//                Assert.Equal(timestampedIters.Get(0), store.fetchAll(0L, 0L));
//                Assert.Equal(windowStoreIter, store.Fetch(KEY, 0L, 1L));
//                Assert.Equal(timestampedIters.Get(1), store.Fetch(KEY, KEY, 0L, 1L));
//                Assert.Equal(VALUE_AND_TIMESTAMP, store.Fetch(KEY, 1L));
//                Assert.Equal(timestampedIters.Get(2), store.all());
//            });
//        }

//        [Xunit.Fact]
//        public void LocalSessionStoreShouldNotAllowInitOrClose()
//        {
//            doTest("LocalSessionStore", (Consumer<ISessionStore<string, long>>)store => {
//                verifyStoreCannotBeInitializedOrClosed(store);

//                store.flush();
//                Assert.True(flushExecuted);

//                store.remove(null);
//                Assert.True(removeExecuted);

//                store.put(null, null);
//                Assert.True(putExecuted);

//                Assert.Equal(iters.Get(3), store.findSessions(KEY, 1L, 2L));
//                Assert.Equal(iters.Get(4), store.findSessions(KEY, KEY, 1L, 2L));
//                Assert.Equal(iters.Get(5), store.Fetch(KEY));
//                Assert.Equal(iters.Get(6), store.Fetch(KEY, KEY));
//            });
//        }


//        private IKeyValueStore<string, long> KeyValueStoreMock()
//        {
//            IKeyValueStore<string, long> keyValueStoreMock = mock(IKeyValueStore);

//            initStateStoreMock(keyValueStoreMock);

//            expect(keyValueStoreMock.Get(KEY)).andReturn(VALUE);
//            expect(keyValueStoreMock.approximateNumEntries).andReturn(VALUE);

//            expect(keyValueStoreMock.Range("one", "two")).andReturn(rangeIter);
//            expect(keyValueStoreMock.all()).andReturn(allIter);


//            keyValueStoreMock.put(anyString(), anyLong());
//            expectLastCall().andAnswer(() =>
//            {
//                putExecuted = true;
//                return null;
//            });

//            keyValueStoreMock.putIfAbsent(anyString(), anyLong());
//            expectLastCall().andAnswer(() =>
//            {
//                putIfAbsentExecuted = true;
//                return null;
//            });

//            keyValueStoreMock.putAll(anyObject(List));
//            expectLastCall().andAnswer(() =>
//            {
//                putAllExecuted = true;
//                return null;
//            });

//            keyValueStoreMock.delete(anyString());
//            expectLastCall().andAnswer(() =>
//            {
//                deleteExecuted = true;
//                return null;
//            });

//            replay(keyValueStoreMock);

//            return keyValueStoreMock;
//        }


//        private ITimestampedKeyValueStore<string, long> TimestampedKeyValueStoreMock()
//        {
//            ITimestampedKeyValueStore<string, long> timestampedKeyValueStoreMock = mock(ITimestampedKeyValueStore);

//            initStateStoreMock(timestampedKeyValueStoreMock);

//            expect(timestampedKeyValueStoreMock.Get(KEY)).andReturn(VALUE_AND_TIMESTAMP);
//            expect(timestampedKeyValueStoreMock.approximateNumEntries).andReturn(VALUE);

//            expect(timestampedKeyValueStoreMock.Range("one", "two")).andReturn(timestampedRangeIter);
//            expect(timestampedKeyValueStoreMock.all()).andReturn(timestampedAllIter);


//            timestampedKeyValueStoreMock.put(anyString(), anyObject(ValueAndTimestamp));
//            expectLastCall().andAnswer(() =>
//            {
//                putExecuted = true;
//                return null;
//            });

//            timestampedKeyValueStoreMock.putIfAbsent(anyString(), anyObject(ValueAndTimestamp));
//            expectLastCall().andAnswer(() =>
//            {
//                putIfAbsentExecuted = true;
//                return null;
//            });

//            timestampedKeyValueStoreMock.putAll(anyObject(List));
//            expectLastCall().andAnswer(() =>
//            {
//                putAllExecuted = true;
//                return null;
//            });

//            timestampedKeyValueStoreMock.delete(anyString());
//            expectLastCall().andAnswer(() =>
//            {
//                deleteExecuted = true;
//                return null;
//            });

//            replay(timestampedKeyValueStoreMock);

//            return timestampedKeyValueStoreMock;
//        }

//        private IWindowStore<string, long> WindowStoreMock()
//        {
//            IWindowStore<string, long> windowStore = mock(IWindowStore);

//            initStateStoreMock(windowStore);

//            expect(windowStore.fetchAll(anyLong(), anyLong())).andReturn(iters.Get(0));
//            expect(windowStore.Fetch(anyString(), anyString(), anyLong(), anyLong())).andReturn(iters.Get(1));
//            expect(windowStore.Fetch(anyString(), anyLong(), anyLong())).andReturn(windowStoreIter);
//            expect(windowStore.Fetch(anyString(), anyLong())).andReturn(VALUE);
//            expect(windowStore.all()).andReturn(iters.Get(2));

//            windowStore.put(anyString(), anyLong());
//            expectLastCall().andAnswer(() =>
//            {
//                putExecuted = true;
//                return null;
//            });

//            replay(windowStore);

//            return windowStore;
//        }


//        private ITimestampedWindowStore<string, long> TimestampedWindowStoreMock()
//        {
//            ITimestampedWindowStore<string, long> windowStore = null; // mock(ITimestampedWindowStore);

//            initStateStoreMock(windowStore);

//            expect(windowStore.fetchAll(anyLong(), anyLong())).andReturn(timestampedIters.Get(0));
//            expect(windowStore.Fetch(anyString(), anyString(), anyLong(), anyLong())).andReturn(timestampedIters.Get(1));
//            expect(windowStore.Fetch(anyString(), anyLong(), anyLong())).andReturn(windowStoreIter);
//            expect(windowStore.Fetch(anyString(), anyLong())).andReturn(VALUE_AND_TIMESTAMP);
//            expect(windowStore.all()).andReturn(timestampedIters.Get(2));

//            windowStore.put(anyString(), anyObject(ValueAndTimestamp));
//            expectLastCall().andAnswer(() =>
//            {
//                putExecuted = true;
//                return null;
//            });

//            windowStore.put(anyString(), anyObject(ValueAndTimestamp), anyLong());
//            expectLastCall().andAnswer(() =>
//            {
//                putWithTimestampExecuted = true;
//                return null;
//            });

//            replay(windowStore);

//            return windowStore;
//        }


//        private ISessionStore<string, long> SessionStoreMock()
//        {
//            ISessionStore<string, long> sessionStore = mock(ISessionStore);

//            initStateStoreMock(sessionStore);

//            expect(sessionStore.findSessions(anyString(), anyLong(), anyLong())).andReturn(iters.Get(3));
//            expect(sessionStore.findSessions(anyString(), anyString(), anyLong(), anyLong())).andReturn(iters.Get(4));
//            expect(sessionStore.Fetch(anyString())).andReturn(iters.Get(5));
//            expect(sessionStore.Fetch(anyString(), anyString())).andReturn(iters.Get(6));

//            sessionStore.put(anyObject(Windowed), anyLong());
//            expectLastCall().andAnswer(() =>
//            {
//                putExecuted = true;
//                return null;
//            });

//            sessionStore.remove(anyObject(Windowed));
//            expectLastCall().andAnswer(() =>
//            {
//                removeExecuted = true;
//                return null;
//            });

//            replay(sessionStore);

//            return sessionStore;
//        }

//        private void InitStateStoreMock(IStateStore stateStore)
//        {
//            expect(stateStore.name()).andReturn(STORE_NAME);
//            expect(stateStore.persistent()).andReturn(true);
//            expect(stateStore.isOpen()).andReturn(true);

//            stateStore.flush();
//            expectLastCall().andAnswer(() =>
//            {
//                flushExecuted = true;
//                return null;
//            });
//        }

//        private void DoTest<IStateStore>(string name, Consumer<T> checker)
//        {
//            Processor processor = new Processor<string, long>()
//            {



//            public void init(ProcessorContext context)
//            {
//                T store = (T)context.getStateStore(name);
//                checker.accept(store);
//            }


//            public void process(string k, long v)
//            {
//                //No-op.
//            }


//            public void close()
//            {
//                //No-op.
//            }
//        };

//        processor.Init(context);
//    }

//    private void VerifyStoreCannotBeInitializedOrClosed(IStateStore store)
//    {
//        Assert.Equal(STORE_NAME, store.name());
//        Assert.True(store.persistent());
//        Assert.True(store.isOpen());

//        checkThrowsUnsupportedOperation(() => store.Init(null, null), "init()");
//        checkThrowsUnsupportedOperation(store::close, "close()");
//    }

//    private void CheckThrowsUnsupportedOperation(Runnable check, string name)
//    {
//        try
//        {
//            check.run();
//            Assert.True(false, name + " should throw exception");
//        }
//        catch (UnsupportedOperationException e)
//        {
//            //ignore.
//        }
//    }
//}
