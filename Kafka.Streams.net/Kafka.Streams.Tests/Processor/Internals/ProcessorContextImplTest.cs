namespace Kafka.Streams.Tests.Processor.Internals
{
    /*






    *

    *





    */








































    public class ProcessorContextImplTest
    {
        private ProcessorContextImpl context;

        private static readonly string KEY = "key";
        private static readonly long VALUE = 42L;
        private static ValueAndTimestamp<long> VALUE_AND_TIMESTAMP = ValueAndTimestamp.make(42L, 21L);
        private static readonly string STORE_NAME = "underlying-store";

        private bool flushExecuted;
        private bool putExecuted;
        private bool putWithTimestampExecuted;
        private bool putIfAbsentExecuted;
        private bool putAllExecuted;
        private bool deleteExecuted;
        private bool removeExecuted;

        private KeyValueIterator<string, long> rangeIter;
        private KeyValueIterator<string, ValueAndTimestamp<long>> timestampedRangeIter;
        private KeyValueIterator<string, long> allIter;
        private KeyValueIterator<string, ValueAndTimestamp<long>> timestampedAllIter;

        private List<KeyValueIterator<Windowed<string>, long>> iters = new ArrayList<>(7);
        private List<KeyValueIterator<Windowed<string>, ValueAndTimestamp<long>>> timestampedIters = new ArrayList<>(7);
        private WindowStoreIterator windowStoreIter;


        public void Setup()
        {
            flushExecuted = false;
            putExecuted = false;
            putIfAbsentExecuted = false;
            putAllExecuted = false;
            deleteExecuted = false;
            removeExecuted = false;

            rangeIter = mock(KeyValueIterator);
            timestampedRangeIter = mock(KeyValueIterator);
            allIter = mock(KeyValueIterator);
            timestampedAllIter = mock(KeyValueIterator);
            windowStoreIter = mock(WindowStoreIterator);

            for (int i = 0; i < 7; i++)
            {
                iters.add(i, mock(KeyValueIterator));
                timestampedIters.add(i, mock(KeyValueIterator));
            }

            StreamsConfig streamsConfig = mock(StreamsConfig);
            expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("add-id");
            expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
            expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
            replay(streamsConfig);

            ProcessorStateManager stateManager = mock(ProcessorStateManager);

            expect(stateManager.getGlobalStore("GlobalKeyValueStore")).andReturn(KeyValueStoreMock());
            expect(stateManager.getGlobalStore("GlobalTimestampedKeyValueStore")).andReturn(TimestampedKeyValueStoreMock());
            expect(stateManager.getGlobalStore("GlobalWindowStore")).andReturn(WindowStoreMock());
            expect(stateManager.getGlobalStore("GlobalTimestampedWindowStore")).andReturn(TimestampedWindowStoreMock());
            expect(stateManager.getGlobalStore("GlobalSessionStore")).andReturn(SessionStoreMock());
            expect(stateManager.getGlobalStore(anyString())).andReturn(null);

            expect(stateManager.getStore("LocalKeyValueStore")).andReturn(KeyValueStoreMock());
            expect(stateManager.getStore("LocalTimestampedKeyValueStore")).andReturn(TimestampedKeyValueStoreMock());
            expect(stateManager.getStore("LocalWindowStore")).andReturn(WindowStoreMock());
            expect(stateManager.getStore("LocalTimestampedWindowStore")).andReturn(TimestampedWindowStoreMock());
            expect(stateManager.getStore("LocalSessionStore")).andReturn(SessionStoreMock());

            replay(stateManager);

            context = new ProcessorContextImpl(
                mock(TaskId),
                mock(StreamTask),
                streamsConfig,
                mock(RecordCollector),
                stateManager,
                mock(StreamsMetricsImpl),
                mock(ThreadCache)
            );

            context.setCurrentNode(new ProcessorNode<string, long>("fake", null,
                new HashSet<>(asList(
                    "LocalKeyValueStore",
                    "LocalTimestampedKeyValueStore",
                    "LocalWindowStore",
                    "LocalTimestampedWindowStore",
                    "LocalSessionStore"))));
        }

        [Xunit.Fact]
        public void GlobalKeyValueStoreShouldBeReadOnly()
        {
            doTest("GlobalKeyValueStore", (Consumer<KeyValueStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                checkThrowsUnsupportedOperation(store::flush, "flush()");
                checkThrowsUnsupportedOperation(() => store.put("1", 1L), "put()");
                checkThrowsUnsupportedOperation(() => store.putIfAbsent("1", 1L), "putIfAbsent()");
                checkThrowsUnsupportedOperation(() => store.putAll(Collections.emptyList()), "putAll()");
                checkThrowsUnsupportedOperation(() => store.delete("1"), "delete()");

                Assert.Equal((long)VALUE, store.get(KEY));
                Assert.Equal(rangeIter, store.range("one", "two"));
                Assert.Equal(allIter, store.all());
                Assert.Equal(VALUE, store.approximateNumEntries());
            });
        }

        [Xunit.Fact]
        public void GlobalTimestampedKeyValueStoreShouldBeReadOnly()
        {
            doTest("GlobalTimestampedKeyValueStore", (Consumer<TimestampedKeyValueStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                checkThrowsUnsupportedOperation(store::flush, "flush()");
                checkThrowsUnsupportedOperation(() => store.put("1", ValueAndTimestamp.make(1L, 2L)), "put()");
                checkThrowsUnsupportedOperation(() => store.putIfAbsent("1", ValueAndTimestamp.make(1L, 2L)), "putIfAbsent()");
                checkThrowsUnsupportedOperation(() => store.putAll(Collections.emptyList()), "putAll()");
                checkThrowsUnsupportedOperation(() => store.delete("1"), "delete()");

                Assert.Equal(VALUE_AND_TIMESTAMP, store.get(KEY));
                Assert.Equal(timestampedRangeIter, store.range("one", "two"));
                Assert.Equal(timestampedAllIter, store.all());
                Assert.Equal(VALUE, store.approximateNumEntries());
            });
        }

        [Xunit.Fact]
        public void GlobalWindowStoreShouldBeReadOnly()
        {
            doTest("GlobalWindowStore", (Consumer<WindowStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                checkThrowsUnsupportedOperation(store::flush, "flush()");
                checkThrowsUnsupportedOperation(() => store.put("1", 1L, 1L), "put()");
                checkThrowsUnsupportedOperation(() => store.put("1", 1L), "put()");

                Assert.Equal(iters.get(0), store.fetchAll(0L, 0L));
                Assert.Equal(windowStoreIter, store.fetch(KEY, 0L, 1L));
                Assert.Equal(iters.get(1), store.fetch(KEY, KEY, 0L, 1L));
                Assert.Equal((long)VALUE, store.fetch(KEY, 1L));
                Assert.Equal(iters.get(2), store.all());
            });
        }

        [Xunit.Fact]
        public void GlobalTimestampedWindowStoreShouldBeReadOnly()
        {
            doTest("GlobalTimestampedWindowStore", (Consumer<TimestampedWindowStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                checkThrowsUnsupportedOperation(store::flush, "flush()");
                checkThrowsUnsupportedOperation(() => store.put("1", ValueAndTimestamp.make(1L, 1L), 1L), "put() [with timestamp]");
                checkThrowsUnsupportedOperation(() => store.put("1", ValueAndTimestamp.make(1L, 1L)), "put() [no timestamp]");

                Assert.Equal(timestampedIters.get(0), store.fetchAll(0L, 0L));
                Assert.Equal(windowStoreIter, store.fetch(KEY, 0L, 1L));
                Assert.Equal(timestampedIters.get(1), store.fetch(KEY, KEY, 0L, 1L));
                Assert.Equal(VALUE_AND_TIMESTAMP, store.fetch(KEY, 1L));
                Assert.Equal(timestampedIters.get(2), store.all());
            });
        }

        [Xunit.Fact]
        public void GlobalSessionStoreShouldBeReadOnly()
        {
            doTest("GlobalSessionStore", (Consumer<SessionStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                checkThrowsUnsupportedOperation(store::flush, "flush()");
                checkThrowsUnsupportedOperation(() => store.remove(null), "remove()");
                checkThrowsUnsupportedOperation(() => store.put(null, null), "put()");

                Assert.Equal(iters.get(3), store.findSessions(KEY, 1L, 2L));
                Assert.Equal(iters.get(4), store.findSessions(KEY, KEY, 1L, 2L));
                Assert.Equal(iters.get(5), store.fetch(KEY));
                Assert.Equal(iters.get(6), store.fetch(KEY, KEY));
            });
        }

        [Xunit.Fact]
        public void LocalKeyValueStoreShouldNotAllowInitOrClose()
        {
            doTest("LocalKeyValueStore", (Consumer<KeyValueStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                store.flush();
                Assert.True(flushExecuted);

                store.put("1", 1L);
                Assert.True(putExecuted);

                store.putIfAbsent("1", 1L);
                Assert.True(putIfAbsentExecuted);

                store.putAll(Collections.emptyList());
                Assert.True(putAllExecuted);

                store.delete("1");
                Assert.True(deleteExecuted);

                Assert.Equal((long)VALUE, store.get(KEY));
                Assert.Equal(rangeIter, store.range("one", "two"));
                Assert.Equal(allIter, store.all());
                Assert.Equal(VALUE, store.approximateNumEntries());
            });
        }

        [Xunit.Fact]
        public void LocalTimestampedKeyValueStoreShouldNotAllowInitOrClose()
        {
            doTest("LocalTimestampedKeyValueStore", (Consumer<TimestampedKeyValueStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                store.flush();
                Assert.True(flushExecuted);

                store.put("1", ValueAndTimestamp.make(1L, 2L));
                Assert.True(putExecuted);

                store.putIfAbsent("1", ValueAndTimestamp.make(1L, 2L));
                Assert.True(putIfAbsentExecuted);

                store.putAll(Collections.emptyList());
                Assert.True(putAllExecuted);

                store.delete("1");
                Assert.True(deleteExecuted);

                Assert.Equal(VALUE_AND_TIMESTAMP, store.get(KEY));
                Assert.Equal(timestampedRangeIter, store.range("one", "two"));
                Assert.Equal(timestampedAllIter, store.all());
                Assert.Equal(VALUE, store.approximateNumEntries());
            });
        }

        [Xunit.Fact]
        public void LocalWindowStoreShouldNotAllowInitOrClose()
        {
            doTest("LocalWindowStore", (Consumer<WindowStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                store.flush();
                Assert.True(flushExecuted);

                store.put("1", 1L);
                Assert.True(putExecuted);

                Assert.Equal(iters.get(0), store.fetchAll(0L, 0L));
                Assert.Equal(windowStoreIter, store.fetch(KEY, 0L, 1L));
                Assert.Equal(iters.get(1), store.fetch(KEY, KEY, 0L, 1L));
                Assert.Equal((long)VALUE, store.fetch(KEY, 1L));
                Assert.Equal(iters.get(2), store.all());
            });
        }

        [Xunit.Fact]
        public void LocalTimestampedWindowStoreShouldNotAllowInitOrClose()
        {
            doTest("LocalTimestampedWindowStore", (Consumer<TimestampedWindowStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                store.flush();
                Assert.True(flushExecuted);

                store.put("1", ValueAndTimestamp.make(1L, 1L));
                Assert.True(putExecuted);

                store.put("1", ValueAndTimestamp.make(1L, 1L), 1L);
                Assert.True(putWithTimestampExecuted);

                Assert.Equal(timestampedIters.get(0), store.fetchAll(0L, 0L));
                Assert.Equal(windowStoreIter, store.fetch(KEY, 0L, 1L));
                Assert.Equal(timestampedIters.get(1), store.fetch(KEY, KEY, 0L, 1L));
                Assert.Equal(VALUE_AND_TIMESTAMP, store.fetch(KEY, 1L));
                Assert.Equal(timestampedIters.get(2), store.all());
            });
        }

        [Xunit.Fact]
        public void LocalSessionStoreShouldNotAllowInitOrClose()
        {
            doTest("LocalSessionStore", (Consumer<SessionStore<string, long>>)store => {
                verifyStoreCannotBeInitializedOrClosed(store);

                store.flush();
                Assert.True(flushExecuted);

                store.remove(null);
                Assert.True(removeExecuted);

                store.put(null, null);
                Assert.True(putExecuted);

                Assert.Equal(iters.get(3), store.findSessions(KEY, 1L, 2L));
                Assert.Equal(iters.get(4), store.findSessions(KEY, KEY, 1L, 2L));
                Assert.Equal(iters.get(5), store.fetch(KEY));
                Assert.Equal(iters.get(6), store.fetch(KEY, KEY));
            });
        }


        private KeyValueStore<string, long> KeyValueStoreMock()
        {
            KeyValueStore<string, long> keyValueStoreMock = mock(KeyValueStore);

            initStateStoreMock(keyValueStoreMock);

            expect(keyValueStoreMock.get(KEY)).andReturn(VALUE);
            expect(keyValueStoreMock.approximateNumEntries()).andReturn(VALUE);

            expect(keyValueStoreMock.range("one", "two")).andReturn(rangeIter);
            expect(keyValueStoreMock.all()).andReturn(allIter);


            keyValueStoreMock.put(anyString(), anyLong());
            expectLastCall().andAnswer(() =>
            {
                putExecuted = true;
                return null;
            });

            keyValueStoreMock.putIfAbsent(anyString(), anyLong());
            expectLastCall().andAnswer(() =>
            {
                putIfAbsentExecuted = true;
                return null;
            });

            keyValueStoreMock.putAll(anyObject(List));
            expectLastCall().andAnswer(() =>
            {
                putAllExecuted = true;
                return null;
            });

            keyValueStoreMock.delete(anyString());
            expectLastCall().andAnswer(() =>
            {
                deleteExecuted = true;
                return null;
            });

            replay(keyValueStoreMock);

            return keyValueStoreMock;
        }


        private TimestampedKeyValueStore<string, long> TimestampedKeyValueStoreMock()
        {
            TimestampedKeyValueStore<string, long> timestampedKeyValueStoreMock = mock(TimestampedKeyValueStore);

            initStateStoreMock(timestampedKeyValueStoreMock);

            expect(timestampedKeyValueStoreMock.get(KEY)).andReturn(VALUE_AND_TIMESTAMP);
            expect(timestampedKeyValueStoreMock.approximateNumEntries()).andReturn(VALUE);

            expect(timestampedKeyValueStoreMock.range("one", "two")).andReturn(timestampedRangeIter);
            expect(timestampedKeyValueStoreMock.all()).andReturn(timestampedAllIter);


            timestampedKeyValueStoreMock.put(anyString(), anyObject(ValueAndTimestamp));
            expectLastCall().andAnswer(() =>
            {
                putExecuted = true;
                return null;
            });

            timestampedKeyValueStoreMock.putIfAbsent(anyString(), anyObject(ValueAndTimestamp));
            expectLastCall().andAnswer(() =>
            {
                putIfAbsentExecuted = true;
                return null;
            });

            timestampedKeyValueStoreMock.putAll(anyObject(List));
            expectLastCall().andAnswer(() =>
            {
                putAllExecuted = true;
                return null;
            });

            timestampedKeyValueStoreMock.delete(anyString());
            expectLastCall().andAnswer(() =>
            {
                deleteExecuted = true;
                return null;
            });

            replay(timestampedKeyValueStoreMock);

            return timestampedKeyValueStoreMock;
        }


        private WindowStore<string, long> WindowStoreMock()
        {
            WindowStore<string, long> windowStore = mock(WindowStore);

            initStateStoreMock(windowStore);

            expect(windowStore.fetchAll(anyLong(), anyLong())).andReturn(iters.get(0));
            expect(windowStore.fetch(anyString(), anyString(), anyLong(), anyLong())).andReturn(iters.get(1));
            expect(windowStore.fetch(anyString(), anyLong(), anyLong())).andReturn(windowStoreIter);
            expect(windowStore.fetch(anyString(), anyLong())).andReturn(VALUE);
            expect(windowStore.all()).andReturn(iters.get(2));

            windowStore.put(anyString(), anyLong());
            expectLastCall().andAnswer(() =>
            {
                putExecuted = true;
                return null;
            });

            replay(windowStore);

            return windowStore;
        }


        private TimestampedWindowStore<string, long> TimestampedWindowStoreMock()
        {
            TimestampedWindowStore<string, long> windowStore = mock(TimestampedWindowStore);

            initStateStoreMock(windowStore);

            expect(windowStore.fetchAll(anyLong(), anyLong())).andReturn(timestampedIters.get(0));
            expect(windowStore.fetch(anyString(), anyString(), anyLong(), anyLong())).andReturn(timestampedIters.get(1));
            expect(windowStore.fetch(anyString(), anyLong(), anyLong())).andReturn(windowStoreIter);
            expect(windowStore.fetch(anyString(), anyLong())).andReturn(VALUE_AND_TIMESTAMP);
            expect(windowStore.all()).andReturn(timestampedIters.get(2));

            windowStore.put(anyString(), anyObject(ValueAndTimestamp));
            expectLastCall().andAnswer(() =>
            {
                putExecuted = true;
                return null;
            });

            windowStore.put(anyString(), anyObject(ValueAndTimestamp), anyLong());
            expectLastCall().andAnswer(() =>
            {
                putWithTimestampExecuted = true;
                return null;
            });

            replay(windowStore);

            return windowStore;
        }


        private SessionStore<string, long> SessionStoreMock()
        {
            SessionStore<string, long> sessionStore = mock(SessionStore);

            initStateStoreMock(sessionStore);

            expect(sessionStore.findSessions(anyString(), anyLong(), anyLong())).andReturn(iters.get(3));
            expect(sessionStore.findSessions(anyString(), anyString(), anyLong(), anyLong())).andReturn(iters.get(4));
            expect(sessionStore.fetch(anyString())).andReturn(iters.get(5));
            expect(sessionStore.fetch(anyString(), anyString())).andReturn(iters.get(6));

            sessionStore.put(anyObject(Windowed), anyLong());
            expectLastCall().andAnswer(() =>
            {
                putExecuted = true;
                return null;
            });

            sessionStore.remove(anyObject(Windowed));
            expectLastCall().andAnswer(() =>
            {
                removeExecuted = true;
                return null;
            });

            replay(sessionStore);

            return sessionStore;
        }

        private void InitStateStoreMock(StateStore stateStore)
        {
            expect(stateStore.name()).andReturn(STORE_NAME);
            expect(stateStore.persistent()).andReturn(true);
            expect(stateStore.isOpen()).andReturn(true);

            stateStore.flush();
            expectLastCall().andAnswer(() =>
            {
                flushExecuted = true;
                return null;
            });
        }

        private <T : StateStore> void DoTest(string name, Consumer<T> checker)
        {
            Processor processor = new Processor<string, long>()
            {



            public void init(ProcessorContext context)
            {
                T store = (T)context.getStateStore(name);
                checker.accept(store);
            }


            public void process(string k, long v)
            {
                //No-op.
            }


            public void close()
            {
                //No-op.
            }
        };

        processor.init(context);
    }

    private void VerifyStoreCannotBeInitializedOrClosed(StateStore store)
    {
        Assert.Equal(STORE_NAME, store.name());
        Assert.True(store.persistent());
        Assert.True(store.isOpen());

        checkThrowsUnsupportedOperation(() => store.init(null, null), "init()");
        checkThrowsUnsupportedOperation(store::close, "close()");
    }

    private void CheckThrowsUnsupportedOperation(Runnable check, string name)
    {
        try
        {
            check.run();
            Assert.True(false, name + " should throw exception");
        }
        catch (UnsupportedOperationException e)
        {
            //ignore.
        }
    }
}
}
/*






*

*





*/










































