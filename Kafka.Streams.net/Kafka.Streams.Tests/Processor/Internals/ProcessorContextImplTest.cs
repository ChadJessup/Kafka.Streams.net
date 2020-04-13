using Kafka.Streams.KStream;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;
using Moq;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class ProcessorContextImplTest
    {
        private ProcessorContextImpl context;

        private const string KEY = "key";
        private const long VALUE = 42L;
        private static IValueAndTimestamp<long> VALUE_AND_TIMESTAMP = ValueAndTimestamp.Make(42L, 21L);
        private const string STORE_NAME = "underlying-store";

        private bool flushExecuted;
        private bool putExecuted;
        private bool putWithTimestampExecuted;
        private bool putIfAbsentExecuted;
        private bool putAllExecuted;
        private bool deleteExecuted;
        private bool removeExecuted;

        private IKeyValueIterator<string, long> rangeIter;
        private IKeyValueIterator<string, IValueAndTimestamp<long>> timestampedRangeIter;
        private IKeyValueIterator<string, long> allIter;
        private IKeyValueIterator<string, IValueAndTimestamp<long>> timestampedAllIter;

        private List<IKeyValueIterator<IWindowed<string>, long>> iters = new List<>(7);
        private List<IKeyValueIterator<IWindowed<string>, IValueAndTimestamp<long>>> timestampedIters = new List<>(7);
        private IWindowStoreIterator windowStoreIter;


        public void Setup()
        {
            flushExecuted = false;
            putExecuted = false;
            putIfAbsentExecuted = false;
            putAllExecuted = false;
            deleteExecuted = false;
            removeExecuted = false;

            rangeIter = Mock.Of < IKeyValueIterator);
            timestampedRangeIter = Mock.Of < IKeyValueIterator);
            allIter = Mock.Of < IKeyValueIterator);
            timestampedAllIter = Mock.Of < IKeyValueIterator);
            windowStoreIter = Mock.Of < IWindowStoreIterator);

            for (int i = 0; i < 7; i++)
            {
                iters.Add(i, Mock.Of < IKeyValueIterator));
            timestampedIters.Add(i, Mock.Of < IKeyValueIterator));
        }

        StreamsConfig streamsConfig = Mock.Of < StreamsConfig);
        expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("add-id");
        expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
        expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
        replay(streamsConfig);

        ProcessorStateManager stateManager = Mock.Of < ProcessorStateManager);

        expect(stateManager.getGlobalStore("GlobalKeyValueStore")).andReturn(KeyValueStoreMock());
        expect(stateManager.getGlobalStore("GlobalTimestampedKeyValueStore")).andReturn(TimestampedKeyValueStoreMock());
        expect(stateManager.getGlobalStore("GlobalWindowStore")).andReturn(WindowStoreMock());
        expect(stateManager.getGlobalStore("GlobalTimestampedWindowStore")).andReturn(TimestampedWindowStoreMock());
        expect(stateManager.getGlobalStore("GlobalSessionStore")).andReturn(SessionStoreMock());
        expect(stateManager.getGlobalStore(string.Empty)).andReturn(null);

        expect(stateManager.getStore("LocalKeyValueStore")).andReturn(KeyValueStoreMock());
        expect(stateManager.getStore("LocalTimestampedKeyValueStore")).andReturn(TimestampedKeyValueStoreMock());
        expect(stateManager.getStore("LocalWindowStore")).andReturn(WindowStoreMock());
        expect(stateManager.getStore("LocalTimestampedWindowStore")).andReturn(TimestampedWindowStoreMock());
        expect(stateManager.getStore("LocalSessionStore")).andReturn(SessionStoreMock());

        replay(stateManager);

        context = new ProcessorContextImpl(
            Mock.Of<TaskId),
                Mock.Of<StreamTask),
                streamsConfig,
                Mock.Of<RecordCollector),
                stateManager,
                Mock.Of<StreamsMetricsImpl),
                Mock.Of<ThreadCache)
            );

            context.setCurrentNode(new ProcessorNode<string, long>("fake", null,
                new HashSet<>(asList(
                    "LocalKeyValueStore",
                    "LocalTimestampedKeyValueStore",
                    "LocalWindowStore",
                    "LocalTimestampedWindowStore",
                    "LocalSessionStore"))));
        }

    [Fact]
    public void GlobalKeyValueStoreShouldBeReadOnly()
    {
        doTest("GlobalKeyValueStore", (IConsumer<IKeyValueStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::Flush, "Flush()");
            checkThrowsUnsupportedOperation(() => store.Put("1", 1L), "Put()");
            checkThrowsUnsupportedOperation(() => store.PutIfAbsent("1", 1L), "PutIfAbsent()");
            checkThrowsUnsupportedOperation(() => store.PutAll(Collections.emptyList()), "putAll()");
            checkThrowsUnsupportedOperation(() => store.Delete("1"), "delete()");

            Assert.Equal((long)VALUE, store.Get(KEY));
            Assert.Equal(rangeIter, store.Range("one", "two"));
            Assert.Equal(allIter, store.All());
            Assert.Equal(VALUE, store.approximateNumEntries);
        });
    }

    [Fact]
    public void GlobalTimestampedKeyValueStoreShouldBeReadOnly()
    {
        doTest("GlobalTimestampedKeyValueStore", (IConsumer<ITimestampedKeyValueStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::Flush, "Flush()");
            checkThrowsUnsupportedOperation(() => store.Put("1", ValueAndTimestamp.Make(1L, 2L)), "Put()");
            checkThrowsUnsupportedOperation(() => store.PutIfAbsent("1", ValueAndTimestamp.Make(1L, 2L)), "PutIfAbsent()");
            checkThrowsUnsupportedOperation(() => store.PutAll(Collections.emptyList()), "putAll()");
            checkThrowsUnsupportedOperation(() => store.Delete("1"), "delete()");

            Assert.Equal(VALUE_AND_TIMESTAMP, store.Get(KEY));
            Assert.Equal(timestampedRangeIter, store.Range("one", "two"));
            Assert.Equal(timestampedAllIter, store.All());
            Assert.Equal(VALUE, store.approximateNumEntries);
        });
    }

    [Fact]
    public void GlobalWindowStoreShouldBeReadOnly()
    {
        doTest("GlobalWindowStore", (IConsumer<IWindowStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::Flush, "Flush()");
            checkThrowsUnsupportedOperation(() => store.Put("1", 1L, 1L), "Put()");
            checkThrowsUnsupportedOperation(() => store.Put("1", 1L), "Put()");

            Assert.Equal(iters.Get(0), store.FetchAll(0L, 0L));
            Assert.Equal(windowStoreIter, store.Fetch(KEY, 0L, 1L));
            Assert.Equal(iters.Get(1), store.Fetch(KEY, KEY, 0L, 1L));
            Assert.Equal((long)VALUE, store.Fetch(KEY, 1L));
            Assert.Equal(iters.Get(2), store.All());
        });
    }

    [Fact]
    public void GlobalTimestampedWindowStoreShouldBeReadOnly()
    {
        doTest("GlobalTimestampedWindowStore", (IConsumer<ITimestampedWindowStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::Flush, "Flush()");
            checkThrowsUnsupportedOperation(() => store.Put("1", ValueAndTimestamp.Make(1L, 1L), 1L), "Put() [with timestamp]");
            checkThrowsUnsupportedOperation(() => store.Put("1", ValueAndTimestamp.Make(1L, 1L)), "Put() [no timestamp]");

            Assert.Equal(timestampedIters.Get(0), store.FetchAll(0L, 0L));
            Assert.Equal(windowStoreIter, store.Fetch(KEY, 0L, 1L));
            Assert.Equal(timestampedIters.Get(1), store.Fetch(KEY, KEY, 0L, 1L));
            Assert.Equal(VALUE_AND_TIMESTAMP, store.Fetch(KEY, 1L));
            Assert.Equal(timestampedIters.Get(2), store.All());
        });
    }

    [Fact]
    public void GlobalSessionStoreShouldBeReadOnly()
    {
        doTest("GlobalSessionStore", (IConsumer<ISessionStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            checkThrowsUnsupportedOperation(store::Flush, "Flush()");
            checkThrowsUnsupportedOperation(() => store.remove(null), "remove()");
            checkThrowsUnsupportedOperation(() => store.Put(null, null), "Put()");

            Assert.Equal(iters.Get(3), store.findSessions(KEY, 1L, 2L));
            Assert.Equal(iters.Get(4), store.findSessions(KEY, KEY, 1L, 2L));
            Assert.Equal(iters.Get(5), store.Fetch(KEY));
            Assert.Equal(iters.Get(6), store.Fetch(KEY, KEY));
        });
    }

    [Fact]
    public void LocalKeyValueStoreShouldNotAllowInitOrClose()
    {
        doTest("LocalKeyValueStore", (IConsumer<IKeyValueStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.Flush();
            Assert.True(flushExecuted);

            store.Put("1", 1L);
            Assert.True(putExecuted);

            store.PutIfAbsent("1", 1L);
            Assert.True(putIfAbsentExecuted);

            store.PutAll(Collections.emptyList());
            Assert.True(putAllExecuted);

            store.Delete("1");
            Assert.True(deleteExecuted);

            Assert.Equal((long)VALUE, store.Get(KEY));
            Assert.Equal(rangeIter, store.Range("one", "two"));
            Assert.Equal(allIter, store.All());
            Assert.Equal(VALUE, store.approximateNumEntries);
        });
    }

    [Fact]
    public void LocalTimestampedKeyValueStoreShouldNotAllowInitOrClose()
    {
        doTest("LocalTimestampedKeyValueStore", (IConsumer<ITimestampedKeyValueStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.Flush();
            Assert.True(flushExecuted);

            store.Put("1", ValueAndTimestamp.Make(1L, 2L));
            Assert.True(putExecuted);

            store.PutIfAbsent("1", ValueAndTimestamp.Make(1L, 2L));
            Assert.True(putIfAbsentExecuted);

            store.PutAll(Collections.emptyList());
            Assert.True(putAllExecuted);

            store.Delete("1");
            Assert.True(deleteExecuted);

            Assert.Equal(VALUE_AND_TIMESTAMP, store.Get(KEY));
            Assert.Equal(timestampedRangeIter, store.Range("one", "two"));
            Assert.Equal(timestampedAllIter, store.All());
            Assert.Equal(VALUE, store.approximateNumEntries);
        });
    }

    [Fact]
    public void LocalWindowStoreShouldNotAllowInitOrClose()
    {
        doTest("LocalWindowStore", (IConsumer<IWindowStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.Flush();
            Assert.True(flushExecuted);

            store.Put("1", 1L);
            Assert.True(putExecuted);

            Assert.Equal(iters.Get(0), store.FetchAll(0L, 0L));
            Assert.Equal(windowStoreIter, store.Fetch(KEY, 0L, 1L));
            Assert.Equal(iters.Get(1), store.Fetch(KEY, KEY, 0L, 1L));
            Assert.Equal((long)VALUE, store.Fetch(KEY, 1L));
            Assert.Equal(iters.Get(2), store.All());
        });
    }

    [Fact]
    public void LocalTimestampedWindowStoreShouldNotAllowInitOrClose()
    {
        doTest("LocalTimestampedWindowStore", (IConsumer<ITimestampedWindowStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.Flush();
            Assert.True(flushExecuted);

            store.Put("1", ValueAndTimestamp.Make(1L, 1L));
            Assert.True(putExecuted);

            store.Put("1", ValueAndTimestamp.Make(1L, 1L), 1L);
            Assert.True(putWithTimestampExecuted);

            Assert.Equal(timestampedIters.Get(0), store.FetchAll(0L, 0L));
            Assert.Equal(windowStoreIter, store.Fetch(KEY, 0L, 1L));
            Assert.Equal(timestampedIters.Get(1), store.Fetch(KEY, KEY, 0L, 1L));
            Assert.Equal(VALUE_AND_TIMESTAMP, store.Fetch(KEY, 1L));
            Assert.Equal(timestampedIters.Get(2), store.All());
        });
    }

    [Fact]
    public void LocalSessionStoreShouldNotAllowInitOrClose()
    {
        doTest("LocalSessionStore", (IConsumer<ISessionStore<string, long>>)store => {
            verifyStoreCannotBeInitializedOrClosed(store);

            store.Flush();
            Assert.True(flushExecuted);

            store.remove(null);
            Assert.True(removeExecuted);

            store.Put(null, null);
            Assert.True(putExecuted);

            Assert.Equal(iters.Get(3), store.findSessions(KEY, 1L, 2L));
            Assert.Equal(iters.Get(4), store.findSessions(KEY, KEY, 1L, 2L));
            Assert.Equal(iters.Get(5), store.Fetch(KEY));
            Assert.Equal(iters.Get(6), store.Fetch(KEY, KEY));
        });
    }


    private IKeyValueStore<string, long> KeyValueStoreMock()
    {
        var keyValueStoreMock = Mock.Of<IKeyValueStore<string, long>>();

        initStateStoreMock(keyValueStoreMock);

        expect(keyValueStoreMock.Get(KEY)).andReturn(VALUE);
        expect(keyValueStoreMock.approximateNumEntries).andReturn(VALUE);

        expect(keyValueStoreMock.Range("one", "two")).andReturn(rangeIter);
        expect(keyValueStoreMock.All()).andReturn(allIter);


        keyValueStoreMock.Add(string.Empty, 0L);
        expectLastCall().andAnswer(() =>
        {
            putExecuted = true;
            return null;
        });

        keyValueStoreMock.PutIfAbsent(string.Empty, 0L);
        expectLastCall().andAnswer(() =>
        {
            putIfAbsentExecuted = true;
            return null;
        });

        keyValueStoreMock.PutAll(default);
        expectLastCall().andAnswer(() =>
        {
            putAllExecuted = true;
            return null;
        });

        keyValueStoreMock.Delete(string.Empty);
        expectLastCall().andAnswer(() =>
        {
            deleteExecuted = true;
            return null;
        });

        replay(keyValueStoreMock);

        return keyValueStoreMock;
    }


    private ITimestampedKeyValueStore<string, long> TimestampedKeyValueStoreMock()
    {
        var timestampedKeyValueStoreMock = Mock.Of<ITimestampedKeyValueStore<string, long>>();

        InitStateStoreMock(timestampedKeyValueStoreMock);

        expect(timestampedKeyValueStoreMock.Get(KEY)).andReturn(VALUE_AND_TIMESTAMP);
        expect(timestampedKeyValueStoreMock.approximateNumEntries).andReturn(VALUE);

        expect(timestampedKeyValueStoreMock.Range("one", "two")).andReturn(timestampedRangeIter);
        expect(timestampedKeyValueStoreMock.All()).andReturn(timestampedAllIter);


        timestampedKeyValueStoreMock.Add(string.Empty, default);
        expectLastCall().andAnswer(() =>
        {
            putExecuted = true;
            return null;
        });

        timestampedKeyValueStoreMock.PutIfAbsent(string.Empty, default);
        expectLastCall().andAnswer(() =>
        {
            putIfAbsentExecuted = true;
            return null;
        });

        timestampedKeyValueStoreMock.PutAll(default);
        expectLastCall().andAnswer(() =>
        {
            putAllExecuted = true;
            return null;
        });

        timestampedKeyValueStoreMock.Delete(string.Empty);
        expectLastCall().andAnswer(() =>
        {
            deleteExecuted = true;
            return null;
        });

        replay(timestampedKeyValueStoreMock);

        return timestampedKeyValueStoreMock;
    }

    private IWindowStore<string, long> WindowStoreMock()
    {
        IWindowStore<string, long> windowStore = Mock.Of<IWindowStore<string, long>>();

        InitStateStoreMock(windowStore);

        expect(windowStore.FetchAll(0L, 0L)).andReturn(iters.Get(0));
        expect(windowStore.Fetch(string.Empty, string.Empty, 0L, 0L)).andReturn(iters.Get(1));
        expect(windowStore.Fetch(string.Empty, 0L, 0L)).andReturn(windowStoreIter);
        expect(windowStore.Fetch(string.Empty, 0L)).andReturn(VALUE);
        expect(windowStore.All()).andReturn(iters.Get(2));

        windowStore.Put(string.Empty, 0L);
        expectLastCall().andAnswer(() =>
        {
            putExecuted = true;
            return null;
        });

        replay(windowStore);

        return windowStore;
    }


    private ITimestampedWindowStore<string, long> TimestampedWindowStoreMock()
    {
        ITimestampedWindowStore<string, long> windowStore = null; // Mock.Of<ITimestampedWindowStore);

        initStateStoreMock(windowStore);

        expect(windowStore.FetchAll(0L, 0L)).andReturn(timestampedIters.Get(0));
        expect(windowStore.Fetch(string.Empty, string.Empty, 0L, 0L)).andReturn(timestampedIters.Get(1));
        expect(windowStore.Fetch(string.Empty, 0L, 0L)).andReturn(windowStoreIter);
        expect(windowStore.Fetch(string.Empty, 0L)).andReturn(VALUE_AND_TIMESTAMP);
        expect(windowStore.All()).andReturn(timestampedIters.Get(2));

        windowStore.Put(string.Empty, default));
        expectLastCall().andAnswer(() =>
        {
            putExecuted = true;
            return null;
        });

        windowStore.Put(string.Empty, default(IValueAndTimestamp<long>), 0L);
        expectLastCall().andAnswer(() =>
        {
            putWithTimestampExecuted = true;
            return null;
        });

        replay(windowStore);

        return windowStore;
    }


    private ISessionStore<string, long> SessionStoreMock()
    {
        ISessionStore<string, long> sessionStore = Mock.Of<ISessionStore<string, long>>();

        initStateStoreMock(sessionStore);

        expect(sessionStore.findSessions(string.Empty, 0L, 0L)).andReturn(iters.Get(3));
        expect(sessionStore.findSessions(string.Empty, string.Empty, 0L, 0L)).andReturn(iters.Get(4));
        expect(sessionStore.Fetch(string.Empty)).andReturn(iters.Get(5));
        expect(sessionStore.Fetch(string.Empty, string.Empty)).andReturn(iters.Get(6));

        sessionStore.Put(default(Windowed), 0L);
        expectLastCall().andAnswer(() =>
        {
            putExecuted = true;
            return null;
        });

        sessionStore.remove(default(Windowed));
        expectLastCall().andAnswer(() =>
        {
            removeExecuted = true;
            return null;
        });

        replay(sessionStore);

        return sessionStore;
    }

    private void InitStateStoreMock(IStateStore stateStore)
    {
        expect(stateStore.Name()).andReturn(STORE_NAME);
        expect(stateStore.Persistent()).andReturn(true);
        expect(stateStore.IsOpen()).andReturn(true);

        stateStore.Flush();
        expectLastCall().andAnswer(() =>
        {
            flushExecuted = true;
            return null;
        });
    }

    private void DoTest<IStateStore>(string Name, IConsumer<T> checker)
    {
        //    Processor processor = new Processor<string, long>()
        //    {
        //
        //
        //
        //    public void Init(IProcessorContext context)
        //    {
        //        T store = (T)context.getStateStore(Name);
        //        checker.accept(store);
        //    }
        //
        //
        //    public void process(string k, long v)
        //    {
        //        //No-op.
        //    }
        //
        //
        //    public void Close()
        //    {
        //        //No-op.
        //    }
        //};
        //
        processor.Init(context);
    }

    private void VerifyStoreCannotBeInitializedOrClosed(IStateStore store)
    {
        Assert.Equal(STORE_NAME, store.Name());
        Assert.True(store.Persistent());
        Assert.True(store.IsOpen());

        checkThrowsUnsupportedOperation(() => store.Init(null, null), "Init()");
        checkThrowsUnsupportedOperation(store.Close, "Close()");
    }

    private void CheckThrowsUnsupportedOperation(Runnable check, string Name)
    {
        try
        {
            check.run();
            Assert.True(false, Name + " should throw exception");
        }
        catch (InvalidOperationException e)
        {
            //ignore.
        }
    }
}
}
