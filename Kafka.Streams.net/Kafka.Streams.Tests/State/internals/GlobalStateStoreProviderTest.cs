namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */




































    public class GlobalStateStoreProviderTest
    {
        private Dictionary<string, StateStore> stores = new HashMap<>();


        public void Before()
        {
            stores.put(
                "kv-store",
                Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("kv-store"),
                    Serdes.String(),
                    Serdes.String()).build());
            stores.put(
                "ts-kv-store",
                Stores.timestampedKeyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("ts-kv-store"),
                    Serdes.String(),
                    Serdes.String()).build());
            stores.put(
                "w-store",
                Stores.windowStoreBuilder(
                    Stores.inMemoryWindowStore(
                        "w-store",
                        Duration.ofMillis(10L),
                        Duration.ofMillis(2L),
                        false),
                    Serdes.String(),
                    Serdes.String()).build());
            stores.put(
                "ts-w-store",
                Stores.timestampedWindowStoreBuilder(
                    Stores.inMemoryWindowStore(
                        "ts-w-store",
                        Duration.ofMillis(10L),
                        Duration.ofMillis(2L),
                        false),
                    Serdes.String(),
                    Serdes.String()).build());

            ProcessorContextImpl mockContext = mock(ProcessorContextImpl);
            expect(mockContext.applicationId()).andReturn("appId").anyTimes();
            expect(mockContext.metrics()).andReturn(new StreamsMetricsImpl(new Metrics(), "threadName")).anyTimes();
            expect(mockContext.taskId()).andReturn(new TaskId(0, 0)).anyTimes();
            expect(mockContext.recordCollector()).andReturn(null).anyTimes();
            replay(mockContext);
            foreach (StateStore store in stores.values())
            {
                store.init(mockContext, null);
            }
        }

        [Xunit.Fact]
        public void ShouldReturnSingleItemListIfStoreExists()
        {
            GlobalStateStoreProvider provider =
                new GlobalStateStoreProvider(Collections.singletonMap("global", new NoOpReadOnlyStore<>()));
            List<ReadOnlyKeyValueStore<object, object>> stores =
                provider.stores("global", QueryableStoreTypes.keyValueStore());
            Assert.Equal(stores.Count, 1);
        }

        [Xunit.Fact]
        public void ShouldReturnEmptyItemListIfStoreDoesntExist()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(Collections.emptyMap());
            List<ReadOnlyKeyValueStore<object, object>> stores =
                provider.stores("global", QueryableStoreTypes.keyValueStore());
            Assert.True(stores.isEmpty());
        }

        [Xunit.Fact]// (expected = InvalidStateStoreException)
        public void ShouldThrowExceptionIfStoreIsntOpen()
        {
            NoOpReadOnlyStore<object, object> store = new NoOpReadOnlyStore<>();
            store.close();
            GlobalStateStoreProvider provider =
                new GlobalStateStoreProvider(Collections.singletonMap("global", store));
            provider.stores("global", QueryableStoreTypes.keyValueStore());
        }

        [Xunit.Fact]
        public void ShouldReturnKeyValueStore()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
            List<ReadOnlyKeyValueStore<string, string>> stores =
                provider.stores("kv-store", QueryableStoreTypes.keyValueStore());
            Assert.Equal(1, stores.Count);
            foreach (ReadOnlyKeyValueStore<string, string> store in stores)
            {
                Assert.Equal(store, instanceOf(ReadOnlyKeyValueStore));
                Assert.Equal(store, not(instanceOf(TimestampedKeyValueStore)));
            }
        }

        [Xunit.Fact]
        public void ShouldReturnTimestampedKeyValueStore()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
            List<ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>> stores =
                provider.stores("ts-kv-store", QueryableStoreTypes.timestampedKeyValueStore());
            Assert.Equal(1, stores.Count);
            foreach (ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>> store in stores)
            {
                Assert.Equal(store, instanceOf(ReadOnlyKeyValueStore));
                Assert.Equal(store, instanceOf(TimestampedKeyValueStore));
            }
        }

        [Xunit.Fact]
        public void ShouldNotReturnKeyValueStoreAsTimestampedStore()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
            List<ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>> stores =
                provider.stores("kv-store", QueryableStoreTypes.timestampedKeyValueStore());
            Assert.Equal(0, stores.Count);
        }

        [Xunit.Fact]
        public void ShouldReturnTimestampedKeyValueStoreAsKeyValueStore()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
            List<ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>> stores =
                provider.stores("ts-kv-store", QueryableStoreTypes.keyValueStore());
            Assert.Equal(1, stores.Count);
            foreach (ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>> store in stores)
            {
                Assert.Equal(store, instanceOf(ReadOnlyKeyValueStore));
                Assert.Equal(store, not(instanceOf(TimestampedKeyValueStore)));
            }
        }

        [Xunit.Fact]
        public void ShouldReturnTimestampedWindowStoreAsWindowStore()
        {
            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
            List<ReadOnlyWindowStore<string, ValueAndTimestamp<string>>> stores =
                provider.stores("ts-w-store", QueryableStoreTypes.windowStore());
            Assert.Equal(1, stores.Count);
            foreach (ReadOnlyWindowStore<string, ValueAndTimestamp<string>> store in stores)
            {
                Assert.Equal(store, instanceOf(ReadOnlyWindowStore));
                Assert.Equal(store, not(instanceOf(TimestampedWindowStore)));
            }
        }
    }
}
/*






*

*





*/




































