//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Internals;
//using Kafka.Streams.State.Queryable;
//using Kafka.Streams.State.ReadOnly;
//using Kafka.Streams.State.TimeStamped;
//using Kafka.Streams.Tasks;
//using Moq;
//using NodaTime;
//using System.Collections.Generic;
//using System.Linq;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class GlobalStateStoreProviderTest
//    {
//        private Dictionary<string, IStateStore> stores = new Dictionary<string, IStateStore>();

//        public GlobalStateStoreProviderTest()
//        {
//            stores.Add(
//                "kv-store",
//                Stores.KeyValueStoreBuilder(
//                    Stores.InMemoryKeyValueStore("kv-store"),
//                    Serdes.String(),
//                    Serdes.String()).Build());
//            stores.Add(
//                "ts-kv-store",
//                Stores.TimestampedKeyValueStoreBuilder(
//                    Stores.InMemoryKeyValueStore("ts-kv-store"),
//                    Serdes.String(),
//                    Serdes.String()).Build());
//            stores.Add(
//                "w-store",
//                Stores.WindowStoreBuilder(
//                    Stores.InMemoryWindowStore(
//                        "w-store",
//                        Duration.FromMilliseconds(10L),
//                        Duration.FromMilliseconds(2L),
//                        false),
//                    Serdes.String(),
//                    Serdes.String()).Build());
//            stores.Add(
//                "ts-w-store",
//                Stores.TimestampedWindowStoreBuilder(
//                    Stores.InMemoryWindowStore(
//                        "ts-w-store",
//                        Duration.FromMilliseconds(10L),
//                        Duration.FromMilliseconds(2L),
//                        false),
//                    Serdes.String(),
//                    Serdes.String()).Build());

//            IProcessorContext mockContext = Mock.Of<IProcessorContext>();

//            expect(mockContext.applicationId()).andReturn("appId").anyTimes();
//            //expect(mockContext.metrics()).andReturn(new StreamsMetricsImpl(new Metrics(), "threadName")).anyTimes();
//            expect(mockContext.taskId()).andReturn(new TaskId(0, 0)).anyTimes();
//            expect(mockContext.recordCollector()).andReturn(null).anyTimes();
//            replay(mockContext);

//            foreach (IStateStore store in stores.Values)
//            {
//                store.Init(mockContext, null);
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldReturnSingleItemListIfStoreExists()
//        {
//            GlobalStateStoreProvider provider =
//                new GlobalStateStoreProvider(new Dictionary<string, IStateStore> { { "global", new NoOpReadOnlyStore<>} });
//            List<IReadOnlyKeyValueStore<object, object>> stores =
//                provider.Stores("global", QueryableStoreTypes.KeyValueStore<object, object>());
//            Assert.Single(stores);
//        }

//        [Xunit.Fact]
//        public void ShouldReturnEmptyItemListIfStoreDoesntExist()
//        {
//            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(new Dictionary<string, IStateStore>());
//            List<IReadOnlyKeyValueStore<object, object>> stores =
//                provider.Stores("global", QueryableStoreTypes.KeyValueStore<object, object>());
//            Assert.False(stores.Any());
//        }

//        [Xunit.Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowExceptionIfStoreIsntOpen()
//        {
//            NoOpReadOnlyStore<object, object> store = new NoOpReadOnlyStore<object, object>();
//            store.close();
//            GlobalStateStoreProvider provider =
//                new GlobalStateStoreProvider(Collections.singletonMap("global", store));
//            provider.Stores("global", QueryableStoreTypes.KeyValueStore<object, object>());
//        }

//        [Xunit.Fact]
//        public void ShouldReturnKeyValueStore()
//        {
//            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
//            List<IReadOnlyKeyValueStore<string, string>> stores =
//                provider.Stores("kv-store", QueryableStoreTypes.KeyValueStore<string, string>());
//            Assert.Single(stores);
//            foreach (IReadOnlyKeyValueStore<string, string> store in stores)
//            {
//                Assert.IsAssignableFrom<IReadOnlyKeyValueStore>(store);
//                Assert.False(store.GetType().IsAssignableFrom(typeof(ITimestampedKeyValueStore)));
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldReturnTimestampedKeyValueStore()
//        {
//            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
//            List<IReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>> stores =
//                provider.Stores("ts-kv-store", QueryableStoreTypes.TimestampedKeyValueStore<string, string>());
//            Assert.Single(stores);
//            foreach (IReadOnlyKeyValueStore<string, ValueAndTimestamp<string>> store in stores)
//            {
//                Assert.IsAssignableFrom<IReadOnlyKeyValueStore>(store);
//                Assert.IsAssignableFrom<ITimestampedKeyValueStore>(store);
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldNotReturnKeyValueStoreAsTimestampedStore()
//        {
//            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
//            List<IReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>> stores =
//                provider.Stores("kv-store", QueryableStoreTypes.TimestampedKeyValueStore<string, string>());
//            Assert.Empty(stores);
//        }

//        [Xunit.Fact]
//        public void ShouldReturnTimestampedKeyValueStoreAsKeyValueStore()
//        {
//            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
//            List<IReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>> stores =
//                provider.Stores("ts-kv-store", QueryableStoreTypes.KeyValueStore<string, ValueAndTimestamp<string>>());
//            Assert.Single(stores);
//            foreach (IReadOnlyKeyValueStore<string, ValueAndTimestamp<string>> store in stores)
//            {
//                Assert.IsAssignableFrom<IReadOnlyKeyValueStore>(store);
//                Assert.False(store.GetType().IsAssignableFrom(typeof(ITimestampedKeyValueStore)));
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldReturnTimestampedWindowStoreAsWindowStore()
//        {
//            GlobalStateStoreProvider provider = new GlobalStateStoreProvider(stores);
//            List<IReadOnlyWindowStore<string, ValueAndTimestamp<string>>> stores =
//                provider.Stores("ts-w-store", QueryableStoreTypes.WindowStore<string, ValueAndTimestamp<string>>());
//            Assert.Single(stores);

//            foreach (IReadOnlyWindowStore<string, ValueAndTimestamp<string>> store in stores)
//            {
//                Assert.IsAssignableFrom<IReadOnlyWindowStore>(store);
//                Assert.False(store.GetType().IsAssignableFrom(typeof(ITimestampedWindowStore)));
//            }
//        }
//    }
//}
