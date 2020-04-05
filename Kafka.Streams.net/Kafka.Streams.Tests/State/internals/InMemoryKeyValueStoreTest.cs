//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class InMemoryKeyValueStoreTest : AbstractKeyValueStoreTest
//    {
//        protected IKeyValueStore<K, V> CreateKeyValueStore<K, V>(IProcessorContext context)
//        {
//            var storeBuilder = Stores.KeyValueStoreBuilder(
//                    Stores.InMemoryKeyValueStore("my-store"),
//                    (Serde<K>)context.keySerde,
//                    (Serde<V>)context.valueSerde);

//            IStateStore store = storeBuilder.Build();
//            store.Init(context, store);
//            return (IKeyValueStore<K, V>)store;
//        }

//        [Xunit.Fact]
//        public void ShouldRemoveKeysWithNullValues()
//        {
//            store.close();
//            // Add any entries that will be restored to any store
//            // that uses the driver's context ...
//            driver.addEntryToRestoreLog(0, "zero");
//            driver.addEntryToRestoreLog(1, "one");
//            driver.addEntryToRestoreLog(2, "two");
//            driver.addEntryToRestoreLog(3, "three");
//            driver.addEntryToRestoreLog(0, null);

//            store = createKeyValueStore(driver.context);
//            context.restore(store.name(), driver.restoredEntries());

//            Assert.Equal(3, driver.sizeOf(store));

//            Assert.Equal(store.Get(0), nullValue());
//        }
//    }
//}
