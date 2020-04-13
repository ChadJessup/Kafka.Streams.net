//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */




















//    public class InMemoryLRUCacheStoreTest : AbstractKeyValueStoreTest
//    {



//        protected IKeyValueStore<K, V> CreateKeyValueStore<K, V>(IProcessorContext context)
//        {

//            var storeBuilder = Stores.KeyValueStoreBuilder(
//                    Stores.lruMap("my-store", 10),
//                    (Serde<K>)context.keySerde,
//                    (Serde<V>)context.valueSerde);

//            IStateStore store = storeBuilder.Build();
//            store.Init(context, store);

//            return (IKeyValueStore<K, V>)store;
//        }

//        [Fact]
//        public void ShouldPutAllKeyValuePairs()
//        {
//            List<KeyValuePair<int, string>> kvPairs = Arrays.asList(KeyValuePair.Create(1, "1"),
//                    KeyValuePair.Create(2, "2"),
//                    KeyValuePair.Create(3, "3"));

//            store.PutAll(kvPairs);

//            Assert.Equal(store.approximateNumEntries, (3L));

//            foreach (KeyValuePair<int, string> kvPair in kvPairs)
//            {
//                Assert.Equal(store.Get(kvPair.Key), (kvPair.Value));
//            }
//        }

//        [Fact]
//        public void ShouldUpdateValuesForExistingKeysOnPutAll()
//        {
//            List<KeyValuePair<int, string>> kvPairs = Arrays.asList(KeyValuePair.Create(1, "1"),
//                    KeyValuePair.Create(2, "2"),
//                    KeyValuePair.Create(3, "3"));

//            store.PutAll(kvPairs);


//            List<KeyValuePair<int, string>> updatedKvPairs = Arrays.asList(KeyValuePair.Create(1, "ONE"),
//                    KeyValuePair.Create(2, "TWO"),
//                    KeyValuePair.Create(3, "THREE"));

//            store.PutAll(updatedKvPairs);

//            Assert.Equal(store.approximateNumEntries, (3L));

//            foreach (KeyValuePair<int, string> kvPair in updatedKvPairs)
//            {
//                Assert.Equal(store.Get(kvPair.Key), (kvPair.Value));
//            }
//        }

//        [Fact]
//        public void TestEvict()
//        {
//            // Create the test driver ...
//            store.Put(0, "zero");
//            store.Put(1, "one");
//            store.Put(2, "two");
//            store.Put(3, "three");
//            store.Put(4, "four");
//            store.Put(5, "five");
//            store.Put(6, "six");
//            store.Put(7, "seven");
//            store.Put(8, "eight");
//            store.Put(9, "nine");
//            Assert.Equal(10, driver.sizeOf(store));

//            store.Put(10, "ten");
//            store.Flush();
//            Assert.Equal(10, driver.sizeOf(store));
//            Assert.True(driver.flushedEntryRemoved(0));
//            Assert.Equal(1, driver.numFlushedEntryRemoved());

//            store.Delete(1);
//            store.Flush();
//            Assert.Equal(9, driver.sizeOf(store));
//            Assert.True(driver.flushedEntryRemoved(0));
//            Assert.True(driver.flushedEntryRemoved(1));
//            Assert.Equal(2, driver.numFlushedEntryRemoved());

//            store.Put(11, "eleven");
//            store.Flush();
//            Assert.Equal(10, driver.sizeOf(store));
//            Assert.Equal(2, driver.numFlushedEntryRemoved());

//            store.Put(2, "two-again");
//            store.Flush();
//            Assert.Equal(10, driver.sizeOf(store));
//            Assert.Equal(2, driver.numFlushedEntryRemoved());

//            store.Put(12, "twelve");
//            store.Flush();
//            Assert.Equal(10, driver.sizeOf(store));
//            Assert.True(driver.flushedEntryRemoved(0));
//            Assert.True(driver.flushedEntryRemoved(1));
//            Assert.True(driver.flushedEntryRemoved(3));
//            Assert.Equal(3, driver.numFlushedEntryRemoved());
//        }

//        [Fact]
//        public void TestRestoreEvict()
//        {
//            store.Close();
//            // Add any entries that will be restored to any store
//            // that uses the driver's context ...
//            driver.addEntryToRestoreLog(0, "zero");
//            driver.addEntryToRestoreLog(1, "one");
//            driver.addEntryToRestoreLog(2, "two");
//            driver.addEntryToRestoreLog(3, "three");
//            driver.addEntryToRestoreLog(4, "four");
//            driver.addEntryToRestoreLog(5, "five");
//            driver.addEntryToRestoreLog(6, "fix");
//            driver.addEntryToRestoreLog(7, "seven");
//            driver.addEntryToRestoreLog(8, "eight");
//            driver.addEntryToRestoreLog(9, "nine");
//            driver.addEntryToRestoreLog(10, "ten");

//            // Create the store, which should register with the context and automatically
//            // receive the restore entries ...
//            store = createKeyValueStore(driver.context);
//            context.restore(store.Name(), driver.restoredEntries());
//            // Verify that the store's changelog does not get more appends ...
//            Assert.Equal(0, driver.numFlushedEntryStored());
//            Assert.Equal(0, driver.numFlushedEntryRemoved());

//            // and there are no other entries ...
//            Assert.Equal(10, driver.sizeOf(store));
//        }
//    }
//}
