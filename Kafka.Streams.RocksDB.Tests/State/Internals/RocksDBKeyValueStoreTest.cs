//using Kafka.Streams.Errors;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.RocksDbState;
//using Microsoft.Extensions.Options;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class RocksDBKeyValueStoreTest : AbstractKeyValueStoreTest
//    {
//        protected IKeyValueStore<K, V> CreateKeyValueStore<K, V>(IProcessorContext context)
//        {
//            var storeBuilder = Stores.KeyValueStoreBuilder(
//                    Stores.PersistentKeyValueStore("my-store"),
//                    context.keySerde,
//                    context.valueSerde);

//            IStateStore store = storeBuilder.Build();
//            store.Init(context, store);
//            return (IKeyValueStore<K, V>)store;
//        }

//        public class TheRocksDbConfigSetter : IRocksDbConfigSetter
//        {
//            static bool called = false;


//            public void SetConfig(string storeName, Options options, Dictionary<string, object> configs)
//            {
//                called = true;
//            }
//        }

//        [Fact]
//        public void ShouldUseCustomRocksDbConfigSetter()
//        {
//            Assert.True(TheRocksDbConfigSetter.called);
//        }

//        [Fact]
//        public void ShouldPerformRangeQueriesWithCachingDisabled()
//        {
//            context.setTime(1L);
//            store.put(1, "hi");
//            store.put(2, "goodbye");
//            IKeyValueIterator<int, string> range = store.Range(1, 2);
//            Assert.Equal("hi", range.MoveNext().value);
//            Assert.Equal("goodbye", range.MoveNext().value);
//            Assert.False(range.hasNext());
//        }

//        [Fact]
//        public void ShouldPerformAllQueriesWithCachingDisabled()
//        {
//            context.setTime(1L);
//            store.put(1, "hi");
//            store.put(2, "goodbye");
//            IKeyValueIterator<int, string> range = store.all();
//            Assert.Equal("hi", range.MoveNext().value);
//            Assert.Equal("goodbye", range.MoveNext().value);
//            Assert.False(range.hasNext());
//        }

//        [Fact]
//        public void ShouldCloseOpenIteratorsWhenStoreClosedAndThrowInvalidStateStoreOnHasNextAndNext()
//        {
//            context.setTime(1L);
//            store.put(1, "hi");
//            store.put(2, "goodbye");
//            IKeyValueIterator<int, string> iteratorOne = store.Range(1, 5);
//            IKeyValueIterator<int, string> iteratorTwo = store.Range(1, 4);

//            Assert.True(iteratorOne.hasNext());
//            Assert.True(iteratorTwo.hasNext());

//            store.close();

//            try
//            {
//                iteratorOne.hasNext();
//                Assert.True(false, "should have thrown InvalidStateStoreException on closed store");
//            }
//            catch (InvalidStateStoreException e)
//            {
//                // ok
//            }

//            try
//            {
//                iteratorOne.MoveNext();
//                Assert.True(false, "should have thrown InvalidStateStoreException on closed store");
//            }
//            catch (InvalidStateStoreException e)
//            {
//                // ok
//            }

//            try
//            {
//                iteratorTwo.hasNext();
//                Assert.True(false, "should have thrown InvalidStateStoreException on closed store");
//            }
//            catch (InvalidStateStoreException e)
//            {
//                // ok
//            }

//            try
//            {
//                iteratorTwo.MoveNext();
//                Assert.True(false, "should have thrown InvalidStateStoreException on closed store");
//            }
//            catch (InvalidStateStoreException e)
//            {
//                // ok
//            }
//        }

//    }
//}
///*






//*

//*





//*/





















