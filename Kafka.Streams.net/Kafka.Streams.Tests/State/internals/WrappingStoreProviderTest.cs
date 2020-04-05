//using Confluent.Kafka;
//using Xunit;
//using System;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Internals;
//using System.Collections.Generic;
//using Kafka.Streams.State.Interfaces;
//using Kafka.Streams.State.ReadOnly;
//using Kafka.Streams.State.Queryable;

//namespace Kafka.Streams.Tests.State
//{
//    public class WrappingStoreProviderTest
//    {
//        private WrappingStoreProvider wrappingStoreProvider;

//        public void Before()
//        {
//            StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
//            StateStoreProviderStub stubProviderTwo = new StateStoreProviderStub(false);

//            stubProviderOne.addStore("kv", Stores.KeyValueStoreBuilder(Stores.InMemoryKeyValueStore("kv"),
//                    Serdes.SerdeFrom<string>(),
//                    Serdes.SerdeFrom<string>())
//                    .Build());
//            stubProviderOne.addStore("window", new NoOpWindowStore());
//            stubProviderTwo.addStore("kv", Stores.KeyValueStoreBuilder(Stores.InMemoryKeyValueStore("kv"),
//                    Serdes.SerdeFrom<string>(),
//                    Serdes.SerdeFrom<string>())
//                    .Build());
//            stubProviderTwo.addStore("window", new NoOpWindowStore());

//            wrappingStoreProvider = new WrappingStoreProvider(
//                    new List<IStateStoreProvider> { stubProviderOne, stubProviderTwo });
//        }

//        [Fact]
//        public void ShouldFindKeyValueStores()
//        {
//            List<IReadOnlyKeyValueStore<string, string>> results =
//                    wrappingStoreProvider.Stores(
//                        "kv",
//                        QueryableStoreTypes.KeyValueStore<string, string>());

//            Assert.Equal(2, results.Count);
//        }

//        [Fact]
//        public void ShouldFindWindowStores()
//        {
//            List<IReadOnlyWindowStore<object, object>>
//                    windowStores =
//                    wrappingStoreProvider.Stores("window", windowStore());
//            Assert.Equal(2, windowStores.Count);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionIfNoStoreOfTypeFound()
//        {
//            wrappingStoreProvider.Stores("doesn't exist", QueryableStoreTypes.KeyValueStore<string, string>());
//        }
//    }
//}
