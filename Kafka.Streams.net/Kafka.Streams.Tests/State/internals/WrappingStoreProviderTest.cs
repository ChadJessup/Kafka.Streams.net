using Confluent.Kafka;
using Xunit;
using System;

namespace Kafka.Streams.Tests.State
{
    public class WrappingStoreProviderTest
    {

        private WrappingStoreProvider wrappingStoreProvider;


        public void before()
        {
            StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
            StateStoreProviderStub stubProviderTwo = new StateStoreProviderStub(false);


            stubProviderOne.addStore("kv", Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("kv"),
                    Serdes.serdeFrom(string),
                    Serdes.serdeFrom(string))
                    .build());
            stubProviderOne.addStore("window", new NoOpWindowStore());
            stubProviderTwo.addStore("kv", Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("kv"),
                    Serdes.serdeFrom(string),
                    Serdes.serdeFrom(string))
                    .build());
            stubProviderTwo.addStore("window", new NoOpWindowStore());

            wrappingStoreProvider = new WrappingStoreProvider(
                    Array.< StateStoreProvider > asList(stubProviderOne, stubProviderTwo));
        }

        [Xunit.Fact]
        public void shouldFindKeyValueStores()
        {
            List<ReadOnlyKeyValueStore<string, string>> results =
                    wrappingStoreProvider.stores("kv", QueryableStoreTypes.< string, string > keyValueStore());
            Assert.Equal(2, results.Count);
        }

        [Xunit.Fact]
        public void shouldFindWindowStores()
        {
            List<ReadOnlyWindowStore<object, object>>
                    windowStores =
                    wrappingStoreProvider.stores("window", windowStore());
            Assert.Equal(2, windowStores.Count);
        }

        [Xunit.Fact]// (expected = InvalidStateStoreException)
        public void shouldThrowInvalidStoreExceptionIfNoStoreOfTypeFound()
        {
            wrappingStoreProvider.stores("doesn't exist", QueryableStoreTypes.keyValueStore());
        }
    }
}
