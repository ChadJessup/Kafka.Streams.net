using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class MaterializedInternalTest
    {
        private readonly IInternalNameProvider nameProvider;
        private readonly IKeyValueBytesStoreSupplier supplier;
        private readonly string prefix = "prefix";

        [Fact]
        public void shouldGenerateStoreNameWithPrefixIfProvidedNameIsNull()
        {
            var generatedName = prefix + "-store";
            //EasyMock.expect(nameProvider.newStoreName(prefix)).andReturn(generatedName);

            //EasyMock.replay(nameProvider);

            MaterializedInternal<object, object, IStateStore> materialized =
                new MaterializedInternal<object, object, IStateStore>(Materialized.With<object, object, IStateStore>(null, null), nameProvider, prefix);

            Assert.Equal(materialized.StoreName, generatedName);
            //EasyMock.verify(nameProvider);
        }

        [Fact]
        public void shouldUseProvidedStoreNameWhenSet()
        {
            var storeName = "store-Name";
            MaterializedInternal<object, object, IStateStore> materialized =
                new MaterializedInternal<object, object, IStateStore>(Materialized.As<object, object, IStateStore>(storeName), nameProvider, prefix);
            Assert.Equal(materialized.StoreName, storeName);
        }

        [Fact]
        public void shouldUseStoreNameOfSupplierWhenProvided()
        {
            var storeName = "other-store-Name";
            //EasyMock.expect(supplier.Name()).andReturn(storeName).anyTimes();
            //EasyMock.replay(supplier);
            MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>> materialized =
                new MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>>(Materialized.As<object, object>(supplier), nameProvider, prefix);

            Assert.Equal(materialized.StoreName, storeName);
        }
    }
}
