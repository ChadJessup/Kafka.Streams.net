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
        private IInternalNameProvider nameProvider;
        private IKeyValueBytesStoreSupplier supplier;
        private string prefix = "prefix";

        [Fact]
        public void shouldGenerateStoreNameWithPrefixIfProvidedNameIsNull()
        {
            var generatedName = prefix + "-store";
            //EasyMock.expect(nameProvider.newStoreName(prefix)).andReturn(generatedName);

            //EasyMock.replay(nameProvider);

            MaterializedInternal<object, object, IStateStore> materialized =
                new MaterializedInternal<object, object, IStateStore>(Materialized.With(null, null), nameProvider, prefix);

            Assert.Equal(materialized.StoreName, generatedName);
            //EasyMock.verify(nameProvider);
        }

        [Fact]
        public void shouldUseProvidedStoreNameWhenSet()
        {
            var storeName = "store-Name";
            MaterializedInternal<object, object, IStateStore> materialized =
                new MaterializedInternal<object, object, IStateStore>(Materialized.As(storeName), nameProvider, prefix);
            Assert.Equal(materialized.StoreName, storeName);
        }

        [Fact]
        public void shouldUseStoreNameOfSupplierWhenProvided()
        {
            var storeName = "other-store-Name";
            //EasyMock.expect(supplier.Name()).andReturn(storeName).anyTimes();
            //EasyMock.replay(supplier);
            MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>> materialized =
                new MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>>(Materialized.As(supplier), nameProvider, prefix);
            Assert.Equal(materialized.StoreName, storeName);
        }
    }
}
