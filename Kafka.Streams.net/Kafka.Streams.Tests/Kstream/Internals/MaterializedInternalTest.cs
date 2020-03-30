namespace Kafka.Streams.Tests.Kstream.Internals
{
}
///*
//
//
//
//
//
//
// *
//
// *
//
//
//
//
//
// */

//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State.KeyValues;

//namespace Kafka.Streams.KStream.Internals
//{

















//    public class MaterializedInternalTest
//    {

//        (type = MockType.NICE)
//        private InternalNameProvider nameProvider;

//        (type = MockType.NICE)
//        private KeyValueBytesStoreSupplier supplier;
//        private string prefix = "prefix";

//        [Fact]
//        public void shouldGenerateStoreNameWithPrefixIfProvidedNameIsNull()
//        {
//            var generatedName = prefix + "-store";
//            EasyMock.expect(nameProvider.newStoreName(prefix)).andReturn(generatedName);

//            EasyMock.replay(nameProvider);

//            MaterializedInternal<object, object, StateStore> materialized =
//                new MaterializedInternal<>(Materialized.with(null, null), nameProvider, prefix);

//            Assert.Equal(materialized.StoreName, (generatedName));
//            EasyMock.verify(nameProvider);
//        }

//        [Fact]
//        public void shouldUseProvidedStoreNameWhenSet()
//        {
//            var storeName = "store-name";
//            MaterializedInternal<object, object, StateStore> materialized =
//                new MaterializedInternal<>(Materialized.As(storeName), nameProvider, prefix);
//            Assert.Equal(materialized.StoreName, (storeName));
//        }

//        [Fact]
//        public void shouldUseStoreNameOfSupplierWhenProvided()
//        {
//            var storeName = "other-store-name";
//            EasyMock.expect(supplier.name()).andReturn(storeName).anyTimes();
//            EasyMock.replay(supplier);
//            MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>> materialized =
//                new MaterializedInternal<>(Materialized.As(supplier), nameProvider, prefix);
//            Assert.Equal(materialized.StoreName, (storeName));
//        }
//    }
//}
