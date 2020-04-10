namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.Processors.Interfaces;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class TransformerSupplierAdapterTest : EasyMockSupport
//    {

//        private IProcessorContext context;
//        private Transformer<string, string, KeyValuePair<int, int>> transformer;
//        private TransformerSupplier<string, string, KeyValuePair<int, int>> transformerSupplier;

//        string key = "Hello";
//        string value = "World";


//        public void before()
//        {
//            context = mock(IProcessorContext));
//            transformer = mock(Transformer));
//            transformerSupplier = mock(TransformerSupplier));
//        }

//        [Fact]
//        public void shouldCallInitOfAdapteeTransformer()
//        {
//            EasyMock.expect(transformerSupplier.Get()).andReturn(transformer);
//            transformer.Init(context);
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adaptedTransformer = adapter.Get();
//            adaptedTransformer.Init(context);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldCallCloseOfAdapteeTransformer()
//        {
//            EasyMock.expect(transformerSupplier.Get()).andReturn(transformer);
//            transformer.Close();
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adaptedTransformer = adapter.Get();
//            adaptedTransformer.Close();

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldCallTransformOfAdapteeTransformerAndReturnSingletonIterable()
//        {
//            EasyMock.expect(transformerSupplier.Get()).andReturn(transformer);
//            EasyMock.expect(transformer.transform(key, value)).andReturn(KeyValuePair.Create(0, 1));
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adaptedTransformer = adapter.Get();
//            Iterator<KeyValuePair<int, int>> iterator = adaptedTransformer.transform(key, value).iterator();

//            //verifyAll();
//            Assert.Equal(iterator..AsNext(), (true));
//            iterator.MoveNext();
//            Assert.Equal(iterator..AsNext(), (false));
//        }

//        [Fact]
//        public void shouldCallTransformOfAdapteeTransformerAndReturnEmptyIterable()
//        {
//            EasyMock.expect(transformerSupplier.Get()).andReturn(transformer);
//            EasyMock.expect(transformer.transform(key, value)).andReturn(null);
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adaptedTransformer = adapter.Get();
//            Iterator<KeyValuePair<int, int>> iterator = adaptedTransformer.transform(key, value).iterator();

//            //verifyAll();
//            Assert.Equal(iterator..AsNext(), (false));
//        }

//        [Fact]
//        public void shouldAlwaysGetNewAdapterTransformer()
//        {
//            Transformer<string, string, KeyValuePair<int, int>> transformer1 = mock(Transformer));
//            Transformer<string, string, KeyValuePair<int, int>> transformer2 = mock(Transformer));
//            Transformer<string, string, KeyValuePair<int, int>> transformer3 = mock(Transformer));
//            EasyMock.expect(transformerSupplier.Get()).andReturn(transformer1);
//            transformer1.Init(context);
//            EasyMock.expect(transformerSupplier.Get()).andReturn(transformer2);
//            transformer2.Init(context);
//            EasyMock.expect(transformerSupplier.Get()).andReturn(transformer3);
//            transformer3.Init(context);
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adapterTransformer1 = adapter.Get();
//            adapterTransformer1.Init(context);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adapterTransformer2 = adapter.Get();
//            adapterTransformer2.Init(context);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adapterTransformer3 = adapter.Get();
//            adapterTransformer3.Init(context);

//            //verifyAll();
//            Assert.Equal(adapterTransformer1, not(sameInstance(adapterTransformer2)));
//            Assert.Equal(adapterTransformer2, not(sameInstance(adapterTransformer3)));
//            Assert.Equal(adapterTransformer3, not(sameInstance(adapterTransformer1)));
//        }

//    }
//}
