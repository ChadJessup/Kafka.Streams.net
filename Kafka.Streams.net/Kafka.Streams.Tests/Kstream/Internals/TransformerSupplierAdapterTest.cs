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
//            EasyMock.expect(transformerSupplier.get()).andReturn(transformer);
//            transformer.init(context);
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adaptedTransformer = adapter.get();
//            adaptedTransformer.init(context);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldCallCloseOfAdapteeTransformer()
//        {
//            EasyMock.expect(transformerSupplier.get()).andReturn(transformer);
//            transformer.close();
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adaptedTransformer = adapter.get();
//            adaptedTransformer.close();

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldCallTransformOfAdapteeTransformerAndReturnSingletonIterable()
//        {
//            EasyMock.expect(transformerSupplier.get()).andReturn(transformer);
//            EasyMock.expect(transformer.transform(key, value)).andReturn(KeyValuePair.Create(0, 1));
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adaptedTransformer = adapter.get();
//            Iterator<KeyValuePair<int, int>> iterator = adaptedTransformer.transform(key, value).iterator();

//            //verifyAll();
//            Assert.Equal(iterator..AsNext(), (true));
//            iterator.next();
//            Assert.Equal(iterator..AsNext(), (false));
//        }

//        [Fact]
//        public void shouldCallTransformOfAdapteeTransformerAndReturnEmptyIterable()
//        {
//            EasyMock.expect(transformerSupplier.get()).andReturn(transformer);
//            EasyMock.expect(transformer.transform(key, value)).andReturn(null);
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adaptedTransformer = adapter.get();
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
//            EasyMock.expect(transformerSupplier.get()).andReturn(transformer1);
//            transformer1.init(context);
//            EasyMock.expect(transformerSupplier.get()).andReturn(transformer2);
//            transformer2.init(context);
//            EasyMock.expect(transformerSupplier.get()).andReturn(transformer3);
//            transformer3.init(context);
//            replayAll();

//            TransformerSupplierAdapter<string, string, int, int> adapter =
//                new TransformerSupplierAdapter<>(transformerSupplier);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adapterTransformer1 = adapter.get();
//            adapterTransformer1.init(context);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adapterTransformer2 = adapter.get();
//            adapterTransformer2.init(context);
//            Transformer<string, string, Iterable<KeyValuePair<int, int>>> adapterTransformer3 = adapter.get();
//            adapterTransformer3.init(context);

//            //verifyAll();
//            Assert.Equal(adapterTransformer1, not(sameInstance(adapterTransformer2)));
//            Assert.Equal(adapterTransformer2, not(sameInstance(adapterTransformer3)));
//            Assert.Equal(adapterTransformer3, not(sameInstance(adapterTransformer1)));
//        }

//    }
//}
