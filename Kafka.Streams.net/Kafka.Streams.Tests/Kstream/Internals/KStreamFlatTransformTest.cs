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
//using Kafka.Streams.KStream.Internals;
//using System;

//namespace Kafka.Streams.KStream.Internals
//{

















//    public class KStreamFlatTransformTest : EasyMockSupport
//    {

//        private int inputKey;
//        private int inputValue;

//        private Transformer<int, int, Iterable<KeyValuePair<int, int>>> transformer;
//        private IProcessorContext context;

//        private KStreamFlatTransformProcessor<int, int, int, int> processor;


//        public void setUp()
//        {
//            inputKey = 1;
//            inputValue = 10;
//            transformer = mock(Transformer));
//            context = strictMock(IProcessorContext));
//            processor = new KStreamFlatTransformProcessor<>(transformer);
//        }

//        [Fact]
//        public void shouldInitialiseFlatTransformProcessor()
//        {
//            transformer.Init(context);
//            replayAll();

//            processor.Init(context);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldTransformInputRecordToMultipleOutputRecords()
//        {
//            Iterable<KeyValuePair<int, int>> outputRecords = Array.AsReadOnly(
//                    KeyValuePair.Create(2, 20),
//                    KeyValuePair.Create(3, 30),
//                    KeyValuePair.Create(4, 40));
//            processor.Init(context);
//            EasyMock.reset(transformer);

//            EasyMock.expect(transformer.transform(inputKey, inputValue)).andReturn(outputRecords);
//            foreach (KeyValuePair<int, int> outputRecord in outputRecords)
//            {
//                context.Forward(outputRecord.key, outputRecord.value);
//            }
//            replayAll();

//            processor.process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldAllowEmptyListAsResultOfTransform()
//        {
//            processor.Init(context);
//            EasyMock.reset(transformer);

//            EasyMock.expect(transformer.transform(inputKey, inputValue))
//                .andReturn(Collections.< KeyValuePair<int, int> > emptyList());
//            replayAll();

//            processor.process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldAllowNullAsResultOfTransform()
//        {
//            processor.Init(context);
//            EasyMock.reset(transformer);

//            EasyMock.expect(transformer.transform(inputKey, inputValue))
//                .andReturn(null);
//            replayAll();

//            processor.process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldCloseFlatTransformProcessor()
//        {
//            transformer.close();
//            replayAll();

//            processor.close();

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldGetFlatTransformProcessor()
//        {
//            TransformerSupplier<int, int, Iterable<KeyValuePair<int, int>>> transformerSupplier =
//        mock(TransformerSupplier));
//            KStreamFlatTransform<int, int, int, int> processorSupplier =
//                new KStreamFlatTransform<>(transformerSupplier);

//            EasyMock.expect(transformerSupplier.Get()).andReturn(transformer);
//            replayAll();

//            Processor<int, int> processor = processorSupplier.Get();

//            //verifyAll();
//            Assert.True(processor is KStreamFlatTransformProcessor);
//        }
//    }
//}
