//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class KStreamFlatTransformValuesTest
//    {

//        private int inputKey;
//        private int inputValue;

//        private IValueTransformerWithKey<int, int, Iterable<string>> valueTransformer;
//        private IProcessorContext context;

//        private KStreamFlatTransformValuesProcessor<int, int, string> processor;


//        public void setUp()
//        {
//            inputKey = 1;
//            inputValue = 10;
//            valueTransformer = mock(typeof(IValueTransformerWithKey));
//            context = strictMock(typeof(IProcessorContext));
//            processor = new KStreamFlatTransformValuesProcessor<>(valueTransformer);
//        }

//        [Fact]
//        public void shouldInitializeFlatTransformValuesProcessor()
//        {
//            valueTransformer.init(typeof(ForwardingDisabledProcessorContext));
//            replayAll();

//            processor.init(context);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldTransformInputRecordToMultipleOutputValues()
//        {
//            Iterable<string> outputValues = Array.AsReadOnly(
//                    "Hello",
//                    "Blue",
//                    "Planet");
//            processor.init(context);
//            EasyMock.reset(valueTransformer);

//            EasyMock.expect(valueTransformer.transform(inputKey, inputValue)).andReturn(outputValues);
//            foreach (string outputValue in outputValues)
//            {
//                context.forward(inputKey, outputValue);
//            }
//            replayAll();

//            processor.process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldEmitNoRecordIfTransformReturnsEmptyList()
//        {
//            processor.init(context);
//            EasyMock.reset(valueTransformer);

//            EasyMock.expect(valueTransformer.transform(inputKey, inputValue)).andReturn(new List<string>());
//            replayAll();

//            processor.process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldEmitNoRecordIfTransformReturnsNull()
//        {
//            processor.init(context);
//            EasyMock.reset(valueTransformer);

//            EasyMock.expect(valueTransformer.transform(inputKey, inputValue)).andReturn(null);
//            replayAll();

//            processor.process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldCloseFlatTransformValuesProcessor()
//        {
//            valueTransformer.close();
//            replayAll();

//            processor.close();

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldGetFlatTransformValuesProcessor()
//        {
//            IValueTransformerWithKeySupplier<int, int, Iterable<string>> valueTransformerSupplier =
//                mock(typeof(IValueTransformerWithKeySupplier));
//            KStreamFlatTransformValues<int, int, string> processorSupplier =
//                new KStreamFlatTransformValues<>(valueTransformerSupplier);

//            EasyMock.expect(valueTransformerSupplier.get()).andReturn(valueTransformer);
//            replayAll();

//            Processor<int, int> processor = processorSupplier.get();

//            //verifyAll();
//            Assert.True(processor is KStreamFlatTransformValuesProcessor);
//        }
//    }
//}
