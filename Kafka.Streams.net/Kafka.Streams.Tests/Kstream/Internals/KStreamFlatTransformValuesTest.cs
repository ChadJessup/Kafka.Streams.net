//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.Metered;
//using Moq;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream.Internals
//{
//    public class KStreamFlatTransformValuesTest
//    {

//        private int inputKey;
//        private int inputValue;

//        private IValueTransformerWithKey<int, int, IEnumerable<string>> valueTransformer;
//        private IProcessorContext context;

//        private KStreamFlatTransformValuesProcessor<int, int, string> processor;

//        public KStreamFlatTransformValuesTest()
//        {
//            inputKey = 1;
//            inputValue = 10;
//            valueTransformer = Mock.Of<IValueTransformerWithKey<int, int, IEnumerable<string>>>();
//            context = Mock.Of<IProcessorContext>();
//            processor = new KStreamFlatTransformValuesProcessor<int, int, string>(valueTransformer);
//        }

//        [Fact]
//        public void shouldInitializeFlatTransformValuesProcessor()
//        {
//            valueTransformer.Init(Mock.Of<ForwardingDisabledProcessorContext<int, int>>());
//            // replayAll();

//            processor.Init(context);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldTransformInputRecordToMultipleOutputValues()
//        {
//            var outputValues = Array.AsReadOnly(
//                new[]
//                {
//                    "Hello",
//                    "Blue",
//                    "Planet"
//                });

//            processor.Init(context);
//            //EasyMock.reset(valueTransformer);

//            EasyMock.expect(valueTransformer.Transform(inputKey, inputValue)).andReturn(outputValues);
//            foreach (string outputValue in outputValues)
//            {
//                context.Forward(inputKey, outputValue);
//            }
//            replayAll();

//            processor.Process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldEmitNoRecordIfTransformReturnsEmptyList()
//        {
//            processor.Init(context);
//            //EasyMock.reset(valueTransformer);

//            EasyMock.expect(valueTransformer.Transform(inputKey, inputValue)).andReturn(new List<string>());
//            //replayAll();

//            processor.Process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldEmitNoRecordIfTransformReturnsNull()
//        {
//            processor.Init(context);
//            //EasyMock.reset(valueTransformer);

//            EasyMock.expect(valueTransformer.Transform(inputKey, inputValue)).andReturn(null);
//            //replayAll();

//            processor.Process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldCloseFlatTransformValuesProcessor()
//        {
//            valueTransformer.Close();
//            //replayAll();

//            processor.Close();

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldGetFlatTransformValuesProcessor()
//        {
//            IValueTransformerWithKeySupplier<int, int, IEnumerable<string>> valueTransformerSupplier =
//                Mock.Of<IValueTransformerWithKeySupplier<int, int, IEnumerable<string>>>();

//            KStreamFlatTransformValues<int, int, string> processorSupplier =
//                new KStreamFlatTransformValues<int, int, string>(valueTransformerSupplier);

//            EasyMock.expect(valueTransformerSupplier.Get()).andReturn(valueTransformer);
//            // replayAll();

//            var processor = processorSupplier.Get();

//            //verifyAll();
//            Assert.True(processor is KStreamFlatTransformValuesProcessor<int, int, string>);
//        }
//    }
//}
