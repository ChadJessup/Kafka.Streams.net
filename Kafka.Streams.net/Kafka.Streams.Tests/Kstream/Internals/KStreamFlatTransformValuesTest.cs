using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{

    public class KStreamFlatTransformValuesTest
    {

        private int inputKey;
        private int inputValue;

        private IValueTransformerWithKey<int, int, Iterable<string>> valueTransformer;
        private IProcessorContext context;

        private KStreamFlatTransformValuesProcessor<int, int, string> processor;


        public void setUp()
        {
            inputKey = 1;
            inputValue = 10;
            valueTransformer = Mock.Of<typeof(IValueTransformerWithKey));
            context = strictMock(typeof(IProcessorContext));
            processor = new KStreamFlatTransformValuesProcessor<>(valueTransformer);
        }

        [Fact]
        public void shouldInitializeFlatTransformValuesProcessor()
        {
            valueTransformer.Init(typeof(ForwardingDisabledProcessorContext));
            replayAll();

            processor.Init(context);

            //verifyAll();
        }

        [Fact]
        public void shouldTransformInputRecordToMultipleOutputValues()
        {
            Iterable<string> outputValues = Array.AsReadOnly(
                    "Hello",
                    "Blue",
                    "Planet");
            processor.Init(context);
            EasyMock.reset(valueTransformer);

            EasyMock.expect(valueTransformer.transform(inputKey, inputValue)).andReturn(outputValues);
            foreach (string outputValue in outputValues)
            {
                context.Forward(inputKey, outputValue);
            }
            replayAll();

            processor.Process(inputKey, inputValue);

            //verifyAll();
        }

        [Fact]
        public void shouldEmitNoRecordIfTransformReturnsEmptyList()
        {
            processor.Init(context);
            EasyMock.reset(valueTransformer);

            EasyMock.expect(valueTransformer.transform(inputKey, inputValue)).andReturn(new List<string>());
            replayAll();

            processor.Process(inputKey, inputValue);

            //verifyAll();
        }

        [Fact]
        public void shouldEmitNoRecordIfTransformReturnsNull()
        {
            processor.Init(context);
            EasyMock.reset(valueTransformer);

            EasyMock.expect(valueTransformer.transform(inputKey, inputValue)).andReturn(null);
            replayAll();

            processor.Process(inputKey, inputValue);

            //verifyAll();
        }

        [Fact]
        public void shouldCloseFlatTransformValuesProcessor()
        {
            valueTransformer.Close();
            replayAll();

            processor.Close();

            //verifyAll();
        }

        [Fact]
        public void shouldGetFlatTransformValuesProcessor()
        {
            IValueTransformerWithKeySupplier<int, int, Iterable<string>> valueTransformerSupplier =
                Mock.Of<typeof(IValueTransformerWithKeySupplier));
            KStreamFlatTransformValues<int, int, string> processorSupplier =
                new KStreamFlatTransformValues<>(valueTransformerSupplier);

            EasyMock.expect(valueTransformerSupplier.Get()).andReturn(valueTransformer);
            replayAll();

            Processor<int, int> processor = processorSupplier.Get();

            //verifyAll();
            Assert.True(processor is KStreamFlatTransformValuesProcessor);
        }
    }
}
