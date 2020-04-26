using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Temporary;
using Moq;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamFlatTransformTest // : EasyMockSupport
    {

        private int inputKey;
        private int inputValue;

        private Transformer<int, int, Iterable<KeyValuePair<int, int>>> transformer;
        private IProcessorContext context;

        private KStreamFlatTransformProcessor<int, int, int, int> processor;


        public void setUp()
        {
            inputKey = 1;
            inputValue = 10;
            transformer = Mock.Of<Transformer>());
            context = strictMock(IProcessorContext));
            processor = new KStreamFlatTransformProcessor<>(transformer);
        }

        [Fact]
        public void shouldInitialiseFlatTransformProcessor()
        {
            transformer.Init(context);
            replayAll();

            processor.Init(context);

            //verifyAll();
        }

        [Fact]
        public void shouldTransformInputRecordToMultipleOutputRecords()
        {
            Iterable<KeyValuePair<int, int>> outputRecords = Array.AsReadOnly(
                    KeyValuePair.Create(2, 20),
                    KeyValuePair.Create(3, 30),
                    KeyValuePair.Create(4, 40));
            processor.Init(context);
            EasyMock.reset(transformer);

            EasyMock.expect(transformer.transform(inputKey, inputValue)).andReturn(outputRecords);
            foreach (KeyValuePair<int, int> outputRecord in outputRecords)
            {
                context.Forward(outputRecord.Key, outputRecord.Value);
            }
            replayAll();

            processor.Process(inputKey, inputValue);

            //verifyAll();
        }

        [Fact]
        public void shouldAllowEmptyListAsResultOfTransform()
        {
            processor.Init(context);
            EasyMock.reset(transformer);

            EasyMock.expect(transformer.transform(inputKey, inputValue))
                .andReturn(Collections.emptyList<KeyValuePair<int, int>>());
            replayAll();

            processor.Process(inputKey, inputValue);

            //verifyAll();
        }

        [Fact]
        public void shouldAllowNullAsResultOfTransform()
        {
            processor.Init(context);
            EasyMock.reset(transformer);

            EasyMock.expect(transformer.transform(inputKey, inputValue))
                .andReturn(null);
            replayAll();

            processor.Process(inputKey, inputValue);

            //verifyAll();
        }

        [Fact]
        public void shouldCloseFlatTransformProcessor()
        {
            transformer.Close();
            replayAll();

            processor.Close();

            //verifyAll();
        }

        [Fact]
        public void shouldGetFlatTransformProcessor()
        {
            ITransformerSupplier<int, int, IEnumerable<KeyValuePair<int, int>>> transformerSupplier =
        Mock.Of<ITransformerSupplier<int, int, IEnumerable<KeyValuePair<int, int>>>>();

            KStreamFlatTransform<int, int, int, int> processorSupplier =
                new KStreamFlatTransform<int, int, int, int>(transformerSupplier);

            // EasyMock.expect(transformerSupplier.Get()).andReturn(transformer);
            // replayAll();

            var processor = processorSupplier.Get();

            //verifyAll();
            Assert.True(processor is IKStreamFlatTransformProcessor);
        }
    }
}
