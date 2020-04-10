namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class KStreamTransformValuesTest
//    {
//        private string topicName = "topic";
//        private MockProcessorSupplier<int, int> supplier = new MockProcessorSupplier<>();
//        private ConsumerRecordFactory<int, int> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.Int(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.Int());
//        private IProcessorContext context;

//        [Fact]
//        public void testTransform()
//        {
//            var builder = new StreamsBuilder();

//            IValueTransformerSupplier<int, int> valueTransformerSupplier = null;
//            //            () => new ValueTransformer<int, int>()
//            //            {
//            //                private int total = 0;


//            //    public void Init(IProcessorContext context) { }


//            //    public int transform(int value)
//            //    {
//            //        total += value.intValue();
//            //        return total;
//            //    }


//            //    public void Close() { }
//            //};

//            int[] expectedKeys = { 1, 10, 100, 1000 };

//            IKStream<int, int> stream;
//            stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.Int()));
//            stream.transformValues(valueTransformerSupplier).process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.PipeInput(recordFactory.Create(topicName, expectedKey, expectedKey * 10, expectedKey / 2L));
//            }
//            //}

//            var expected = new[] KeyValueTimestamp
//            {
//            new KeyValueTimestamp<>(1, 10, 0),
//            new KeyValueTimestamp<>(10, 110, 5),
//            new KeyValueTimestamp<>(100, 1110, 50),
//            new KeyValueTimestamp<>(1000, 11110, 500),
//            };

//            Assert.Equal(expected, supplier.theCapturedProcessor().processed.ToArray());
//        }

//        [Fact]
//        public void testTransformWithKey()
//        {
//            var builder = new StreamsBuilder();

//            ValueTransformerWithKeySupplier<int, int, int> valueTransformerSupplier = null;
//            //            () => new ValueTransformerWithKey<int, int, int>()
//            //            {
//            //                    private int total = 0;


//            //    public void Init(IProcessorContext context) { }


//            //    public int transform(int readOnlyKey, int value)
//            //    {
//            //        total += value.intValue() + readOnlyKey;
//            //        return total;
//            //    }


//            //    public void Close() { }
//            //};

//            int[] expectedKeys = { 1, 10, 100, 1000 };

//            IKStream<int, int> stream;
//            stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.Int()));
//            stream.transformValues(valueTransformerSupplier).process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.PipeInput(recordFactory.Create(topicName, expectedKey, expectedKey * 10, expectedKey / 2L));
//            }
//            //}

//            var expected = new[] KeyValueTimestamp
//            {
//            new KeyValueTimestamp<>(1, 11, 0),
//            new KeyValueTimestamp<>(10, 121, 5),
//            new KeyValueTimestamp<>(100, 1221, 50),
//            new KeyValueTimestamp<>(1000, 12221, 500),
//            };

//            Assert.Equal(expected, supplier.theCapturedProcessor().processed.ToArray());
//        }


//        [Fact]
//        public void shouldInitializeTransformerWithForwardDisabledProcessorContext()
//        {
//            SingletonNoOpValueTransformer<string, string> transformer = new SingletonNoOpValueTransformer<>();
//            KStreamTransformValues<string, string, string> transformValues = new KStreamTransformValues<>(transformer);
//            Processor<string, string> processor = transformValues.Get();

//            processor.Init(context);

//            Assert.Equal(transformer.context, typeof(ForwardingDisabledProcessorContext<string, string>));
//        }
//    }
