//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.KStream.Internals.Graph;
//using Kafka.Streams.Processors;
//using System;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class AbstractStreamTest
//    {

//        [Fact]
//        public void testToInternalValueTransformerSupplierSuppliesNewTransformers()
//        {
//            ValueTransformerSupplier<object, object> valueTransformerSupplier = createMock(ValueTransformerSupplier));
//            expect(valueTransformerSupplier.get()).andReturn(null).times(3);
//            ValueTransformerWithKeySupplier<object, object, object> valueTransformerWithKeySupplier =
//                 AbstractStream.toValueTransformerWithKeySupplier(valueTransformerSupplier);
//            replay(valueTransformerSupplier);
//            valueTransformerWithKeySupplier.get();
//            valueTransformerWithKeySupplier.get();
//            valueTransformerWithKeySupplier.get();
//            verify(valueTransformerSupplier);
//        }

//        [Fact]
//        public void testToInternalValueTransformerWithKeySupplierSuppliesNewTransformers()
//        {
//            IValueTransformerWithKeySupplier<object, object, object> valueTransformerWithKeySupplier =
//            createMock(typeof(IValueTransformerWithKeySupplier));
//            expect(valueTransformerWithKeySupplier.get()).andReturn(null).times(3);
//            replay(valueTransformerWithKeySupplier);
//            valueTransformerWithKeySupplier.get();
//            valueTransformerWithKeySupplier.get();
//            valueTransformerWithKeySupplier.get();
//            verify(valueTransformerWithKeySupplier);
//        }

//        [Fact]
//        public void testShouldBeExtensible()
//        {
//            var builder = new StreamsBuilder();
//            var expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
//            var topicName = "topic";

//            ExtendedKStream<int, string> stream = new ExtendedKStream<>(builder.Stream(topicName, Consumed.with(Serdes.Int(), Serdes.String())));

//            stream.randomFilter().process(supplier);

//            var props = new StreamsConfig();
//            props.Set(StreamsConfigPropertyNames.ApplicationId, "abstract-stream-test");
//            props.Set(StreamsConfigPropertyNames.BootstrapServers, "localhost:9091");
//            props.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory());

//            ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String());
//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topicName, expectedKey, "V" + expectedKey));
//            }

//            Assert.True(supplier.theCapturedProcessor().processed.Count <= expectedKeys.Length);
//        }

//        private class ExtendedKStream<K, V> : AbstractStream<K, V>
//        {

//            ExtendedKStream(IKStream<K, V> stream)
//                : base((KStream<K, V>)stream)
//            {
//            }

//            public IKStream<K, V> randomFilter()
//            {
//                string name = builder.NewProcessorName("RANDOM-FILTER-");
//                ProcessorGraphNode<K, V> processorNode = new ProcessorGraphNode<>(
//                    name,
//                    new ProcessorParameters<>(new ExtendedKStreamDummy<>(), name));
//                builder.AddGraphNode<K, V>(this.streamsGraphNode, processorNode);
//                return new KStream<K, V>(name, null, null, sourceNodes, false, processorNode, builder);
//            }
//        }

//        public class ExtendedKStreamDummy<K, V> : IProcessorSupplier<K, V>
//        {

//            private Random rand;

//            ExtendedKStreamDummy()
//            {
//                rand = new Random();
//            }


//            public Processor<K, V> get()
//            {
//                return new ExtendedKStreamDummyProcessor();
//            }

//            private class ExtendedKStreamDummyProcessor : AbstractProcessor<K, V>
//            {

//                public void process(K key, V value)
//                {
//                    // flip a coin and filter
//                    if (rand.nextBoolean())
//                    {
//                        context().forward(key, value);
//                    }
//                }
//            }
//        }
//    }
//}
