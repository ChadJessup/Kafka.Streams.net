//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.KStream.Internals.Graph;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using Moq;
//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream.Internals
//{
//    public class AbstractStreamTest
//    {

//        [Fact]
//        public void testToInternalValueTransformerSupplierSuppliesNewTransformers()
//        {
//            IValueTransformerSupplier<object, object> valueTransformerSupplier = Mock.Of<IValueTransformerSupplier<object, object>>();
//            expect(valueTransformerSupplier.Get()).andReturn(null).times(3);

//            IValueTransformerWithKeySupplier<object, object, object> valueTransformerWithKeySupplier =
//                 AbstractStream<object, object>.ToValueTransformerWithKeySupplier(valueTransformerSupplier);

//            replay(valueTransformerSupplier);
//            valueTransformerWithKeySupplier.Get();
//            valueTransformerWithKeySupplier.Get();
//            valueTransformerWithKeySupplier.Get();
//            verify(valueTransformerSupplier);
//        }

//        [Fact]
//        public void testToInternalValueTransformerWithKeySupplierSuppliesNewTransformers()
//        {
//            IValueTransformerWithKeySupplier<object, object, object> valueTransformerWithKeySupplier =
//            Mock.Of<IValueTransformerWithKeySupplier<object, object, object>>();

//            expect(valueTransformerWithKeySupplier.Get()).andReturn(null).times(3);
//            replay(valueTransformerWithKeySupplier);
//            valueTransformerWithKeySupplier.Get();
//            valueTransformerWithKeySupplier.Get();
//            valueTransformerWithKeySupplier.Get();
//            verify(valueTransformerWithKeySupplier);
//        }

//        [Fact]
//        public void testShouldBeExtensible()
//        {
//            var builder = new StreamsBuilder();
//            var expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<int, string>();
//            var topicName = "topic";

//            ExtendedKStream<int, string> stream = builder.Stream(
//                topicName, 
//                Consumed.With(Serdes.Int(), Serdes.String()));

//            stream.randomFilter().Process(supplier);

//            var props = new StreamsConfig();
//            props.ApplicationId = "abstract-stream-test";
//            props.BootstrapServers = "localhost:9091";
//            props.StateStoreDirectory = TestUtils.GetTempDirectory();

//            ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<int, string>(Serdes.Int(), Serdes.String());
//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.PipeInput(recordFactory.Create(topicName, expectedKey, "V" + expectedKey));
//            }

//            Assert.True(supplier.TheCapturedProcessor().processed.Count <= expectedKeys.Length);
//        }

//        private class ExtendedKStream<K, V> : AbstractStream<K, V>
//        {
//            public ExtendedKStream(IKStream<K, V> stream)
//                : base((KStream<K, V>)stream)
//            {
//            }

//            public IKStream<K, V> randomFilter()
//            {
//                string Name = builder.NewProcessorName("RANDOM-FILTER-");
//                ProcessorGraphNode<K, V> processorNode = new ProcessorGraphNode<K, V>(
//                    Name,
//                    new ProcessorParameters<K, V>(new ExtendedKStreamDummy<>(), Name));

//                builder.AddGraphNode<K, V>(this.StreamsGraphNode, processorNode);
//                return new KStream<K, V>(
//                    Name,
//                    null,
//                    null,
//                    sourceNodes,
//                    false,
//                    processorNode,
//                    builder);
//            }
//        }

//        public class ExtendedKStreamDummy<K, V> : IProcessorSupplier<K, V>
//        {
//            private Random rand;

//            public ExtendedKStreamDummy()
//            {
//                rand = new Random();
//            }

//            public IKeyValueProcessor Get()
//            {
//                return new ExtendedKStreamDummyProcessor();
//            }

//            IKeyValueProcessor<K, V> IProcessorSupplier<K, V>.Get()
//            {
//                throw new NotImplementedException();
//            }

//            private class ExtendedKStreamDummyProcessor : AbstractProcessor<K, V>
//            {
//                public override void Process(K key, V value)
//                {
//                    // flip a coin and filter
//                    if (rand.nextBoolean())
//                    {
//                        context.Forward(key, value);
//                    }
//                }
//            }
//        }
//    }
//}
