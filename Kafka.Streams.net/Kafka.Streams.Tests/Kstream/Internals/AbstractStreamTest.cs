using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processors;
using System;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class AbstractStreamTest
    {

        [Fact]
        public void testToInternalValueTransformerSupplierSuppliesNewTransformers()
        {
            IValueTransformerSupplier<object, object> valueTransformerSupplier = createMock(IValueTransformerSupplier));
            expect(valueTransformerSupplier.Get()).andReturn(null).times(3);
            ValueTransformerWithKeySupplier<object, object, object> valueTransformerWithKeySupplier =
                 AbstractStream.toValueTransformerWithKeySupplier(valueTransformerSupplier);
            replay(valueTransformerSupplier);
            valueTransformerWithKeySupplier.Get();
            valueTransformerWithKeySupplier.Get();
            valueTransformerWithKeySupplier.Get();
            verify(valueTransformerSupplier);
        }

        [Fact]
        public void testToInternalValueTransformerWithKeySupplierSuppliesNewTransformers()
        {
            IValueTransformerWithKeySupplier<object, object, object> valueTransformerWithKeySupplier =
            createMock(typeof(IValueTransformerWithKeySupplier));
            expect(valueTransformerWithKeySupplier.Get()).andReturn(null).times(3);
            replay(valueTransformerWithKeySupplier);
            valueTransformerWithKeySupplier.Get();
            valueTransformerWithKeySupplier.Get();
            valueTransformerWithKeySupplier.Get();
            verify(valueTransformerWithKeySupplier);
        }

        [Fact]
        public void testShouldBeExtensible()
        {
            var builder = new StreamsBuilder();
            var expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };
            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
            var topicName = "topic";

            ExtendedIIKStream<K, V>(builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String())));

            stream.randomFilter().Process(supplier);

            var props = new StreamsConfig();
            props.Set(StreamsConfig.ApplicationId, "abstract-stream-test");
            props.Set(StreamsConfig.BootstrapServers, "localhost:9091");
            props.Set(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory());

            ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String());
            var driver = new TopologyTestDriver(builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, "V" + expectedKey));
            }

            Assert.True(supplier.TheCapturedProcessor().processed.Count <= expectedKeys.Length);
        }

        private class ExtendedIIKStream<K, V>
        {

            ExtendedKStream(IKStream<K, V> stream)
                : base((KStream<K, V>)stream)
            {
            }

            public IKStream<K, V> randomFilter()
            {
                string Name = builder.NewProcessorName("RANDOM-FILTER-");
                ProcessorGraphNode<K, V> processorNode = new ProcessorGraphNode<>(
                    Name,
                    new ProcessorParameters<>(new ExtendedKStreamDummy<>(), Name));
                builder.AddGraphNode<K, V>(this.streamsGraphNode, processorNode);
                return new KStream<K, V>(Name, null, null, sourceNodes, false, processorNode, builder);
            }
        }

        public class ExtendedKStreamDummy<K, V> : IProcessorSupplier<K, V>
        {

            private Random rand;

            ExtendedKStreamDummy()
            {
                rand = new Random();
            }


            public Processor<K, V> get()
            {
                return new ExtendedKStreamDummyProcessor();
            }

            private class ExtendedKStreamDummyProcessor : AbstractProcessor<K, V>
            {

                public void process(K key, V value)
                {
                    // flip a coin and filter
                    if (rand.nextBoolean())
                    {
                        context.Forward(key, value);
                    }
                }
            }
        }
    }
}
