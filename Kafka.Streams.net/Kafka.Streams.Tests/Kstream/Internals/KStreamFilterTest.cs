using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{


















    public class KStreamFilterTest
    {

        private readonly string topicName = "topic";
        private readonly ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<int, string>(Serdes.Int(), Serdes.String());
        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        private readonly FilterPredicate<int, string> isMultipleOfThree = (key, value) => (key % 3) == 0;

        [Fact]
        public void TestFilter()
        {
            var builder = new StreamsBuilder();
            var expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

            IKStream<int, string> stream;
            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<int, string>();

            stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            stream.Filter(isMultipleOfThree).Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, "V" + expectedKey));
            }

            Assert.Equal(2, supplier.TheCapturedProcessor().processed.Count);
        }

        [Fact]
        public void testFilterNot()
        {
            var builder = new StreamsBuilder();
            var expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

            IKStream<int, string> stream;
            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<int, string>();

            stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            stream.FilterNot(isMultipleOfThree).Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, "V" + expectedKey));
            }

            Assert.Equal(5, supplier.TheCapturedProcessor().processed.Count);
        }

        [Fact]
        public void testTypeVariance()
        {
            static bool numberKeyPredicate(int key, string value) => false;

            new StreamsBuilder()
                .Stream<int, string>("empty")
                .Filter(numberKeyPredicate)
                .FilterNot(numberKeyPredicate)
                .To("nirvana");

        }
    }
}
