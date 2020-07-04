using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamMapValuesTest
    {
        private readonly string topicName = "topic";
        private readonly MockProcessorSupplier<int, int> supplier = new MockProcessorSupplier<int, int>();
        private readonly ConsumerRecordFactory<int, string> recordFactory =
            new ConsumerRecordFactory<int, string>(Serdes.Int(), Serdes.String(), 0L);
        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void testFlatMapValues()
        {
            var builder = new StreamsBuilder();

            int[] expectedKeys = { 1, 10, 100, 1000 };

            IKStream<int, string> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            // stream.MapValues(CharSequence).Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, expectedKey.ToString(), expectedKey / 2L));
            }

            var expected = new KeyValueTimestamp<int, int>[]
            {
                new KeyValueTimestamp<int, int>(1, 1, 0),
                new KeyValueTimestamp<int, int>(10, 2, 5),
                new KeyValueTimestamp<int, int>(100, 3, 50),
                new KeyValueTimestamp<int, int>(1000, 4, 500),
            };

            Assert.Equal(expected, supplier.TheCapturedProcessor().processed.ToArray());
        }

        [Fact]
        public void testMapValuesWithKeys()
        {
            var builder = new StreamsBuilder();

            ValueMapperWithKey<int, string, int> mapper =
                (readOnlyKey, value) => value.Length + readOnlyKey;

            int[] expectedKeys = { 1, 10, 100, 1000 };

            IKStream<int, string> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            stream.MapValues(mapper).Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, expectedKey.ToString(), expectedKey / 2L));
            }

            KeyValueTimestamp<int, int>[] expected = 
            {
                new KeyValueTimestamp<int, int>(1, 2, 0),
                new KeyValueTimestamp<int, int>(10, 12, 5),
                new KeyValueTimestamp<int, int>(100, 103, 50),
                new KeyValueTimestamp<int, int>(1000, 1004, 500),
            };

            Assert.Equal(expected, supplier.TheCapturedProcessor().processed.ToArray());
        }
    }
}
