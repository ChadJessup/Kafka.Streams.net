using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamSelectKeyTest
    {
        private readonly string topicName = "topic_key_select";
        private readonly ConsumerRecordFactory<string, int> recordFactory;
        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.Int());

        public KStreamSelectKeyTest()
        {
            recordFactory = new ConsumerRecordFactory<string, int>(
                topicName,
                Serdes.String().Serializer,
                Serdes.Int().Serializer,
                0L);
        }

        [Fact]
        public void TestSelectKey()
        {
            var builder = new StreamsBuilder();

            var keyMap = new Dictionary<int, string>
            {
                { 1, "ONE" },
                { 2, "TWO" },
                { 3, "THREE" }
            };

            var expected = new KeyValueTimestamp<string, int>[]
            {
                new KeyValueTimestamp<string, int>("ONE", 1, 0),
                new KeyValueTimestamp<string, int>("TWO", 2, 0),
                new KeyValueTimestamp<string, int>("THREE", 3, 0),
            };

            var expectedValues = new int[] { 1, 2, 3 };

            IKStream<string, int> stream =
                builder.Stream(topicName, Consumed.With(Serdes.String(), Serdes.Int()));
            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<string, int>();

            stream.SelectKey((key, value) => keyMap[value]).Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            foreach (var expectedValue in expectedValues)
            {
                driver.PipeInput(recordFactory.Create(expectedValue));
            }

            Assert.Equal(3, supplier.TheCapturedProcessor().processed.Count);
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], supplier.TheCapturedProcessor().processed[i]);
            }
        }

        [Fact]
        public void testTypeVariance()
        {
            new StreamsBuilder()
                .Stream<int, string>("empty")
                .ForEach((key, value) => { });
        }
    }
}
