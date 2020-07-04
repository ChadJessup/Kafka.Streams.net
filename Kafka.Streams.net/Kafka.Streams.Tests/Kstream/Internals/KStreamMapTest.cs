using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamMapTest
    {
        private readonly ConsumerRecordFactory<int, string> recordFactory =
            new ConsumerRecordFactory<int, string>(Serdes.Int().Serializer, Serdes.String().Serializer, 0L);
        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void TestMap()
        {
            var builder = new StreamsBuilder();
            var topicName = "topic";
            var expectedKeys = new int[] { 0, 1, 2, 3 };

            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<string, int>();
            IKStream<int, string> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            stream.Map((key, value) => KeyValuePair.Create(value, key)).Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, "V" + expectedKey, 10L - expectedKey));
            }

            var expected = new KeyValueTimestamp<string, int>[]
            {
                new KeyValueTimestamp<string, int>("V0", 0, 10),
                new KeyValueTimestamp<string, int>("V1", 1, 9),
                new KeyValueTimestamp<string, int>("V2", 2, 8),
                new KeyValueTimestamp<string, int>("V3", 3, 7),
            };
            
            Assert.Equal(4, supplier.TheCapturedProcessor().processed.Count);
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], supplier.TheCapturedProcessor().processed.ElementAt(i));
            }
        }

        [Fact]
        public void TestTypeVariance()
        {
            new StreamsBuilder()
                .Stream<int, string>("numbers")
                .Map((key, value) => KeyValuePair.Create(key, key + ":" + value))
                .To("strings");
        }
    }
}
