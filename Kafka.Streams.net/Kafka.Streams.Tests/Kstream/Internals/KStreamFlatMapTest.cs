using Confluent.Kafka;
using Kafka.Streams;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Tests;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapTest
    {
        private readonly ConsumerRecordFactory<int, string> recordFactory =
            new ConsumerRecordFactory<int, string>(Serializers.Int32, Serializers.Utf8, 0L);
        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void TestFlatMap()
        {
            var builder = new StreamsBuilder();
            var topicName = "topic";

            var mapper = new KeyValueMapper<int, string, IEnumerable<KeyValuePair<string, string>>>(
                (key, value) =>
                {
                    var result = new List<KeyValuePair<string, string>>();
                    for (var i = 0; i < key; i++)
                    {
                        result.Add(KeyValuePair.Create((key * 10 + i).ToString(), value?.ToString() ?? string.Empty));
                    }

                    return result;
                });

            int[] expectedKeys = { 0, 1, 2, 3 };

            var supplier = new MockProcessorSupplier<string, string>();
            IKStream<int, string> stream = builder.Stream(topicName, Consumed.With<int, string>(Serdes.Int(), Serdes.String()));

            stream.FlatMap(mapper).Process(supplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, "V" + expectedKey));
            }

            Assert.Equal(6, supplier.TheCapturedProcessor().processed.Count);

            var expected = new KeyValueTimestamp<string, string>[]
            {
                new KeyValueTimestamp<string, string>("10", "V1", 0),
                new KeyValueTimestamp<string, string>("20", "V2", 0),
                new KeyValueTimestamp<string, string>("21", "V2", 0),
                new KeyValueTimestamp<string, string>("30", "V3", 0),
                new KeyValueTimestamp<string, string>("31", "V3", 0),
                new KeyValueTimestamp<string, string>("32", "V3", 0),
            };

            for (var i = 0; i < expected.Length; i++)
            {
                var expectedKvp = expected[i];
                var actualKvp = supplier.TheCapturedProcessor().processed[i];
                Assert.Equal(expectedKvp, actualKvp);
            }
        }
    }
}
