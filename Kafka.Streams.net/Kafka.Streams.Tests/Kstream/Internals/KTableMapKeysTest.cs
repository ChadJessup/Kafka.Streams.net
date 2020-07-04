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

    public class KTableMapKeysTest
    {
        private readonly ConsumerRecordFactory<int, string> recordFactory =
            new ConsumerRecordFactory<int, string>(Serdes.Int(), Serdes.String(), 0L);

        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void testMapKeysConvertingToStream()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic_map_keys";

            IKTable<int, string> table1 = builder.Table(topic1, Consumed.With(Serdes.Int(), Serdes.String()));

            Dictionary<int, string> keyMap = new Dictionary<int, string>();
            keyMap.Put(1, "ONE");
            keyMap.Put(2, "TWO");
            keyMap.Put(3, "THREE");

            IKStream<string, string> convertedStream = table1.ToStream((key, value) => keyMap[key]);

            var expected = new KeyValueTimestamp<string, string>[]
            {
                new KeyValueTimestamp<string, string>("ONE", "V_ONE", 5),
                new KeyValueTimestamp<string, string>("TWO", "V_TWO", 10),
                new KeyValueTimestamp<string, string>("THREE", "V_THREE", 15),
            };

            var originalKeys = new int[] { 1, 2, 3 };
            var values = new string[] { "V_ONE", "V_TWO", "V_THREE" };

            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<string, string>();
            convertedStream.Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            for (var i = 0; i < originalKeys.Length; i++)
            {
                driver.PipeInput(recordFactory.Create(topic1, originalKeys[i], values[i], 5 + i * 5));
            }

            Assert.Equal(3, supplier.TheCapturedProcessor().processed.Count);
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], supplier.TheCapturedProcessor().processed.ElementAt(i));
            }
        }
    }
}
