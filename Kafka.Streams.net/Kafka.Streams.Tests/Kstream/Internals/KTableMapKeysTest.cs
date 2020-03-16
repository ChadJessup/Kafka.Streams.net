//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Tests.Helpers;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class KTableMapKeysTest
//    {
//        private ConsumerRecordFactory<int, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

//        [Fact]
//        public void testMapKeysConvertingToStream()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic_map_keys";

//            IKTable<int, string> table1 = builder.Table(topic1, Consumed.with(Serdes.Int(), Serdes.String()));

//            Dictionary<int, string> keyMap = new HashMap<>();
//            keyMap.put(1, "ONE");
//            keyMap.put(2, "TWO");
//            keyMap.put(3, "THREE");

//            IKStream<string, string> convertedStream = table1.toStream((key, value) => keyMap.get(key));

//            var expected = new[] KeyValueTimestamp {new KeyValueTimestamp<>("ONE", "V_ONE", 5),
//            new KeyValueTimestamp<>("TWO", "V_TWO", 10),
//            new KeyValueTimestamp<>("THREE", "V_THREE", 15)};
//            var originalKeys = new int[] { 1, 2, 3 };
//            var values = new string[] { "V_ONE", "V_TWO", "V_THREE" };

//            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<>();
//            convertedStream.process(supplier);

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                for (var i = 0; i < originalKeys.Length; i++)
//                {
//                    driver.pipeInput(recordFactory.create(topic1, originalKeys[i], values[i], 5 + i * 5));
//                }
//            }

//       Assert.Equal(3, supplier.theCapturedProcessor().processed.size());
//            for (var i = 0; i < expected.Length; i++)
//            {
//                Assert.Equal(expected[i], supplier.theCapturedProcessor().processed.get(i));
//            }
//        }
//    }