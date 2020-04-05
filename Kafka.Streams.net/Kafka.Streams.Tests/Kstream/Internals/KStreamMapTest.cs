namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Tests.Helpers;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamMapTest
//    {
//        private ConsumerRecordFactory<int, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

//        [Fact]
//        public void testMap()
//        {
//            var builder = new StreamsBuilder();
//            var topicName = "topic";
//            var expectedKeys = new int[] { 0, 1, 2, 3 };

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            IKStream<int, string> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
//            stream.map((key, value) => KeyValuePair.Create(value, key)).process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.PipeInput(recordFactory.Create(topicName, expectedKey, "V" + expectedKey, 10L - expectedKey));
//            }


//            var expected = new[] KeyValueTimestamp {new KeyValueTimestamp<>("V0", 0, 10),
//            new KeyValueTimestamp<>("V1", 1, 9),
//            new KeyValueTimestamp<>("V2", 2, 8),
//            new KeyValueTimestamp<>("V3", 3, 7)};
//            Assert.Equal(4, supplier.theCapturedProcessor().processed.Count);
//            for (var i = 0; i < expected.Length; i++)
//            {
//                Assert.Equal(expected[i], supplier.theCapturedProcessor().processed.Get(i));
//            }
//        }

//        [Fact]
//        public void testTypeVariance()
//        {
//            new StreamsBuilder()
//                .Stream<int, string>("numbers")
//                .map((key, value) => KeyValuePair.Create(key, key + ":" + value))
//                .To("strings");
//        }
//    }
//}
