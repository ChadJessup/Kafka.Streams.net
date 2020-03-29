//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Tests.Helpers;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamSelectKeyTest
//    {
//        private string topicName = "topic_key_select";
//        private ConsumerRecordFactory<string, int> recordFactory = null;
//            //new ConsumerRecordFactory<>(topicName, Serdes.String(), Serdes.Int(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.Int());

//        [Fact]
//        public void testSelectKey()
//        {
//            var builder = new StreamsBuilder();

//            var keyMap = new Dictionary<int, string>();
//            keyMap.Add(1, "ONE");
//            keyMap.Add(2, "TWO");
//            keyMap.Add(3, "THREE");

//            var expected = new KeyValueTimestamp[]{new KeyValueTimestamp<>("ONE", 1, 0),
//            new KeyValueTimestamp<>("TWO", 2, 0),
//            new KeyValueTimestamp<>("THREE", 3, 0)};
//            var expectedValues = new int[] { 1, 2, 3 };

//            IKStream<string, int> stream =
//                builder.Stream(topicName, Consumed.with(Serdes.String(), Serdes.Int()));
//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            stream.selectKey<string>((key, value) => keyMap.get(value)).process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedValue in expectedValues)
//            {
//                driver.pipeInput(recordFactory.create(expectedValue));
//            }

//            Assert.Equal(3, supplier.theCapturedProcessor().processed.Count);
//            for (var i = 0; i < expected.Length; i++)
//            {
//                Assert.Equal(expected[i], supplier.theCapturedProcessor().processed.get(i));
//            }

//        }

//        [Fact]
//        public void testTypeVariance()
//        {
//            new StreamsBuilder()
//                .Stream<int, string>("empty")
//                .ForEach((key, value) => { });
//        }
//    }
//}
