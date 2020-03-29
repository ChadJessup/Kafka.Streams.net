///*
//
//
//
//
//
//
// *
//
// *
//
//
//
//
//
// */
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Mappers;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{



















//    public class KStreamMapValuesTest
//    {
//        private string topicName = "topic";
//        private MockProcessorSupplier<int, int> supplier = new MockProcessorSupplier<>();
//        private ConsumerRecordFactory<int, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

//        [Fact]
//        public void testFlatMapValues()
//        {
//            var builder = new StreamsBuilder();

//            int[] expectedKeys = { 1, 10, 100, 1000 };

//            IKStream<int, string> stream = builder.Stream(topicName, Consumed.with(Serdes.Int(), Serdes.String()));
//            stream.mapValues(CharSequence).process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey.ToString(), expectedKey / 2L));
//            }

//            var expected = new KeyValueTimestamp[]
//            {
//                new KeyValueTimestamp<>(1, 1, 0),
//                new KeyValueTimestamp<>(10, 2, 5),
//                new KeyValueTimestamp<>(100, 3, 50),
//                new KeyValueTimestamp<>(1000, 4, 500),
//            };

//            Assert.Equal(expected, supplier.theCapturedProcessor().processed.ToArray());
//        }

//        [Fact]
//        public void testMapValuesWithKeys()
//        {
//            var builder = new StreamsBuilder();

//            ValueMapperWithKey<int, CharSequence, int> mapper =
//                (readOnlyKey, value) => value.Length() + readOnlyKey;

//            int[] expectedKeys = { 1, 10, 100, 1000 };

//            IKStream<int, string> stream = builder.Stream(topicName, Consumed.with(Serdes.Int(), Serdes.String()));
//            stream.mapValues(mapper).process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topicName, expectedKey, int.ToString(expectedKey), expectedKey / 2L));
//            }

//            KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(1, 2, 0),
//            new KeyValueTimestamp<>(10, 12, 5),
//            new KeyValueTimestamp<>(100, 103, 50),
//            new KeyValueTimestamp<>(1000, 1004, 500)};

//            Assert.Equal(expected, supplier.theCapturedProcessor().processed.ToArray());
//        }

//    }
//}
