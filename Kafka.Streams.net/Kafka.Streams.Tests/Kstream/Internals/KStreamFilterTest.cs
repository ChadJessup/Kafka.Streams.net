namespace Kafka.Streams.Tests.Kstream.Internals
{
}
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
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;

//namespace Kafka.Streams.KStream.Internals
//{


















//    public class KStreamFilterTest
//    {

//        private string topicName = "topic";
//        private ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String());
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

//        private IPredicate<int, string> isMultipleOfThree = (key, value) => (key % 3) == 0;

//        [Fact]
//        public void testFilter()
//        {
//            var builder = new StreamsBuilder();
//            var expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

//            IKStream<int, string> stream;
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();

//            stream = builder.Stream(topicName, Consumed.with(Serdes.Int(), Serdes.String()));
//            stream.filter(isMultipleOfThree).process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topicName, expectedKey, "V" + expectedKey));
//            }

//            Assert.Equal(2, supplier.theCapturedProcessor().processed.Count);
//        }

//        [Fact]
//        public void testFilterNot()
//        {
//            var builder = new StreamsBuilder();
//            var expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

//            IKStream<int, string> stream;
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();

//            stream = builder.Stream(topicName, Consumed.with(Serdes.Int(), Serdes.String()));
//            stream.filterNot(isMultipleOfThree).process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topicName, expectedKey, "V" + expectedKey));
//            }

//            Assert.Equal(5, supplier.theCapturedProcessor().processed.Count);
//        }

//        [Fact]
//        public void testTypeVariance()
//        {
//            IPredicate<int, object> numberKeyPredicate = (key, value) => false;

//            new StreamsBuilder()
//                .Stream<int, string>("empty")
//                .filter(numberKeyPredicate)
//                .filterNot(numberKeyPredicate)
//                .to("nirvana");

//        }
//    }
//}
