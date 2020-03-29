//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Tests.Helpers;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class KStreamBranchTest
//    {

//        private string topicName = "topic";
//        private ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String());
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());


//        [Fact]
//        public void testKStreamBranch()
//        {
//            var builder = new StreamsBuilder();

//            IPredicate<int, string> isEven = (key, value) => (key % 2) == 0;
//            IPredicate<int, string> isMultipleOfThree = (key, value) => (key % 3) == 0;
//            IPredicate<int, string> isOdd = (key, value) => (key % 2) != 0;

//            var expectedKeys = new int[] { 1, 2, 3, 4, 5, 6 };

//            IKStream<int, string> stream;
//            IKStream<int, string>[] branches;

//            stream = builder.Stream(topicName, Consumed.with(Serdes.Int(), Serdes.String()));
//            branches = stream.branch(isEven, isMultipleOfThree, isOdd);

//            Assert.Equal(3, branches.Length);

//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
//            for (var i = 0; i < branches.Length; i++)
//            {
//                branches[i].process(supplier);
//            }

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topicName, expectedKey, "V" + expectedKey));
//            }

//            List<MockProcessor<int, string>> processors = supplier.capturedProcessors(3);
//            Assert.Equal(3, processors[0].processed.Count);
//            Assert.Equal(1, processors[1].processed.Count);
//            Assert.Equal(2, processors[2].processed.Count);
//        }


//        [Fact]
//        public void testTypeVariance()
//        {
//            IPredicate<int, object> positive = (key, value) => key.doubleValue() > 0;
//            IPredicate<int, object> negative = (key, value) => key.doubleValue() < 0;

//            new StreamsBuilder()
//                .Stream<int, string>("empty")
//                .branch(positive, negative);
//        }
//    }
//}
