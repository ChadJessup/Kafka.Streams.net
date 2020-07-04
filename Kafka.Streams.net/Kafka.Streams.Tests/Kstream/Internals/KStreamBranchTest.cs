using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{

    public class KStreamBranchTest
    {

        private readonly string topicName = "topic";
        private readonly ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<int, string>(
            Serdes.Int(),
            Serdes.String());

        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());


        [Fact]
        public void testKStreamBranch()
        {
            var builder = new StreamsBuilder();

            Func<int, string, bool> isEven = (key, value) => (key % 2) == 0;
            Func<int, string, bool> isMultipleOfThree = (key, value) => (key % 3) == 0;
            Func<int, string, bool> isOdd = (key, value) => (key % 2) != 0;

            var expectedKeys = new int[] { 1, 2, 3, 4, 5, 6 };

            IKStream<int, string> stream;
            IKStream<int, string>[] branches;

            stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            branches = stream.Branch(isEven, isMultipleOfThree, isOdd);

            Assert.Equal(3, branches.Length);

            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<int, string>();
            for (var i = 0; i < branches.Length; i++)
            {
                branches[i].Process(supplier);
            }

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, "V" + expectedKey));
            }

            List<MockProcessor<int, string>> processors = supplier.CapturedProcessors(3);
            Assert.Equal(3, processors[0].processed.Count);
            Assert.Single(processors[1].processed);
            Assert.Equal(2, processors[2].processed.Count);
        }


        [Fact]
        public void testTypeVariance()
        {
            // Func<int, object> positive = (key, value) => key.doubleValue() > 0;
            // Func<int, object> negative = (key, value) => key.doubleValue() < 0;
            // 
            // new StreamsBuilder()
            //     .Stream<int, string>("empty")
            //     .Branch(positive, negative);
        }
    }
}
