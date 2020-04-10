namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class GlobalKTableJoinsTest
//    {

//        private StreamsBuilder builder = new StreamsBuilder();
//        private string streamTopic = "stream";
//        private string globalTopic = "global";
//        private IGlobalKTable<string, string> global;
//        private IKStream<string, string> stream;
//        private IKeyValueMapper<string, string, string> keyValueMapper;


//        public void setUp()
//        {
//            Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
//            global = builder.globalTable(globalTopic, consumed);
//            stream = builder.Stream(streamTopic, consumed);
//            keyValueMapper = (key, value) => value;
//        }

//        [Fact]
//        public void shouldLeftJoinWithStream()
//        {
//            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<>();
//            stream
//                .leftJoin(global, keyValueMapper, MockValueJoiner.TOSTRING_JOINER)
//                .process(supplier);

//            Dictionary<string, ValueAndTimestamp<string>> expected = new HashMap<>();
//            expected.Put("1", ValueAndTimestamp.Make("a+A", 2L));
//            expected.Put("2", ValueAndTimestamp.Make("b+B", 10L));
//            expected.Put("3", ValueAndTimestamp.Make("c+null", 3L));

//            verifyJoin(expected, supplier);
//        }

//        [Fact]
//        public void shouldInnerJoinWithStream()
//        {
//            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<>();
//            stream
//                .join(global, keyValueMapper, MockValueJoiner.TOSTRING_JOINER)
//                .process(supplier);

//            Dictionary<string, ValueAndTimestamp<string>> expected = new HashMap<>();
//            expected.Put("1", ValueAndTimestamp.Make("a+A", 2L));
//            expected.Put("2", ValueAndTimestamp.Make("b+B", 10L));

//            verifyJoin(expected, supplier);
//        }

//        private void verifyJoin(Dictionary<string, ValueAndTimestamp<string>> expected,
//                                MockProcessorSupplier<string, string> supplier)
//        {
//            ConsumerRecordFactory<string, string> recordFactory = new ConsumerRecordFactory<>(Serdes.String(), Serdes.String());
//            StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            // write some data to the global table
//            driver.PipeInput(recordFactory.Create(globalTopic, "a", "A", 1L));
//            driver.PipeInput(recordFactory.Create(globalTopic, "b", "B", 5L));
//            //write some data to the stream
//            driver.PipeInput(recordFactory.Create(streamTopic, "1", "a", 2L));
//            driver.PipeInput(recordFactory.Create(streamTopic, "2", "b", 10L));
//            driver.PipeInput(recordFactory.Create(streamTopic, "3", "c", 3L));

//            Assert.Equal(expected, supplier.theCapturedProcessor().lastValueAndTimestampPerKey);
//        }
//    }
//}
