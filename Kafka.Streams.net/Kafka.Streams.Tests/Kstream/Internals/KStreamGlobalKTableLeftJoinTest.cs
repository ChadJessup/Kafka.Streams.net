//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Tests.Helpers;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamGlobalKTableLeftJoinTest
//    {
//        private static KeyValueTimestamp[] EMPTY = new KeyValueTimestamp[0];

//        private string streamTopic = "streamTopic";
//        private string globalTableTopic = "globalTableTopic";
//        private int[] expectedKeys = { 0, 1, 2, 3 };

//        private MockProcessor<int, string> processor;
//        private TopologyTestDriver driver;
//        private StreamsBuilder builder;


//        public void setUp()
//        {

//            builder = new StreamsBuilder();
//            IKStream<int, string> stream;
//            IGlobalKTable<string, string> table; // value of stream optionally.Contains key of table
//            IKeyValueMapper<int, string, string> keyMapper;

//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
//            Consumed<int, string> streamConsumed = Consumed.with(Serdes.Int(), Serdes.String());
//            Consumed<string, string> tableConsumed = Consumed.with(Serdes.String(), Serdes.String());
//            stream = builder.Stream(streamTopic, streamConsumed);
//            table = builder.globalTable(globalTableTopic, tableConsumed);
//            keyMapper = (key, value) =>
//            {
//                string[] tokens = value.split(",");
//                // Value is comma delimited. If second token is present, it's the key to the global ktable.
//                // If not present, use null to indicate no match
//                return tokens.Length > 1 ? tokens[1] : null;
//            }

//            stream.leftJoin(table, keyMapper, MockValueJoiner.TOSTRING_JOINER).process(supplier);

//            StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());
//            driver = new TopologyTestDriver(builder.Build(), props);

//            processor = supplier.theCapturedProcessor();
//        }


//        public void cleanup()
//        {
//            driver.close();
//        }

//        private void pushToStream(int messageCount, string valuePrefix, bool includeForeignKey)
//        {
//            ConsumerRecordFactory<int, string> recordFactory =
//                new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String(), 0L, 1L);
//            for (var i = 0; i < messageCount; i++)
//            {
//                var value = valuePrefix + expectedKeys[i];
//                if (includeForeignKey)
//                {
//                    value = value + ",FKey" + expectedKeys[i];
//                }
//                driver.pipeInput(recordFactory.create(streamTopic, expectedKeys[i], value));
//            }
//        }

//        private void pushToGlobalTable(int messageCount, string valuePrefix)
//        {
//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 1L);
//            for (var i = 0; i < messageCount; i++)
//            {
//                driver.pipeInput(recordFactory.create(globalTableTopic, "FKey" + expectedKeys[i], valuePrefix + expectedKeys[i]));
//            }
//        }

//        private void pushNullValueToGlobalTable(int messageCount)
//        {
//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 1L);
//            for (var i = 0; i < messageCount; i++)
//            {
//                driver.pipeInput(recordFactory.create(globalTableTopic, "FKey" + expectedKeys[i], (string)null));
//            }
//        }

//        [Fact]
//        public void shouldNotRequireCopartitioning()
//        {
//            Collection<HashSet<string>> copartitionGroups =
//                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

//            Assert.Equal("KStream-GlobalKTable joins do not need to be co-partitioned", 0, copartitionGroups.Count);
//        }

//        [Fact]
//        public void shouldNotJoinWithEmptyGlobalTableOnStreamUpdates()
//        {

//            // push two items to the primary stream. the globalTable is empty

//            pushToStream(2, "X", true);
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0,FKey0+null", 0),
//                    new KeyValueTimestamp<>(1, "X1,FKey1+null", 1));
//        }

//        [Fact]
//        public void shouldNotJoinOnGlobalTableUpdates()
//        {

//            // push two items to the primary stream. the globalTable is empty

//            pushToStream(2, "X", true);
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0,FKey0+null", 0),
//                    new KeyValueTimestamp<>(1, "X1,FKey1+null", 1));

//            // push two items to the globalTable. this should not produce any item.

//            pushToGlobalTable(2, "Y");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.

//            pushToStream(4, "X", true);
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0,FKey0+Y0", 0),
//                    new KeyValueTimestamp<>(1, "X1,FKey1+Y1", 1),
//                    new KeyValueTimestamp<>(2, "X2,FKey2+null", 2),
//                    new KeyValueTimestamp<>(3, "X3,FKey3+null", 3));

//            // push all items to the globalTable. this should not produce any item

//            pushToGlobalTable(4, "YY");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.

//            pushToStream(4, "X", true);
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0,FKey0+YY0", 0),
//                    new KeyValueTimestamp<>(1, "X1,FKey1+YY1", 1),
//                    new KeyValueTimestamp<>(2, "X2,FKey2+YY2", 2),
//                    new KeyValueTimestamp<>(3, "X3,FKey3+YY3", 3));

//            // push all items to the globalTable. this should not produce any item

//            pushToGlobalTable(4, "YYY");
//            processor.checkAndClearProcessResult(EMPTY);
//        }

//        [Fact]
//        public void shouldJoinRegardlessIfMatchFoundOnStreamUpdates()
//        {

//            // push two items to the globalTable. this should not produce any item.

//            pushToGlobalTable(2, "Y");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.

//            pushToStream(4, "X", true);
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0,FKey0+Y0", 0),
//                    new KeyValueTimestamp<>(1, "X1,FKey1+Y1", 1),
//                    new KeyValueTimestamp<>(2, "X2,FKey2+null", 2),
//                    new KeyValueTimestamp<>(3, "X3,FKey3+null", 3));

//        }

//        [Fact]
//        public void shouldClearGlobalTableEntryOnNullValueUpdates()
//        {

//            // push all four items to the globalTable. this should not produce any item.

//            pushToGlobalTable(4, "Y");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.

//            pushToStream(4, "X", true);
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0,FKey0+Y0", 0),
//                    new KeyValueTimestamp<>(1, "X1,FKey1+Y1", 1),
//                    new KeyValueTimestamp<>(2, "X2,FKey2+Y2", 2),
//                    new KeyValueTimestamp<>(3, "X3,FKey3+Y3", 3));

//            // push two items with null to the globalTable.As deletes. this should not produce any item.

//            pushNullValueToGlobalTable(2);
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.

//            pushToStream(4, "XX", true);
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "XX0,FKey0+null", 0),
//                    new KeyValueTimestamp<>(1, "XX1,FKey1+null", 1),
//                    new KeyValueTimestamp<>(2, "XX2,FKey2+Y2", 2),
//                    new KeyValueTimestamp<>(3, "XX3,FKey3+Y3", 3));
//        }

//        [Fact]
//        public void shouldJoinOnNullKeyMapperValues()
//        {

//            // push all items to the globalTable. this should not produce any item

//            pushToGlobalTable(4, "Y");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream with no foreign key, resulting in null keyMapper values.
//            // this should produce four items.

//            pushToStream(4, "XXX", false);
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "XXX0+null", 0),
//                    new KeyValueTimestamp<>(1, "XXX1+null", 1),
//                    new KeyValueTimestamp<>(2, "XXX2+null", 2),
//                    new KeyValueTimestamp<>(3, "XXX3+null", 3));
//        }

//    }
//}
