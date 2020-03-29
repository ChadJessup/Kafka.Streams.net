//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using System;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamKTableLeftJoinTest
//    {
//        private static KeyValueTimestamp[] EMPTY = new KeyValueTimestamp[0];

//        private string streamTopic = "streamTopic";
//        private string tableTopic = "tableTopic";
//        private ConsumerRecordFactory<int, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String(), 0L);
//        private int[] expectedKeys = { 0, 1, 2, 3 };

//        private var driver;
//        private MockProcessor<int, string> processor;
//        private StreamsBuilder builder;


//        public void setUp()
//        {
//            builder = new StreamsBuilder();

//            IKStream<int, string> stream;
//            IKTable<int, string> table;

//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
//            Consumed<int, string> consumed = Consumed.with(Serdes.Int(), Serdes.String());
//            stream = builder.Stream(streamTopic, consumed);
//            table = builder.Table(tableTopic, consumed);
//            stream.leftJoin(table, MockValueJoiner.TOSTRING_JOINER).process(supplier);

//            StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());
//            driver = new TopologyTestDriver(builder.Build(), props);

//            processor = supplier.theCapturedProcessor();
//        }


//        public void cleanup()
//        {
//            driver.close();
//        }

//        private void pushToStream(int messageCount, string valuePrefix)
//        {
//            for (var i = 0; i < messageCount; i++)
//            {
//                driver.pipeInput(recordFactory.create(streamTopic, expectedKeys[i], valuePrefix + expectedKeys[i], i));
//            }
//        }

//        private void pushToTable(int messageCount, string valuePrefix)
//        {
//            var r = new Random(System.currentTimeMillis());
//            for (var i = 0; i < messageCount; i++)
//            {
//                driver.pipeInput(recordFactory.create(
//                    tableTopic,
//                    expectedKeys[i],
//                    valuePrefix + expectedKeys[i],
//                    r.nextInt(int.MaxValue)));
//            }
//        }

//        private void pushNullValueToTable(int messageCount)
//        {
//            for (var i = 0; i < messageCount; i++)
//            {
//                driver.pipeInput(recordFactory.create(tableTopic, expectedKeys[i], null));
//            }
//        }

//        [Fact]
//        public void shouldRequireCopartitionedStreams()
//        {
//            Collection<HashSet<string>> copartitionGroups =
//                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

//            Assert.Equal(1, copartitionGroups.Count);
//            Assert.Equal(new HashSet<>(new List<string> { streamTopic, tableTopic }), copartitionGroups.iterator().next());
//        }

//        [Fact]
//        public void shouldJoinWithEmptyTableOnStreamUpdates()
//        {
//            // push two items to the primary stream. the table is empty
//            pushToStream(2, "X");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+null", 0),
//                    new KeyValueTimestamp<>(1, "X1+null", 1));
//        }

//        [Fact]
//        public void shouldNotJoinOnTableUpdates()
//        {
//            // push two items to the primary stream. the table is empty
//            pushToStream(2, "X");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+null", 0),
//                    new KeyValueTimestamp<>(1, "X1+null", 1));

//            // push two items to the table. this should not produce any item.
//            pushToTable(2, "Y");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.
//            pushToStream(4, "X");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
//                    new KeyValueTimestamp<>(1, "X1+Y1", 1),
//                    new KeyValueTimestamp<>(2, "X2+null", 2),
//                    new KeyValueTimestamp<>(3, "X3+null", 3));

//            // push all items to the table. this should not produce any item
//            pushToTable(4, "YY");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.
//            pushToStream(4, "X");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+YY0", 0),
//                    new KeyValueTimestamp<>(1, "X1+YY1", 1),
//                    new KeyValueTimestamp<>(2, "X2+YY2", 2),
//                    new KeyValueTimestamp<>(3, "X3+YY3", 3));

//            // push all items to the table. this should not produce any item
//            pushToTable(4, "YYY");
//            processor.checkAndClearProcessResult(EMPTY);
//        }

//        [Fact]
//        public void shouldJoinRegardlessIfMatchFoundOnStreamUpdates()
//        {
//            // push two items to the table. this should not produce any item.
//            pushToTable(2, "Y");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.
//            pushToStream(4, "X");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
//                    new KeyValueTimestamp<>(1, "X1+Y1", 1),
//                    new KeyValueTimestamp<>(2, "X2+null", 2),
//                    new KeyValueTimestamp<>(3, "X3+null", 3));

//        }

//        [Fact]
//        public void shouldClearTableEntryOnNullValueUpdates()
//        {
//            // push all four items to the table. this should not produce any item.
//            pushToTable(4, "Y");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.
//            pushToStream(4, "X");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
//                    new KeyValueTimestamp<>(1, "X1+Y1", 1),
//                    new KeyValueTimestamp<>(2, "X2+Y2", 2),
//                    new KeyValueTimestamp<>(3, "X3+Y3", 3));

//            // push two items with null to the table.As deletes. this should not produce any item.
//            pushNullValueToTable(2);
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce four items.
//            pushToStream(4, "XX");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "XX0+null", 0),
//                    new KeyValueTimestamp<>(1, "XX1+null", 1),
//                    new KeyValueTimestamp<>(2, "XX2+Y2", 2),
//                    new KeyValueTimestamp<>(3, "XX3+Y3", 3));
//        }

//    }
