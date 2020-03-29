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
//using System;

//namespace Kafka.Streams.KStream.Internals
//{

































//    public class KStreamKTableJoinTest
//    {
//        private static KeyValueTimestamp[] EMPTY = new KeyValueTimestamp[0];

//        private string streamTopic = "streamTopic";
//        private string tableTopic = "tableTopic";
//        private ConsumerRecordFactory<int, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String(), 0L);
//        private int[] expectedKeys = { 0, 1, 2, 3 };

//        private MockProcessor<int, string> processor;
//        private var driver;
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
//            stream.join(table, MockValueJoiner.TOSTRING_JOINER).process(supplier);

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

//        private void pushNullValueToTable()
//        {
//            for (var i = 0; i < 2; i++)
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
//        public void shouldNotJoinWithEmptyTableOnStreamUpdates()
//        {
//            // push two items to the primary stream. the table is empty
//            pushToStream(2, "X");
//            processor.checkAndClearProcessResult(EMPTY);
//        }

//        [Fact]
//        public void shouldNotJoinOnTableUpdates()
//        {
//            // push two items to the primary stream. the table is empty
//            pushToStream(2, "X");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push two items to the table. this should not produce any item.
//            pushToTable(2, "Y");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce two items.
//            pushToStream(4, "X");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
//                    new KeyValueTimestamp<>(1, "X1+Y1", 1));

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
//        public void shouldJoinOnlyIfMatchFoundOnStreamUpdates()
//        {
//            // push two items to the table. this should not produce any item.
//            pushToTable(2, "Y");
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce two items.
//            pushToStream(4, "X");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
//                    new KeyValueTimestamp<>(1, "X1+Y1", 1));

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
//            pushNullValueToTable();
//            processor.checkAndClearProcessResult(EMPTY);

//            // push all four items to the primary stream. this should produce two items.
//            pushToStream(4, "XX");
//            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "XX2+Y2", 2),
//                    new KeyValueTimestamp<>(3, "XX3+Y3", 3));
//        }

//        [Fact]
//        public void shouldLogAndMeterWhenSkippingNullLeftKey()
//        {
//            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//            driver.pipeInput(recordFactory.create(streamTopic, null, "A"));
//            LogCaptureAppender.unregister(appender);

//            Assert.Equal(1.0, getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue());
//            Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key or value. key=[null] value=[A] topic=[streamTopic] partition=[0] offset=[0]"));
//        }

//        [Fact]
//        public void shouldLogAndMeterWhenSkippingNullLeftValue()
//        {
//            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//            driver.pipeInput(recordFactory.create(streamTopic, 1, null));
//            LogCaptureAppender.unregister(appender);

//            Assert.Equal(1.0, getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue());
//            Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key or value. key=[1] value=[null] topic=[streamTopic] partition=[0] offset=[0]"));
//        }
//    }
//}
