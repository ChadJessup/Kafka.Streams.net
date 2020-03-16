//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Topologies;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class KTableSourceTest
//    {
//        private Consumed<string, string> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
//        private ConsumerRecordFactory<string, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

//        [Fact]
//        public void testKTable()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            IKTable<string, int> table1 = builder.Table(topic1, Consumed.with(Serdes.String(), Serdes.Int()));

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            table1.toStream().process(supplier);

//            ConsumerRecordFactory<string, int> integerFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), Serdes.Int(), 0L);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            driver.pipeInput(integerFactory.create(topic1, "A", 1, 10L));
//            driver.pipeInput(integerFactory.create(topic1, "B", 2, 11L));
//            driver.pipeInput(integerFactory.create(topic1, "C", 3, 12L));
//            driver.pipeInput(integerFactory.create(topic1, "D", 4, 13L));
//            driver.pipeInput(integerFactory.create(topic1, "A", null, 14L));
//            driver.pipeInput(integerFactory.create(topic1, "B", null, 15L));

//            Assert.Equal(
//                asList(new KeyValueTimestamp<>("A", 1, 10L),
//                     new KeyValueTimestamp<>("B", 2, 11L),
//                     new KeyValueTimestamp<>("C", 3, 12L),
//                     new KeyValueTimestamp<>("D", 4, 13L),
//                     new KeyValueTimestamp<>("A", null, 14L),
//                     new KeyValueTimestamp<>("B", null, 15L)),
//                 supplier.theCapturedProcessor().processed);
//        }

//        [Fact]
//        public void kTableShouldLogAndMeterOnSkippedRecords()
//        {
//            var builder = new StreamsBuilder();
//            var topic = "topic";
//            builder.Table(topic, stringConsumed);

//            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                driver.pipeInput(recordFactory.create(topic, null, "value"));
//                LogCaptureAppender.unregister(appender);

//                Assert.Equal(1.0, getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue());
//                Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key. topic=[topic] partition=[0] offset=[0]"));
//            }
//    }

//        [Fact]
//        public void testValueGetter()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";


//            var table1 = (IKTable<string, string>)builder.Table(topic1, stringConsumed, Materialized.As("store"));

//            Topology topology = builder.Build();
//            KTableValueGetterSupplier<string, string> getterSupplier1 = table1.valueGetterSupplier();

//            InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
//            topologyBuilder.connectProcessorAndStateStores(table1.name, getterSupplier1.storeNames());

//            var driver = new TopologyTestDriverWrapper(builder.Build(), props){
//            IKTableValueGetter<string, string> getter1 = getterSupplier1.get();
//            getter1.init(driver.setCurrentNodeForProcessorContext(table1.name));

//            driver.pipeInput(recordFactory.create(topic1, "A", "01", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "01", 20L));
//            driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));

//            Assert.Equal(ValueAndTimestamp.make("01", 10L), getter1.get("A"));
//            Assert.Equal(ValueAndTimestamp.make("01", 20L), getter1.get("B"));
//            Assert.Equal(ValueAndTimestamp.make("01", 15L), getter1.get("C"));

//            driver.pipeInput(recordFactory.create(topic1, "A", "02", 30L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "02", 5L));

//            Assert.Equal(ValueAndTimestamp.make("02", 30L), getter1.get("A"));
//            Assert.Equal(ValueAndTimestamp.make("02", 5L), getter1.get("B"));
//            Assert.Equal(ValueAndTimestamp.make("01", 15L), getter1.get("C"));

//            driver.pipeInput(recordFactory.create(topic1, "A", "03", 29L));

//            Assert.Equal(ValueAndTimestamp.make("03", 29L), getter1.get("A"));
//            Assert.Equal(ValueAndTimestamp.make("02", 5L), getter1.get("B"));
//            Assert.Equal(ValueAndTimestamp.make("01", 15L), getter1.get("C"));

//            driver.pipeInput(recordFactory.create(topic1, "A", (string)null, 50L));
//            driver.pipeInput(recordFactory.create(topic1, "B", (string)null, 3L));

//            Assert.Null(getter1.get("A"));
//            Assert.Null(getter1.get("B"));
//            Assert.Equal(ValueAndTimestamp.make("01", 15L), getter1.get("C"));
//        }

//        [Fact]
//        public void testNotSendingOldValue()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";


//            var table1 = (IKTable<string, string>)builder.Table(topic1, stringConsumed);

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            Topology topology = builder.Build().addProcessor("proc1", supplier, table1.name);

//            var driver = new TopologyTestDriver(topology, props);
//            MockProcessor<string, int> proc1 = supplier.theCapturedProcessor();

//            driver.pipeInput(recordFactory.create(topic1, "A", "01", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "01", 20L));
//            driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));
//            proc1.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>("01", null), 10),
//                new KeyValueTimestamp<>("B", new Change<>("01", null), 20),
//                new KeyValueTimestamp<>("C", new Change<>("01", null), 15));

//            driver.pipeInput(recordFactory.create(topic1, "A", "02", 8L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "02", 22L));
//            proc1.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>("02", null), 8),
//                new KeyValueTimestamp<>("B", new Change<>("02", null), 22));

//            driver.pipeInput(recordFactory.create(topic1, "A", "03", 12L));
//            proc1.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>("03", null), 12));

//            driver.pipeInput(recordFactory.create(topic1, "A", (string)null, 15L));
//            driver.pipeInput(recordFactory.create(topic1, "B", (string)null, 20L));
//            proc1.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 15),
//                new KeyValueTimestamp<>("B", new Change<>(null, null), 20));
//        }

//        [Fact]
//        public void testSendingOldValue()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";


//            var table1 = (IKTable<string, string, string>)builder.Table(topic1, stringConsumed);
//            table1.enableSendingOldValues();
//            Assert.True(table1.sendingOldValueEnabled());

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            Topology topology = builder.Build().addProcessor("proc1", supplier, table1.name);

//            try
//            {
//                var driver = new TopologyTestDriver(topology, props);
//                MockProcessor<string, int> proc1 = supplier.theCapturedProcessor();

//                driver.pipeInput(recordFactory.create(topic1, "A", "01", 10L));
//                driver.pipeInput(recordFactory.create(topic1, "B", "01", 20L));
//                driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));
//                proc1.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>("01", null), 10),
//                    new KeyValueTimestamp<>("B", new Change<>("01", null), 20),
//                    new KeyValueTimestamp<>("C", new Change<>("01", null), 15));

//                driver.pipeInput(recordFactory.create(topic1, "A", "02", 8L));
//                driver.pipeInput(recordFactory.create(topic1, "B", "02", 22L));
//                proc1.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>("02", "01"), 8),
//                    new KeyValueTimestamp<>("B", new Change<>("02", "01"), 22));

//                driver.pipeInput(recordFactory.create(topic1, "A", "03", 12L));
//                proc1.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>("03", "02"), 12));

//                driver.pipeInput(recordFactory.create(topic1, "A", (string)null, 15L));
//                driver.pipeInput(recordFactory.create(topic1, "B", (string)null, 20L));
//                proc1.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, "03"), 15),
//                    new KeyValueTimestamp<>("B", new Change<>(null, "02"), 20));
//            }
//    }
//    }
