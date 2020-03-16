//using Kafka.Streams;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.Topologies;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableMapValuesTest
//    {
//        private Consumed<string, string> consumed = Consumed.with(Serdes.String(), Serdes.String());
//        private ConsumerRecordFactory<string, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

//        private void doTestKTable(StreamsBuilder builder,
//                                  string topic1,
//                                  MockProcessorSupplier<string, int> supplier)
//        {

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            driver.pipeInput(recordFactory.create(topic1, "A", "1", 5L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "2", 25L));
//            driver.pipeInput(recordFactory.create(topic1, "C", "3", 20L));
//            driver.pipeInput(recordFactory.create(topic1, "D", "4", 10L));
//            Assert.Equal(new KeyValueTimestamp<>("A", 1, 5),
//                     new KeyValueTimestamp<>("B", 2, 25),
//                     new KeyValueTimestamp<>("C", 3, 20),
//                     new KeyValueTimestamp<>("D", 4, 10)), supplier.theCapturedProcessor().processed);
//        }

//        [Fact]
//        public void testKTable()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            IKTable<string, string> table1 = builder.Table(topic1, consumed);
//            IKTable<string, int> table2 = table1.mapValues(value => value.charAt(0) - 48);

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            table2.toStream().process(supplier);

//            doTestKTable(builder, topic1, supplier);
//        }

//        [Fact]
//        public void testQueryableKTable()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            IKTable<string, string> table1 = builder.Table(topic1, consumed);
//            IKTable<string, int> table2 = table1
//                .mapValues(
//                    value => value.charAt(0) - 48,
//                    Materialize.As < string, int, IKeyValueStore<Bytes, byte[]>("anyName")
//                        .withValueSerde(Serdes.Int()));

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            table2.toStream().process(supplier);

//            doTestKTable(builder, topic1, supplier);
//        }

//        private void doTestValueGetter(StreamsBuilder builder,
//                                       string topic1,
//                                       IKTable<string, int> table2,
//                                       IKTable<string, int> table3)
//        {

//            Topology topology = builder.Build();

//            KTableValueGetterSupplier<string, int> getterSupplier2 = table2.valueGetterSupplier();
//            KTableValueGetterSupplier<string, int> getterSupplier3 = table3.valueGetterSupplier();

//            InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
//            topologyBuilder.ConnectProcessorAndStateStores(table2.name, getterSupplier2.storeNames());
//            topologyBuilder.ConnectProcessorAndStateStores(table3.name, getterSupplier3.storeNames());

//            var driver = new TopologyTestDriverWrapper(builder.Build(), props);
//            IKTableValueGetter<string, int> getter2 = getterSupplier2.get();
//            IKTableValueGetter<string, int> getter3 = getterSupplier3.get();

//            getter2.init(driver.setCurrentNodeForProcessorContext(table2.name));
//            getter3.init(driver.setCurrentNodeForProcessorContext(table3.name));

//            driver.pipeInput(recordFactory.create(topic1, "A", "01", 50L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "01", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "C", "01", 30L));

//            Assert.Equal(ValueAndTimestamp.make(1, 50L), getter2.get("A"));
//            Assert.Equal(ValueAndTimestamp.make(1, 10L), getter2.get("B"));
//            Assert.Equal(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

//            Assert.Equal(ValueAndTimestamp.make(-1, 50L), getter3.get("A"));
//            Assert.Equal(ValueAndTimestamp.make(-1, 10L), getter3.get("B"));
//            Assert.Equal(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));

//            driver.pipeInput(recordFactory.create(topic1, "A", "02", 25L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "02", 20L));

//            Assert.Equal(ValueAndTimestamp.make(2, 25L), getter2.get("A"));
//            Assert.Equal(ValueAndTimestamp.make(2, 20L), getter2.get("B"));
//            Assert.Equal(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

//            Assert.Equal(ValueAndTimestamp.make(-2, 25L), getter3.get("A"));
//            Assert.Equal(ValueAndTimestamp.make(-2, 20L), getter3.get("B"));
//            Assert.Equal(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));

//            driver.pipeInput(recordFactory.create(topic1, "A", "03", 35L));

//            Assert.Equal(ValueAndTimestamp.make(3, 35L), getter2.get("A"));
//            Assert.Equal(ValueAndTimestamp.make(2, 20L), getter2.get("B"));
//            Assert.Equal(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

//            Assert.Equal(ValueAndTimestamp.make(-3, 35L), getter3.get("A"));
//            Assert.Equal(ValueAndTimestamp.make(-2, 20L), getter3.get("B"));
//            Assert.Equal(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));

//            driver.pipeInput(recordFactory.create(topic1, "A", (string)null, 1L));

//            Assert.Null(getter2.get("A"));
//            Assert.Equal(ValueAndTimestamp.make(2, 20L), getter2.get("B"));
//            Assert.Equal(ValueAndTimestamp.make(1, 30L), getter2.get("C"));

//            Assert.Null(getter3.get("A"));
//            Assert.Equal(ValueAndTimestamp.make(-2, 20L), getter3.get("B"));
//            Assert.Equal(ValueAndTimestamp.make(-1, 30L), getter3.get("C"));
//        }

//        [Fact]
//        public void testQueryableValueGetter()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";
//            var storeName2 = "store2";
//            var storeName3 = "store3";

//            var table1 = (IKTable<string, string>)builder.Table(topic1, consumed);
//            var table2 = (IKTable<string, int>)table1.mapValues(
//                null,
//                    Materialize.As < string, int, IKeyValueStore<Bytes, byte[]>(storeName2)
//                        .withValueSerde(Serdes.Int()));
//            var table3 = (IKTable<string, int>)table1.mapValues(
//                    value => value * (-1),
//                    Materialize.As < string, int, IKeyValueStore<Bytes, byte[]>(storeName3)
//                        .withValueSerde(Serdes.Int()));
//            var table4 = (IKTable<string, int>)table1.mapValues();

//            Assert.Equal(storeName2, table2.queryableStoreName);
//            Assert.Equal(storeName3, table3.queryableStoreName);
//            Assert.Null(table4.queryableStoreName);

//            doTestValueGetter(builder, topic1, table2, table3);
//        }

//        [Fact]
//        public void testNotSendingOldValue()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 = (IKTable<string, string>)builder.Table(topic1, consumed);
//            var table2 = (IKTable<string, string>)table1.mapValues();

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            Topology topology = builder.Build().addProcessor("proc", supplier, table2.name);

//            var driver = new TopologyTestDriver(topology, props);
//            MockProcessor<string, int> proc = supplier.theCapturedProcessor();

//            Assert.False(table1.sendingOldValueEnabled());
//            Assert.False(table2.sendingOldValueEnabled());

//            driver.pipeInput(recordFactory.create(topic1, "A", "01", 5L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "01", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));
//            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
//                    new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
//                    new KeyValueTimestamp<>("C", new Change<>(1, null), 15));

//            driver.pipeInput(recordFactory.create(topic1, "A", "02", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "02", 8L));
//            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 10),
//                    new KeyValueTimestamp<>("B", new Change<>(2, null), 8));

//            driver.pipeInput(recordFactory.create(topic1, "A", "03", 20L));
//            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, null), 20));

//            driver.pipeInput(recordFactory.create(topic1, "A", (string)null, 30L));
//            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 30));
//        }

//        [Fact]
//        public void testSendingOldValue()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 =
//                (IKTable<string, string>)builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, string>)table1.mapValues();
//            table2.enableSendingOldValues();

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            builder.Build().AddProcessor("proc", supplier, table2.name);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            MockProcessor<string, int> proc = supplier.theCapturedProcessor();

//            Assert.True(table1.sendingOldValueEnabled());
//            Assert.True(table2.sendingOldValueEnabled());

//            driver.pipeInput(recordFactory.create(topic1, "A", "01", 5L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "01", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "C", "01", 15L));
//            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
//                    new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
//                    new KeyValueTimestamp<>("C", new Change<>(1, null), 15));

//            driver.pipeInput(recordFactory.create(topic1, "A", "02", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "02", 8L));
//            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, 1), 10),
//                    new KeyValueTimestamp<>("B", new Change<>(2, 1), 8));

//            driver.pipeInput(recordFactory.create(topic1, "A", "03", 20L));
//            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, 2), 20));

//            driver.pipeInput(recordFactory.create(topic1, "A", (string)null, 30L));
//            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, 3), 30));
//        }
//    }
