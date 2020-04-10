namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.Topologies;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableFilterTest
//    {
//        private Consumed<string, int> consumed = Consumed.With(Serdes.String(), Serdes.Int());
//        private ConsumerRecordFactory<string, int> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.String(), Serdes.Int(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.Int());


//        public void setUp()
//        {
//            // disable caching at the config level
//            props.Set(StreamsConfigPropertyNames.CacheMaxBytesBuffering, "0");
//        }

//        private IPredicate<string, int> predicate = (key, value) => (value % 2) == 0;

//        private void doTestKTable(StreamsBuilder builder,
//                                  IKTable<string, int> table2,
//                                  IKTable<string, int> table3,
//                                  string topic)
//        {
//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            table2.toStream().process(supplier);
//            table3.toStream().process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            driver.PipeInput(recordFactory.Create(topic, "A", 1, 10L));
//            driver.PipeInput(recordFactory.Create(topic, "B", 2, 5L));
//            driver.PipeInput(recordFactory.Create(topic, "C", 3, 8L));
//            driver.PipeInput(recordFactory.Create(topic, "D", 4, 14L));
//            driver.PipeInput(recordFactory.Create(topic, "A", null, 18L));
//            driver.PipeInput(recordFactory.Create(topic, "B", null, 15L));

//            List<MockProcessor<string, int>> processors = supplier.capturedProcessors(2);

//            processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", null, 10),
//                new KeyValueTimestamp<>("B", 2, 5),
//                new KeyValueTimestamp<>("C", null, 8),
//                new KeyValueTimestamp<>("D", 4, 14),
//                new KeyValueTimestamp<>("A", null, 18),
//                new KeyValueTimestamp<>("B", null, 15));
//            processors.Get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", 1, 10),
//                new KeyValueTimestamp<>("B", null, 5),
//                new KeyValueTimestamp<>("C", 3, 8),
//                new KeyValueTimestamp<>("D", null, 14),
//                new KeyValueTimestamp<>("A", null, 18),
//                new KeyValueTimestamp<>("B", null, 15));
//        }

//        [Fact]
//        public void shouldPassThroughWithoutMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            IKTable<string, int> table1 = builder.Table(topic1, consumed);
//            IKTable<string, int> table2 = table1.filter(predicate);
//            IKTable<string, int> table3 = table1.filterNot(predicate);

//            Assert.Null(table1.queryableStoreName);
//            Assert.Null(table2.queryableStoreName);
//            Assert.Null(table3.queryableStoreName);

//            doTestKTable(builder, table2, table3, topic1);
//        }

//        [Fact]
//        public void shouldPassThroughOnMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            IKTable<string, int> table1 = builder.Table(topic1, consumed);
//            IKTable<string, int> table2 = table1.filter(predicate, Materialized.As("store2"));
//            IKTable<string, int> table3 = table1.filterNot(predicate);

//            Assert.Null(table1.queryableStoreName);
//            Assert.Equal("store2", table2.queryableStoreName);
//            Assert.Null(table3.queryableStoreName);

//            doTestKTable(builder, table2, table3, topic1);
//        }

//        private void doTestValueGetter(StreamsBuilder builder,
//                                       IKTable<string, int> table2,
//                                       IKTable<string, int> table3,
//                                       string topic1)
//        {

//            Topology topology = builder.Build();

//            KTableValueGetterSupplier<string, int> getterSupplier2 = table2.valueGetterSupplier();
//            KTableValueGetterSupplier<string, int> getterSupplier3 = table3.valueGetterSupplier();

//            InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
//            topologyBuilder.connectProcessorAndStateStores(table2.Name, getterSupplier2.storeNames());
//            topologyBuilder.connectProcessorAndStateStores(table3.Name, getterSupplier3.storeNames());

//            var driver = new TopologyTestDriverWrapper(topology, props);
//            IKTableValueGetter<string, int> getter2 = getterSupplier2.Get();
//            IKTableValueGetter<string, int> getter3 = getterSupplier3.Get();

//            getter2.Init(driver.setCurrentNodeForProcessorContext(table2.Name));
//            getter3.Init(driver.setCurrentNodeForProcessorContext(table3.Name));

//            driver.PipeInput(recordFactory.Create(topic1, "A", 1, 5L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", 1, 10L));
//            driver.PipeInput(recordFactory.Create(topic1, "C", 1, 15L));

//            Assert.Null(getter2.Get("A"));
//            Assert.Null(getter2.Get("B"));
//            Assert.Null(getter2.Get("C"));

//            Assert.Equal(ValueAndTimestamp.Make(1, 5L), getter3.Get("A"));
//            Assert.Equal(ValueAndTimestamp.Make(1, 10L), getter3.Get("B"));
//            Assert.Equal(ValueAndTimestamp.Make(1, 15L), getter3.Get("C"));

//            driver.PipeInput(recordFactory.Create(topic1, "A", 2, 10L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", 2, 5L));

//            Assert.Equal(ValueAndTimestamp.Make(2, 10L), getter2.Get("A"));
//            Assert.Equal(ValueAndTimestamp.Make(2, 5L), getter2.Get("B"));
//            Assert.Null(getter2.Get("C"));

//            Assert.Null(getter3.Get("A"));
//            Assert.Null(getter3.Get("B"));
//            Assert.Equal(ValueAndTimestamp.Make(1, 15L), getter3.Get("C"));

//            driver.PipeInput(recordFactory.Create(topic1, "A", 3, 15L));

//            Assert.Null(getter2.Get("A"));
//            Assert.Equal(ValueAndTimestamp.Make(2, 5L), getter2.Get("B"));
//            Assert.Null(getter2.Get("C"));

//            Assert.Equal(ValueAndTimestamp.Make(3, 15L), getter3.Get("A"));
//            Assert.Null(getter3.Get("B"));
//            Assert.Equal(ValueAndTimestamp.Make(1, 15L), getter3.Get("C"));

//            driver.PipeInput(recordFactory.Create(topic1, "A", null, 10L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", null, 20L));

//            Assert.Null(getter2.Get("A"));
//            Assert.Null(getter2.Get("B"));
//            Assert.Null(getter2.Get("C"));

//            Assert.Null(getter3.Get("A"));
//            Assert.Null(getter3.Get("B"));
//            Assert.Equal(ValueAndTimestamp.Make(1, 15L), getter3.Get("C"));
//        }

//        [Fact]
//        public void shouldGetValuesOnMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 = (IKTable<string, int>)builder.Table(topic1, consumed);
//            var table2 = (IKTable<string, int>)table1.filter(predicate, Materialized.As("store2"));
//            var table3 = (IKTable<string, int>)table1.filterNot(predicate, Materialized.As("store3"));
//            var table4 = (IKTable<string, int>)table1.filterNot(predicate);

//            Assert.Null(table1.queryableStoreName);
//            Assert.Equal("store2", table2.queryableStoreName);
//            Assert.Equal("store3", table3.queryableStoreName);
//            Assert.Null(table4.queryableStoreName);

//            doTestValueGetter(builder, table2, table3, topic1);
//        }

//        private void doTestNotSendingOldValue(StreamsBuilder builder,
//                                              IKTable<string, int> table2,
//                                              IKTable<string, int> table1,
//                                              string topic1)
//        {
//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();

//            builder.Build().AddProcessor("proc1", supplier, table1.Name);
//            builder.Build().AddProcessor("proc2", supplier, table2.Name);

//            var driver = new TopologyTestDriver(builder.Build(), props);

//            driver.PipeInput(recordFactory.Create(topic1, "A", 1, 5L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", 1, 10L));
//            driver.PipeInput(recordFactory.Create(topic1, "C", 1, 15L));

//            List<MockProcessor<string, int>> processors = supplier.capturedProcessors(2);

//            processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
//                new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
//                new KeyValueTimestamp<>("C", new Change<>(1, null), 15));
//            processors.Get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 5),
//                new KeyValueTimestamp<>("B", new Change<>(null, null), 10),
//                new KeyValueTimestamp<>("C", new Change<>(null, null), 15));

//            driver.PipeInput(recordFactory.Create(topic1, "A", 2, 15L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", 2, 8L));

//            processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 15),
//                new KeyValueTimestamp<>("B", new Change<>(2, null), 8));
//            processors.Get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 15),
//                new KeyValueTimestamp<>("B", new Change<>(2, null), 8));

//            driver.PipeInput(recordFactory.Create(topic1, "A", 3, 20L));

//            processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, null), 20));
//            processors.Get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 20));
//            driver.PipeInput(recordFactory.Create(topic1, "A", null, 10L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", null, 20L));

//            processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 10),
//                new KeyValueTimestamp<>("B", new Change<>(null, null), 20));
//            processors.Get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 10),
//                new KeyValueTimestamp<>("B", new Change<>(null, null), 20));
//        }


//        [Fact]
//        public void shouldNotSendOldValuesWithoutMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 = (IKTable<string, int>)builder.Table(topic1, consumed);
//            var table2 = (IKTable<string, int>)table1.filter(predicate);

//            doTestNotSendingOldValue(builder, table1, table2, topic1);
//        }

//        [Fact]
//        public void shouldNotSendOldValuesOnMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 =
//                (IKTable<string, int>)builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, int>)table1.filter(predicate, Materialized.As("store2"));

//            doTestNotSendingOldValue(builder, table1, table2, topic1);
//        }

//        private void doTestSendingOldValue(StreamsBuilder builder,
//                                           IKTable<string, int> table1,
//                                           IKTable<string, int> table2,
//                                           string topic1)
//        {
//            table2.enableSendingOldValues();

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            Topology topology = builder.Build();

//            topology.AddProcessor("proc1", supplier, table1.Name);
//            topology.AddProcessor("proc2", supplier, table2.Name);

//            try
//            {
//                var driver = new TopologyTestDriver(topology, props);
//            driver.PipeInput(recordFactory.Create(topic1, "A", 1, 5L));
//                driver.PipeInput(recordFactory.Create(topic1, "B", 1, 10L));
//                driver.PipeInput(recordFactory.Create(topic1, "C", 1, 15L));

//                List<MockProcessor<string, int>> processors = supplier.capturedProcessors(2);

//                processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
//                    new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
//                    new KeyValueTimestamp<>("C", new Change<>(1, null), 15));
//                processors.Get(1).checkEmptyAndClearProcessResult();

//                driver.PipeInput(recordFactory.Create(topic1, "A", 2, 15L));
//                driver.PipeInput(recordFactory.Create(topic1, "B", 2, 8L));

//                processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, 1), 15),
//                    new KeyValueTimestamp<>("B", new Change<>(2, 1), 8));
//                processors.Get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 15),
//                    new KeyValueTimestamp<>("B", new Change<>(2, null), 8));

//                driver.PipeInput(recordFactory.Create(topic1, "A", 3, 20L));

//                processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, 2), 20));
//                processors.Get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, 2), 20));

//                driver.PipeInput(recordFactory.Create(topic1, "A", null, 10L));
//                driver.PipeInput(recordFactory.Create(topic1, "B", null, 20L));

//                processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, 3), 10),
//                    new KeyValueTimestamp<>("B", new Change<>(null, 2), 20));
//                processors.Get(1).checkAndClearProcessResult(new KeyValueTimestamp<>("B", new Change<>(null, 2), 20));
//            }
//    }

//        [Fact]
//        public void shouldSendOldValuesWhenEnabledWithoutMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 =
//                (IKTable<string, int, int>)builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, int, int>)table1.filter(predicate);

//            doTestSendingOldValue(builder, table1, table2, topic1);
//        }

//        [Fact]
//        public void shouldSendOldValuesWhenEnabledOnMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 =
//                (IKTable<string, int, int>)builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, int, int>)table1.filter(predicate, Materialized.As("store2"));

//            doTestSendingOldValue(builder, table1, table2, topic1);
//        }

//        private void doTestSkipNullOnMaterialization(StreamsBuilder builder,
//                                                     IKTable<string, string, string> table1,
//                                                     IKTable<string, string, string> table2,
//                                                     string topic1)
//        {
//            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<>();
//            Topology topology = builder.Build();

//            topology.AddProcessor("proc1", supplier, table1.Name);
//            topology.AddProcessor("proc2", supplier, table2.Name);

//            ConsumerRecordFactory<string, string> stringRecordFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);
//            try
//            {
//                var driver = new TopologyTestDriver(topology, props);

//            driver.PipeInput(stringRecordFactory.Create(topic1, "A", "reject", 5L));
//                driver.PipeInput(stringRecordFactory.Create(topic1, "B", "reject", 10L));
//                driver.PipeInput(stringRecordFactory.Create(topic1, "C", "reject", 20L));
//            }

//        List<MockProcessor<string, string>> processors = supplier.capturedProcessors(2);
//            processors.Get(0).checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>("reject", null), 5),
//                new KeyValueTimestamp<>("B", new Change<>("reject", null), 10),
//                new KeyValueTimestamp<>("C", new Change<>("reject", null), 20));
//            processors.Get(1).checkEmptyAndClearProcessResult();
//        }

//        [Fact]
//        public void shouldSkipNullToRepartitionWithoutMaterialization()
//        {
//            // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
//            var builder = new StreamsBuilder();

//            var topic1 = "topic1";

//            Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
//            var table1 =
//                (IKTable<string, string, string>)builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, string, string>)table1.filter((key, value) => value.equalsIgnorecase("accept"))
//                    .groupBy(MockMapper.noOpKeyValueMapper())
//                    .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER);

//            doTestSkipNullOnMaterialization(builder, table1, table2, topic1);
//        }

//        [Fact]
//        public void shouldSkipNullToRepartitionOnMaterialization()
//        {
//            // Do not explicitly set enableSendingOldValues. Let a further downstream stateful operator trigger it instead.
//            var builder = new StreamsBuilder();

//            var topic1 = "topic1";

//            Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
//            var table1 =
//                (IKTable<string, string, string>)builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, string, string>)table1.filter((key, value) => value.equalsIgnorecase("accept"), Materialized.As("store2"))
//                    .groupBy(MockMapper.noOpKeyValueMapper())
//                    .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, Materialized.As("mock-result"));

//            doTestSkipNullOnMaterialization(builder, table1, table2, topic1);
//        }

//        [Fact]
//        public void testTypeVariance()
//        {
//            Predicate<int, object> numberKeyPredicate = (key, value) => false;

//            new StreamsBuilder()
//                .< int, string> table("empty")
//                 .filter(numberKeyPredicate)
//                 .filterNot(numberKeyPredicate)
//                 .toStream()
//                 .To("nirvana");
//        }
//    }
