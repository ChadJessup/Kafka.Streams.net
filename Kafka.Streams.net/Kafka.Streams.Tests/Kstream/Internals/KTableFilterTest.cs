//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using Kafka.Streams.Topologies;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream.Internals
//{
//    public class KTableFilterTest
//    {
//        private Consumed<string, int> consumed = Consumed.With(Serdes.String(), Serdes.Int());
//        private ConsumerRecordFactory<string, int> recordFactory =
//            new ConsumerRecordFactory<string, int>(Serdes.String(), Serdes.Int(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.Int());

//        public KTableFilterTest()
//        {
//            // disable caching at the config level
//            props.Set(StreamsConfig.CacheMaxBytesBuffering, "0");
//        }

//        private Func<string, int, bool> predicate = (key, value) => (value % 2) == 0;

//        private void doTestKTable(StreamsBuilder builder,
//                                  IKTable<string, int> table2,
//                                  IKTable<string, int> table3,
//                                  string topic)
//        {
//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            table2.ToStream().Process(supplier);
//            table3.ToStream().Process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            driver.PipeInput(recordFactory.Create(topic, "A", 1, 10L));
//            driver.PipeInput(recordFactory.Create(topic, "B", 2, 5L));
//            driver.PipeInput(recordFactory.Create(topic, "C", 3, 8L));
//            driver.PipeInput(recordFactory.Create(topic, "D", 4, 14L));
//            driver.PipeInput(recordFactory.Create(topic, "A", null, 18L));
//            driver.PipeInput(recordFactory.Create(topic, "B", null, 15L));

//            List<MockProcessor<string, int>> processors = supplier.capturedProcessors(2);

//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", null, 10),
//                new KeyValueTimestamp<string, string>("B", 2, 5),
//                new KeyValueTimestamp<string, string>("C", null, 8),
//                new KeyValueTimestamp<string, string>("D", 4, 14),
//                new KeyValueTimestamp<string, string>("A", null, 18),
//                new KeyValueTimestamp<string, string>("B", null, 15));
//            processors.Get(1).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", 1, 10),
//                new KeyValueTimestamp<string, string>("B", null, 5),
//                new KeyValueTimestamp<string, string>("C", 3, 8),
//                new KeyValueTimestamp<string, string>("D", null, 14),
//                new KeyValueTimestamp<string, string>("A", null, 18),
//                new KeyValueTimestamp<string, string>("B", null, 15));
//        }

//        [Fact]
//        public void shouldPassThroughWithoutMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            IKTable<string, int> table1 = builder.Table(topic1, consumed);
//            IKTable<string, int> table2 = table1.Filter(predicate);
//            IKTable<string, int> table3 = table1.FilterNot(predicate);

//            Assert.Null(table1.QueryableStoreName);
//            Assert.Null(table2.QueryableStoreName);
//            Assert.Null(table3.QueryableStoreName);

//            doTestKTable(builder, table2, table3, topic1);
//        }

//        [Fact]
//        public void shouldPassThroughOnMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            IKTable<string, int> table1 = builder.Table(topic1, consumed);
//            IKTable<string, int> table2 = table1.Filter(predicate, Materialized.As("store2"));
//            IKTable<string, int> table3 = table1.FilterNot(predicate);

//            Assert.Null(table1.QueryableStoreName);
//            Assert.Equal("store2", table2.QueryableStoreName);
//            Assert.Null(table3.QueryableStoreName);

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

//            var table1 = builder.Table(topic1, consumed);
//            var table2 = (IKTable<string, int>)table1.Filter(predicate, Materialized.As("store2"));
//            var table3 = (IKTable<string, int>)table1.FilterNot(predicate, Materialized.As("store3"));
//            var table4 = (IKTable<string, int>)table1.FilterNot(predicate);

//            Assert.Null(table1.QueryableStoreName);
//            Assert.Equal("store2", table2.QueryableStoreName);
//            Assert.Equal("store3", table3.QueryableStoreName);
//            Assert.Null(table4.QueryableStoreName);

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

//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(1, null), 5),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(1, null), 10),
//                new KeyValueTimestamp<string, string>("C", new Change<string>(1, null), 15));
//            processors.Get(1).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(null, null), 5),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(null, null), 10),
//                new KeyValueTimestamp<string, string>("C", new Change<string>(null, null), 15));

//            driver.PipeInput(recordFactory.Create(topic1, "A", 2, 15L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", 2, 8L));

//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(2, null), 15),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(2, null), 8));
//            processors.Get(1).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(2, null), 15),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(2, null), 8));

//            driver.PipeInput(recordFactory.Create(topic1, "A", 3, 20L));

//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(3, null), 20));
//            processors.Get(1).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(null, null), 20));
//            driver.PipeInput(recordFactory.Create(topic1, "A", null, 10L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", null, 20L));

//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(null, null), 10),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(null, null), 20));
//            processors.Get(1).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(null, null), 10),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(null, null), 20));
//        }


//        [Fact]
//        public void shouldNotSendOldValuesWithoutMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 = builder.Table(topic1, consumed);
//            var table2 = (IKTable<string, int>)table1.Filter(predicate);

//            doTestNotSendingOldValue(builder, table1, table2, topic1);
//        }

//        [Fact]
//        public void shouldNotSendOldValuesOnMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 =
//                builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, int>)table1.Filter(predicate, Materialized.As("store2"));

//            doTestNotSendingOldValue(builder, table1, table2, topic1);
//        }

//        private void doTestSendingOldValue(StreamsBuilder builder,
//                                           IKTable<string, int> table1,
//                                           IKTable<string, int> table2,
//                                           string topic1)
//        {
//            table2.EnableSendingOldValues();

//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            Topology topology = builder.Build();

//            topology.AddProcessor("proc1", supplier, table1.Name);
//            topology.AddProcessor("proc2", supplier, table2.Name);

//            var driver = new TopologyTestDriver(topology, props);
//            driver.PipeInput(recordFactory.Create(topic1, "A", 1, 5L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", 1, 10L));
//            driver.PipeInput(recordFactory.Create(topic1, "C", 1, 15L));

//            List<MockProcessor<string, int>> processors = supplier.capturedProcessors(2);

//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(1, null), 5),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(1, null), 10),
//                new KeyValueTimestamp<string, string>("C", new Change<string>(1, null), 15));
//            processors.Get(1).checkEmptyAndClearProcessResult();

//            driver.PipeInput(recordFactory.Create(topic1, "A", 2, 15L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", 2, 8L));

//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(2, 1), 15),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(2, 1), 8));
//            processors.Get(1).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(2, null), 15),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(2, null), 8));

//            driver.PipeInput(recordFactory.Create(topic1, "A", 3, 20L));

//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(3, 2), 20));
//            processors.Get(1).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(null, 2), 20));

//            driver.PipeInput(recordFactory.Create(topic1, "A", null, 10L));
//            driver.PipeInput(recordFactory.Create(topic1, "B", null, 20L));

//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(null, 3), 10),
//                new KeyValueTimestamp<string, string>("B", new Change<string>(null, 2), 20));
//            processors.Get(1).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("B", new Change<string>(null, 2), 20));
//        }

//        [Fact]
//        public void shouldSendOldValuesWhenEnabledWithoutMaterialization()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            var table1 =
//                (IKTable<string, int, int>)builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, int, int>)table1.Filter(predicate);

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
//                (IKTable<string, int, int>)table1.Filter(predicate, Materialized.As("store2"));

//            doTestSendingOldValue(builder, table1, table2, topic1);
//        }

//        private void doTestSkipNullOnMaterialization(
//            StreamsBuilder builder,
//            IKTable<string, string> table1,
//            IKTable<string, string> table2,
//            string topic1)
//        {
//            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<string, string>();
//            Topology topology = builder.Build();

//            topology.AddProcessor("proc1", supplier, table1.Name);
//            topology.AddProcessor("proc2", supplier, table2.Name);

//            ConsumerRecordFactory<string, string> stringRecordFactory =
//                new ConsumerRecordFactory<string, string>(Serdes.String(), Serdes.String(), 0L);
//            var driver = new TopologyTestDriver(topology, props);

//            driver.PipeInput(stringRecordFactory.Create(topic1, "A", "reject", 5L));
//            driver.PipeInput(stringRecordFactory.Create(topic1, "B", "reject", 10L));
//            driver.PipeInput(stringRecordFactory.Create(topic1, "C", "reject", 20L));

//            List<MockProcessor<string, string>> processors = supplier.capturedProcessors(2);
//            processors.Get(0).CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>("reject", null), 5),
//                new KeyValueTimestamp<string, string>("B", new Change<string>("reject", null), 10),
//                new KeyValueTimestamp<string, string>("C", new Change<string>("reject", null), 20));
//            processors.Get(1).checkEmptyAndClearProcessResult();
//        }

//        [Fact]
//        public void shouldSkipNullToRepartitionWithoutMaterialization()
//        {
//            // Do not explicitly set EnableSendingOldValues. Let a further downstream stateful operator trigger it instead.
//            var builder = new StreamsBuilder();

//            var topic1 = "topic1";

//            Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
//            var table1 =
//                (IKTable<string, string>)builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, string>)table1.Filter((key, value) => value.EqualsIgnorecase("accept"))
//                    .GroupBy(MockMapper.GetNoOpKeyValueMapper())
//                    .Reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER);

//            doTestSkipNullOnMaterialization(builder, table1, table2, topic1);
//        }

//        [Fact]
//        public void shouldSkipNullToRepartitionOnMaterialization()
//        {
//            // Do not explicitly set EnableSendingOldValues. Let a further downstream stateful operator trigger it instead.
//            var builder = new StreamsBuilder();

//            var topic1 = "topic1";

//            Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
//            var table1 =
//                (IKTable<string, string>)builder.Table(topic1, consumed);
//            var table2 =
//                (IKTable<string, string>)table1.Filter((key, value) => value.Equals("accept", StringComparison.OrdinalIgnoreCase), Materialized.As("store2"))
//                    .GroupBy(MockMapper.GetNoOpKeyValueMapper())
//                    .Reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, Materialized.As("mock-result"));

//            doTestSkipNullOnMaterialization(builder, table1, table2, topic1);
//        }

//        [Fact]
//        public void testTypeVariance()
//        {
//            Func<int, string, bool> numberKeyPredicate = (key, value) => false;

//            new StreamsBuilder()
//                 .Table<int, string>("empty")
//                 .Filter(numberKeyPredicate)
//                 .FilterNot(numberKeyPredicate)
//                 .ToStream()
//                 .To("nirvana");
//        }
//    }
//}
