using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KTableMapValuesTest
    {
        private Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
        private ConsumerRecordFactory<string, string> recordFactory =
            new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

        private void doTestKTable(StreamsBuilder builder,
                                  string topic1,
                                  MockProcessorSupplier<string, int> supplier)
        {

            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 5L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 25L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "3", 20L));
            driver.PipeInput(recordFactory.Create(topic1, "D", "4", 10L));
            Assert.Equal(new KeyValueTimestamp<>("A", 1, 5),
                     new KeyValueTimestamp<>("B", 2, 25),
                     new KeyValueTimestamp<>("C", 3, 20),
                     new KeyValueTimestamp<>("D", 4, 10)), supplier.TheCapturedProcessor().processed);
        }

        [Fact]
        public void testKTable()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            IKTable<string, string> table1 = builder.Table(topic1, consumed);
            IKTable<string, int> table2 = table1.MapValues(value => value.charAt(0) - 48);

            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
            table2.ToStream().Process(supplier);

            doTestKTable(builder, topic1, supplier);
        }

        [Fact]
        public void testQueryableKTable()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            IKTable<string, string> table1 = builder.Table(topic1, consumed);
            IKTable<string, int> table2 = table1
                .MapValues(
                    value => value.charAt(0) - 48,
                    Materialized.As < string, int, IKeyValueStore<Bytes, byte[]>("anyName")
                        .withValueSerde(Serdes.Int()));

            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
            table2.ToStream().Process(supplier);

            doTestKTable(builder, topic1, supplier);
        }

        private void doTestValueGetter(StreamsBuilder builder,
                                       string topic1,
                                       IKTable<string, int> table2,
                                       IKTable<string, int> table3)
        {

            Topology topology = builder.Build();

            KTableValueGetterSupplier<string, int> getterSupplier2 = table2.valueGetterSupplier();
            KTableValueGetterSupplier<string, int> getterSupplier3 = table3.valueGetterSupplier();

            InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
            topologyBuilder.ConnectProcessorAndStateStores(table2.Name, getterSupplier2.storeNames());
            topologyBuilder.ConnectProcessorAndStateStores(table3.Name, getterSupplier3.storeNames());

            var driver = new TopologyTestDriverWrapper(builder.Build(), props);
            IKTableValueGetter<string, int> getter2 = getterSupplier2.Get();
            IKTableValueGetter<string, int> getter3 = getterSupplier3.Get();

            getter2.Init(driver.setCurrentNodeForProcessorContext(table2.Name));
            getter3.Init(driver.setCurrentNodeForProcessorContext(table3.Name));

            driver.PipeInput(recordFactory.Create(topic1, "A", "01", 50L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "01", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "01", 30L));

            Assert.Equal(ValueAndTimestamp.Make(1, 50L), getter2.Get("A"));
            Assert.Equal(ValueAndTimestamp.Make(1, 10L), getter2.Get("B"));
            Assert.Equal(ValueAndTimestamp.Make(1, 30L), getter2.Get("C"));

            Assert.Equal(ValueAndTimestamp.Make(-1, 50L), getter3.Get("A"));
            Assert.Equal(ValueAndTimestamp.Make(-1, 10L), getter3.Get("B"));
            Assert.Equal(ValueAndTimestamp.Make(-1, 30L), getter3.Get("C"));

            driver.PipeInput(recordFactory.Create(topic1, "A", "02", 25L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "02", 20L));

            Assert.Equal(ValueAndTimestamp.Make(2, 25L), getter2.Get("A"));
            Assert.Equal(ValueAndTimestamp.Make(2, 20L), getter2.Get("B"));
            Assert.Equal(ValueAndTimestamp.Make(1, 30L), getter2.Get("C"));

            Assert.Equal(ValueAndTimestamp.Make(-2, 25L), getter3.Get("A"));
            Assert.Equal(ValueAndTimestamp.Make(-2, 20L), getter3.Get("B"));
            Assert.Equal(ValueAndTimestamp.Make(-1, 30L), getter3.Get("C"));

            driver.PipeInput(recordFactory.Create(topic1, "A", "03", 35L));

            Assert.Equal(ValueAndTimestamp.Make(3, 35L), getter2.Get("A"));
            Assert.Equal(ValueAndTimestamp.Make(2, 20L), getter2.Get("B"));
            Assert.Equal(ValueAndTimestamp.Make(1, 30L), getter2.Get("C"));

            Assert.Equal(ValueAndTimestamp.Make(-3, 35L), getter3.Get("A"));
            Assert.Equal(ValueAndTimestamp.Make(-2, 20L), getter3.Get("B"));
            Assert.Equal(ValueAndTimestamp.Make(-1, 30L), getter3.Get("C"));

            driver.PipeInput(recordFactory.Create(topic1, "A", (string)null, 1L));

            Assert.Null(getter2.Get("A"));
            Assert.Equal(ValueAndTimestamp.Make(2, 20L), getter2.Get("B"));
            Assert.Equal(ValueAndTimestamp.Make(1, 30L), getter2.Get("C"));

            Assert.Null(getter3.Get("A"));
            Assert.Equal(ValueAndTimestamp.Make(-2, 20L), getter3.Get("B"));
            Assert.Equal(ValueAndTimestamp.Make(-1, 30L), getter3.Get("C"));
        }

        [Fact]
        public void testQueryableValueGetter()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";
            var storeName2 = "store2";
            var storeName3 = "store3";

            var table1 = (IKTable<string, string>)builder.Table(topic1, consumed);
            var table2 = (IKTable<string, int>)table1.MapValues(
                null,
                    Materialized.As < string, int, IKeyValueStore<Bytes, byte[]>(storeName2)
                        .withValueSerde(Serdes.Int()));
            var table3 = (IKTable<string, int>)table1.MapValues(
                    value => value * (-1),
                    Materialized.As < string, int, IKeyValueStore<Bytes, byte[]>(storeName3)
                        .withValueSerde(Serdes.Int()));
            var table4 = (IKTable<string, int>)table1.MapValues();

            Assert.Equal(storeName2, table2.QueryableStoreName);
            Assert.Equal(storeName3, table3.QueryableStoreName);
            Assert.Null(table4.QueryableStoreName);

            doTestValueGetter(builder, topic1, table2, table3);
        }

        [Fact]
        public void testNotSendingOldValue()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            var table1 = (IKTable<string, string>)builder.Table(topic1, consumed);
            var table2 = (IKTable<string, string>)table1.MapValues();

            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
            Topology topology = builder.Build().AddProcessor("proc", supplier, table2.Name);

            var driver = new TopologyTestDriver(topology, props);
            MockProcessor<string, int> proc = supplier.TheCapturedProcessor();

            Assert.False(table1.sendingOldValueEnabled());
            Assert.False(table2.sendingOldValueEnabled());

            driver.PipeInput(recordFactory.Create(topic1, "A", "01", 5L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "01", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "01", 15L));
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                    new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                    new KeyValueTimestamp<>("C", new Change<>(1, null), 15));

            driver.PipeInput(recordFactory.Create(topic1, "A", "02", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "02", 8L));
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, null), 10),
                    new KeyValueTimestamp<>("B", new Change<>(2, null), 8));

            driver.PipeInput(recordFactory.Create(topic1, "A", "03", 20L));
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, null), 20));

            driver.PipeInput(recordFactory.Create(topic1, "A", (string)null, 30L));
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, null), 30));
        }

        [Fact]
        public void testSendingOldValue()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            var table1 =
                (IKTable<string, string>)builder.Table(topic1, consumed);
            var table2 =
                (IKTable<string, string>)table1.MapValues();
            table2.enableSendingOldValues();

            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
            builder.Build().AddProcessor("proc", supplier, table2.Name);

            var driver = new TopologyTestDriver(builder.Build(), props);
            MockProcessor<string, int> proc = supplier.TheCapturedProcessor();

            Assert.True(table1.sendingOldValueEnabled());
            Assert.True(table2.sendingOldValueEnabled());

            driver.PipeInput(recordFactory.Create(topic1, "A", "01", 5L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "01", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "01", 15L));
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(1, null), 5),
                    new KeyValueTimestamp<>("B", new Change<>(1, null), 10),
                    new KeyValueTimestamp<>("C", new Change<>(1, null), 15));

            driver.PipeInput(recordFactory.Create(topic1, "A", "02", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "02", 8L));
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(2, 1), 10),
                    new KeyValueTimestamp<>("B", new Change<>(2, 1), 8));

            driver.PipeInput(recordFactory.Create(topic1, "A", "03", 20L));
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(3, 2), 20));

            driver.PipeInput(recordFactory.Create(topic1, "A", (string)null, 30L));
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>("A", new Change<>(null, 3), 30));
        }
    }
}
