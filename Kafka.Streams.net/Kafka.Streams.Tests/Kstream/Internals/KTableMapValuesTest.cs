using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Integration;
using Kafka.Streams.Tests.Mocks;
using Kafka.Streams.Topologies;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KTableMapValuesTest
    {
        private readonly Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
        private readonly ConsumerRecordFactory<string, string> recordFactory =
            new ConsumerRecordFactory<string, string>(Serdes.String().Serializer, Serdes.String().Serializer, 0L);

        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

        private void DoTestKTable(
            StreamsBuilder builder,
            string topic1,
            MockProcessorSupplier<string, int> supplier)
        {
            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);

            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 5L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 25L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "3", 20L));
            driver.PipeInput(recordFactory.Create(topic1, "D", "4", 10L));

            Assert.Equal(
                new[]
                {
                    new KeyValueTimestamp<string, int>("A", 1, 5),
                    new KeyValueTimestamp<string, int>("B", 2, 25),
                    new KeyValueTimestamp<string, int>("C", 3, 20),
                    new KeyValueTimestamp<string, int>("D", 4, 10),
                },
                supplier.TheCapturedProcessor().processed);
        }

        [Fact]
        public void testKTable()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            IKTable<string, string> table1 = builder.Table(topic1, consumed);
            IKTable<string, int> table2 = table1.MapValues(value => value[0] - 48);

            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<string, int>();
            table2.ToStream().Process(supplier);

            DoTestKTable(builder, topic1, supplier);
        }

        [Fact]
        public void TestQueryableKTable()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            IKTable<string, string> table1 = builder.Table(topic1, consumed);
            IKTable<string, int> table2 = table1
                .MapValues(
                    value => value[0] - 48,
                    Materialized.As<string, int, IKeyValueStore<Bytes, byte[]>>("anyName")
                        .WithValueSerde(Serdes.Int()));

            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<string, int>();
            table2.ToStream().Process(supplier);

            DoTestKTable(builder, topic1, supplier);
        }

        private void DoTestValueGetter(
            StreamsBuilder builder,
            string topic1,
            IKTable<string, int> table2,
            IKTable<string, int> table3)
        {
            Topology topology = builder.Build();

            IKTableValueGetterSupplier<string, int> getterSupplier2 = table2.ValueGetterSupplier<int>();
            IKTableValueGetterSupplier<string, int> getterSupplier3 = table3.ValueGetterSupplier<int>();

            InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
            topologyBuilder.ConnectProcessorAndStateStores(table2.Name, getterSupplier2.StoreNames());
            topologyBuilder.ConnectProcessorAndStateStores(table3.Name, getterSupplier3.StoreNames());

            var driver = new TopologyTestDriverWrapper(builder.Build(), props);
            IKTableValueGetter<string, int> getter2 = getterSupplier2.Get();
            IKTableValueGetter<string, int> getter3 = getterSupplier3.Get();

            getter2.Init(driver.SetCurrentNodeForProcessorContext(table2.Name));
            getter3.Init(driver.SetCurrentNodeForProcessorContext(table3.Name));

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

            var table1 = builder.Table(topic1, consumed);
            var table2 = table1.MapValues((ValueMapper<string, int>)null,
                    Materialized.As<string, int, IKeyValueStore<Bytes, byte[]>>(storeName2)
                        .WithValueSerde(Serdes.Int()));

            var table3 = table1.MapValues<int>(value => int.Parse(value) * -1,
                    Materialized.As<string, int, IKeyValueStore<Bytes, byte[]>>(storeName3)
                        .WithValueSerde(Serdes.Int()));

            var table4 = (IKTable<string, int>)table1.MapValues<int>(value => int.Parse(value));

            Assert.Equal(storeName2, table2.QueryableStoreName);
            Assert.Equal(storeName3, table3.QueryableStoreName);
            Assert.Null(table4.QueryableStoreName);

            DoTestValueGetter(builder, topic1, table2, table3);
        }

        // [Fact]
        // public void testNotSendingOldValue()
        // {
        //     var builder = new StreamsBuilder();
        //     var topic1 = "topic1";
        // 
        //     var table1 = builder.Table(topic1, consumed);
        //     var table2 = (IKTable<string, string>)table1.MapValues(v => v);
        // 
        //     MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<string, int>();
        //     Topology topology = builder.Build().AddProcessor("proc", supplier, table2.Name);
        // 
        //     var driver = new TopologyTestDriver(topology, props);
        //     MockProcessor<string, int> proc = supplier.TheCapturedProcessor();
        // 
        //     Assert.False(table1.EnableSendingOldValues());
        //     Assert.False(table2.EnableSendingOldValues());
        // 
        //     driver.PipeInput(recordFactory.Create(topic1, "A", "01", 5L));
        //     driver.PipeInput(recordFactory.Create(topic1, "B", "01", 10L));
        //     driver.PipeInput(recordFactory.Create(topic1, "C", "01", 15L));
        //     proc.CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(1, null), 5),
        //             new KeyValueTimestamp<string, string>("B", new Change<string>(1, null), 10),
        //             new KeyValueTimestamp<string, string>("C", new Change<string>(1, null), 15));
        // 
        //     driver.PipeInput(recordFactory.Create(topic1, "A", "02", 10L));
        //     driver.PipeInput(recordFactory.Create(topic1, "B", "02", 8L));
        //     proc.CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(2, null), 10),
        //             new KeyValueTimestamp<string, string>("B", new Change<string>(2, null), 8));
        // 
        //     driver.PipeInput(recordFactory.Create(topic1, "A", "03", 20L));
        //     proc.CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(3, null), 20));
        // 
        //     driver.PipeInput(recordFactory.Create(topic1, "A", (string)null, 30L));
        //     proc.CheckAndClearProcessResult(new KeyValueTimestamp<string, string>("A", new Change<string>(null, null), 30));
        // }

        [Fact]
        public void testSendingOldValue()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            var table1 = builder.Table(topic1, consumed);
            var table2 = (IKTable<string, string>)table1.MapValues(v => int.Parse(v));
            table2.EnableSendingOldValues();

            MockProcessorSupplier<string, int?> supplier = new MockProcessorSupplier<string, int?>();
            builder.Build().AddProcessor("proc", supplier, table2.Name);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            MockProcessor<string, int?> proc = supplier.TheCapturedProcessor();

            // Assert.True(table1.SendingOldValueEnabled());
            // Assert.True(table2.SendingOldValueEnabled());

            driver.PipeInput(recordFactory.Create(topic1, "A", "01", 5L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "01", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "01", 15L));
            proc.CheckAndClearProcessResult(new KeyValueTimestamp<string, int?>("A", new Change<int?>(1, null), 5),
                    new KeyValueTimestamp<string, int?>("B", new Change<int?>(1, null), 10),
                    new KeyValueTimestamp<string, int?>("C", new Change<int?>(1, null), 15));

            driver.PipeInput(recordFactory.Create(topic1, "A", "02", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "02", 8L));
            proc.CheckAndClearProcessResult(new KeyValueTimestamp<string, int?>("A", new Change<int?>(2, 1), 10),
                    new KeyValueTimestamp<string, int?>("B", new Change<int?>(2, 1), 8));

            driver.PipeInput(recordFactory.Create(topic1, "A", "03", 20L));
            proc.CheckAndClearProcessResult(new KeyValueTimestamp<string, int?>("A", new Change<int?>(3, 2), 20));

            driver.PipeInput(recordFactory.Create(topic1, "A", (string)null, 30L));
            proc.CheckAndClearProcessResult(new KeyValueTimestamp<string, int?>("A", new Change<int?>(null, 3), 30));
        }
    }
}
