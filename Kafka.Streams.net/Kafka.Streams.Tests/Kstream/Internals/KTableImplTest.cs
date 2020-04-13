using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Nodes;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using Kafka.Streams.Topologies;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KTableImplTest
    {
        private Consumed<string, string> stringConsumed = Consumed.With(Serdes.String(), Serdes.String());
        private Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
        private Produced<string, string> produced = Produced.With(Serdes.String(), Serdes.String());
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());
        private ConsumerRecordFactory<string, string> recordFactory =
            new ConsumerRecordFactory<string, string>(Serdes.String().Serializer, Serdes.String().Serializer, 0L);
        private ISerde<string> mySerde = Serdes.String();

        private IKTable<string, string> table;

        public KTableImplTest()
        {
            table = new StreamsBuilder().Table("test");
        }

        [Fact]
        public void testKTable()
        {
            StreamsBuilder builder = new StreamsBuilder();

            string topic1 = "topic1";
            string topic2 = "topic2";

            IKTable<string, string> table1 = builder.Table(topic1, consumed);

            MockProcessorSupplier<string, object> supplier = new MockProcessorSupplier<string, object>();
            table1.ToStream().Process(supplier);

            IKTable<string, int> table2 = table1.MapValues();
            table2.ToStream().Process(supplier);

            IKTable<string, int> table3 = table2.filter((key, value) => (value % 2) == 0);
            table3.ToStream().Process(supplier);
            table1.ToStream().To(topic2, produced);

            IKTable<string, string> table4 = builder.Table(topic2, consumed);
            table4.ToStream().Process(supplier);


            driver.PipeInput(recordFactory.Create(topic1, "A", "01", 5L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "02", 100L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "03", 0L));
            driver.PipeInput(recordFactory.Create(topic1, "D", "04", 0L));
            driver.PipeInput(recordFactory.Create(topic1, "A", "05", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "A", "06", 8L));

            List<MockProcessor<string, object>> processors = supplier.CapturedProcessors(4);

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, string>("A", "01", 5),
                    new KeyValueTimestamp<string, string>("B", "02", 100),
                    new KeyValueTimestamp<string, string>("C", "03", 0),
                    new KeyValueTimestamp<string, string>("D", "04", 0),
                    new KeyValueTimestamp<string, string>("A", "05", 10),
                    new KeyValueTimestamp<string, string>("A", "06", 8)),
                processors.ElementAt(0).processed);

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, long>("A", 1, 5),
                    new KeyValueTimestamp<string, long>("B", 2, 100),
                    new KeyValueTimestamp<string, long>("C", 3, 0),
                    new KeyValueTimestamp<string, long>("D", 4, 0),
                    new KeyValueTimestamp<string, long>("A", 5, 10),
                    new KeyValueTimestamp<string, long>("A", 6, 8)),
                processors.ElementAt(1).processed);

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, string>("A", null, 5),
                    new KeyValueTimestamp<string, string>("B", 2, 100),
                    new KeyValueTimestamp<string, string>("C", null, 0),
                    new KeyValueTimestamp<string, string>("D", 4, 0),
                    new KeyValueTimestamp<string, string>("A", null, 10),
                    new KeyValueTimestamp<string, string>("A", 6, 8)),
                processors.ElementAt(2).processed);

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, string>("A", "01", 5),
                    new KeyValueTimestamp<string, string>("B", "02", 100),
                    new KeyValueTimestamp<string, string>("C", "03", 0),
                    new KeyValueTimestamp<string, string>("D", "04", 0),
                    new KeyValueTimestamp<string, string>("A", "05", 10),
                    new KeyValueTimestamp<string, string>("A", "06", 8)),
                processors.ElementAt(3).processed);
        }

        [Fact]
        public void shouldPreserveSerdesForOperators()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKTable<string, string> table1 = builder.Table("topic-2", stringConsumed);
            ConsumedInternal<string, string> consumedInternal = new ConsumedInternal<>(stringConsumed);

            IKeyValueMapper<string, string, string> selector = (key, value) => key;
            IValueMapper<string, string> mapper = value => value;
            IValueJoiner<string, string, string> joiner = (value1, value2) => value1;
            //            IValueTransformerWithKeySupplier<string, string, string> valueTransformerWithKeySupplier =
            //                () => new ValueTransformerWithKey<string, string, string>()
            //                {
            //
            //
            //                public void Init(IProcessorContext context) { }
            //
            //
            //            public string transform(string key, string value)
            //            {
            //                return value;
            //            }
            //
            //
            //            public void Close() { }
            //        };

            Assert.Equal(
                 ((AbstractStream)table1.filter((key, value) => false)).keySerde(),
                consumedInternal.keySerde());
            Assert.Equal(
                 ((AbstractStream)table1.filter((key, value) => false)).valueSerde(),
                 consumedInternal.valueSerde());
            Assert.Equal(
                 ((AbstractStream)table1.filter((key, value) => false, Materialized.With(mySerde, mySerde))).keySerde(),
                 mySerde);
            Assert.Equal(
                 ((AbstractStream)table1.filter((key, value) => false, Materialized.With(mySerde, mySerde))).valueSerde(),
                 mySerde);

            Assert.Equal(
                 ((AbstractStream)table1.filterNot((key, value) => false)).keySerde(),
                 consumedInternal.keySerde());
            Assert.Equal(
                 ((AbstractStream)table1.filterNot((key, value) => false)).valueSerde(),
                 consumedInternal.valueSerde());
            Assert.Equal(
                 ((AbstractStream)table1.filterNot((key, value) => false, Materialized.With(mySerde, mySerde))).keySerde(),
                 mySerde);
            Assert.Equal(
                 ((AbstractStream)table1.filterNot((key, value) => false, Materialized.With(mySerde, mySerde))).valueSerde(),
                 mySerde);

            Assert.Equal(
                 ((AbstractStream)table1.MapValues(mapper)).keySerde(),
                 consumedInternal.keySerde());
            Assert.Null(((AbstractStream)table1.MapValues(mapper)).valueSerde());
            Assert.Equal(
                 ((AbstractStream)table1.MapValues(mapper, Materialized.With(mySerde, mySerde))).keySerde(),
                 mySerde);
            Assert.Equal(
                 ((AbstractStream)table1.MapValues(mapper, Materialized.With(mySerde, mySerde))).valueSerde(),
                 mySerde);

            Assert.Equal(
                 ((AbstractStream)table1.ToStream()).keySerde(),
                 consumedInternal.keySerde());
            Assert.Equal(
                 ((AbstractStream)table1.ToStream()).valueSerde(),
                 consumedInternal.valueSerde());
            Assert.Null(((AbstractStream)table1.toStream(selector)).keySerde());
            Assert.Equal(
                 ((AbstractStream)table1.toStream(selector)).valueSerde(),
                 consumedInternal.valueSerde());

            Assert.Equal(
                 ((AbstractStream)table1.transformValues(valueTransformerWithKeySupplier)).keySerde(),
                 consumedInternal.keySerde());
            Assert.Null(((AbstractStream)table1.transformValues(valueTransformerWithKeySupplier)).valueSerde());
            Assert.Equal(
                 ((AbstractStream)table1.transformValues(valueTransformerWithKeySupplier, Materialized.With(mySerde, mySerde))).keySerde(),
                 mySerde);
            Assert.Equal(((AbstractStream)table1.transformValues(valueTransformerWithKeySupplier, Materialized.With(mySerde, mySerde))).valueSerde(),
                 mySerde);

            Assert.Null(((AbstractStream)table1.GroupBy(KeyValuePair.Create)).keySerde());
            Assert.Null(((AbstractStream)table1.GroupBy(KeyValuePair.Create)).valueSerde());
            Assert.Equal(
                 ((AbstractStream)table1.GroupBy(KeyValuePair.Create, Grouped.With(mySerde, mySerde))).keySerde(),
                 mySerde);
            Assert.Equal(
                 ((AbstractStream)table1.GroupBy(KeyValuePair.Create, Grouped.With(mySerde, mySerde))).valueSerde(),
                 mySerde);

            Assert.Equal(
                 ((AbstractStream)table1.Join(table1, joiner)).keySerde(),
                 consumedInternal.keySerde());
            Assert.Null(((AbstractStream)table1.Join(table1, joiner)).valueSerde());
            Assert.Equal(
                 ((AbstractStream)table1.Join(table1, joiner, Materialized.With(mySerde, mySerde))).keySerde(),
                 mySerde);
            Assert.Equal(
                 ((AbstractStream)table1.Join(table1, joiner, Materialized.With(mySerde, mySerde))).valueSerde(),
                 mySerde);

            Assert.Equal(
                 ((AbstractStream)table1.LeftJoin(table1, joiner)).keySerde(),
                 consumedInternal.keySerde());
            Assert.Null(((AbstractStream)table1.LeftJoin(table1, joiner)).valueSerde());
            Assert.Equal(
                 ((AbstractStream)table1.LeftJoin(table1, joiner, Materialized.With(mySerde, mySerde))).keySerde(),
                 mySerde);
            Assert.Equal(
                 ((AbstractStream)table1.LeftJoin(table1, joiner, Materialized.With(mySerde, mySerde))).valueSerde(),
                 mySerde);

            Assert.Equal(
                 ((AbstractStream)table1.OuterJoin(table1, joiner)).keySerde(),
                 consumedInternal.keySerde());
            Assert.Null(((AbstractStream)table1.OuterJoin(table1, joiner)).valueSerde());
            Assert.Equal(
                 ((AbstractStream)table1.OuterJoin(table1, joiner, Materialized.With(mySerde, mySerde))).keySerde(),
                 mySerde);
            Assert.Equal(
                 ((AbstractStream)table1.OuterJoin(table1, joiner, Materialized.With(mySerde, mySerde))).valueSerde(),
                 mySerde);
        }

        [Fact]
        public void testStateStoreLazyEval()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string topic1 = "topic1";
            string topic2 = "topic2";

            KTable<string, string, string> table1 =
                (KTable<string, string, string>)builder.Table(topic1, consumed);
            builder.Table(topic2, consumed);

            KTable<string, string, int> table1Mapped =
                (KTable<string, string, int>)table1.MapValues();
            table1Mapped.Filter((key, value) => (value % 2) == 0);

            Assert.Equal(0, driver.getAllStateStores().Count);
        }

        [Fact]
        public void testStateStore()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string topic1 = "topic1";
            string topic2 = "topic2";

            KTable<string, string, string> table1 =
                (KTable<string, string, string>)builder.Table(topic1, consumed);
            KTable<string, string, string> table2 =
                (KTable<string, string, string>)builder.Table(topic2, consumed);

            KTable<string, string, int> table1Mapped =
                (KTable<string, string, int>)table1.MapValues();
            KTable<string, int, int> table1MappedFiltered =
                (KTable<string, int, int>)table1Mapped.filter((key, value) => (value % 2) == 0);
            table2.Join(table1MappedFiltered, (v1, v2) => v1 + v2);

            Assert.Equal(2, driver.getAllStateStores().Count);
        }

        private void assertTopologyContainsProcessor(Topology topology, string processorName)
        {
            foreach (Subtopology subtopology in topology.Describe().subtopologies)
            {
                foreach (var node in subtopology.nodes)
                {
                    if (node.Name.Equals(processorName))
                    {
                        return;
                    }
                }
            }

            throw new Exception("No processor named '" + processorName + "'"
                + "found in the provided Topology:\n" + topology.Describe());
        }

        [Fact]
        public void shouldCreateSourceAndSinkNodesForRepartitioningTopic()// throws Exception {
        {
            StreamsBuilder builder = new StreamsBuilder();
            string topic1 = "topic1";
            string storeName1 = "storeName1";

            KTable<string, string, string> table1 =
                (KTable<string, string, string>)builder.Table(
                    topic1,
                    consumed,
                    Materialized.As < string, string, IKeyValueStore<Bytes, byte[]>(storeName1)
                        .WithKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                );

            table1.GroupBy(MockMapper.noOpKeyValueMapper())
                .Aggregate(
                    MockInitializer.STRING_INIT,
                    MockAggregator.TOSTRING_ADDER,
                    MockAggregator.TOSTRING_REMOVER,
                    Materialized.As("mock-result1"));

            table1.GroupBy(MockMapper.noOpKeyValueMapper())
                .Reduce(
                    MockReducer.STRING_ADDER,
                    MockReducer.STRING_REMOVER,
                    Materialized.As("mock-result2"));

            Topology topology = builder.Build();


            Assert.Equal(3, driver.getAllStateStores().Count);

            AssertTopologyContainsProcessor(topology, "KSTREAM-SINK-0000000003");
            AssertTopologyContainsProcessor(topology, "KSTREAM-SOURCE-0000000004");
            AssertTopologyContainsProcessor(topology, "KSTREAM-SINK-0000000007");
            AssertTopologyContainsProcessor(topology, "KSTREAM-SOURCE-0000000008");

            Field valSerializerField = ((SinkNode)driver.getProcessor("KSTREAM-SINK-0000000003"))
                .GetType()
                .getDeclaredField("valSerializer");
            Field valDeserializerField = ((SourceNode)driver.getProcessor("KSTREAM-SOURCE-0000000004"))
                .GetType()
                .getDeclaredField("valDeserializer");
            valSerializerField.setAccessible(true);
            valDeserializerField.setAccessible(true);

            Assert.NotNull(((ChangedSerializer)valSerializerField.Get(driver.getProcessor("KSTREAM-SINK-0000000003"))).inner());
            Assert.NotNull(((ChangedDeserializer)valDeserializerField.Get(driver.getProcessor("KSTREAM-SOURCE-0000000004"))).inner());
            Assert.NotNull(((ChangedSerializer)valSerializerField.Get(driver.getProcessor("KSTREAM-SINK-0000000007"))).inner());
            Assert.NotNull(((ChangedDeserializer)valDeserializerField.Get(driver.getProcessor("KSTREAM-SOURCE-0000000008"))).inner());
        }

        [Fact]// (typeof(expected = NullPointerException))
        public void shouldNotAllowNullSelectorOnToStream()
        {
            table.ToStream((IKeyValueMapper)null);
        }

        [Fact]// (typeof(expected = NullPointerException))
        public void shouldNotAllowNullPredicateOnFilter()
        {
            table.Filter(null);
        }

        [Fact]// (typeof(expected = NullPointerException))
        public void shouldNotAllowNullPredicateOnFilterNot()
        {
            table.FilterNot(null);
        }

        [Fact]// (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnMapValues()
        {
            table.MapValues((ValueMapper)null);
        }

        [Fact]// (typeof(expected = NullPointerException))
        public void shouldNotAllowNullMapperOnMapValueWithKey()
        {
            table.MapValues((ValueMapperWithKey)null);
        }

        [Fact]// (typeof(expected = NullPointerException))
        public void shouldNotAllowNullSelectorOnGroupBy()
        {
            table.GroupBy(null);
        }

        [Fact]// (typeof(expected = NullPointerException))
        public void shouldNotAllowNullOtherTableOnJoin()
        {
            table.Join(null, MockValueJoiner.TOSTRING_JOINER);
        }

        [Fact]
        public void shouldAllowNullStoreInJoin()
        {
            table.Join(table, MockValueJoiner.TOSTRING_JOINER);
        }

        [Fact] //(typeof(expected = NullPointerException))
        public void shouldNotAllowNullJoinerJoin()
        {
            table.Join(table, null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullOtherTableOnOuterJoin()
        {
            table.OuterJoin(null, MockValueJoiner.TOSTRING_JOINER);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullJoinerOnOuterJoin()
        {
            table.OuterJoin(table, null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullJoinerOnLeftJoin()
        {
            table.LeftJoin(table, null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldNotAllowNullOtherTableOnLeftJoin()
        {
            table.LeftJoin(null, MockValueJoiner.TOSTRING_JOINER);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnFilterWhenMaterializedIsNull()
        {
            table.Filter((key, value) => false, (Materialized)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnFilterNotWhenMaterializedIsNull()
        {
            table.FilterNot((key, value) => false, (Materialized)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnJoinWhenMaterializedIsNull()
        {
            table.Join(table, MockValueJoiner.TOSTRING_JOINER, (Materialized)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnLeftJoinWhenMaterializedIsNull()
        {
            table.LeftJoin(table, MockValueJoiner.TOSTRING_JOINER, (Materialized)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnOuterJoinWhenMaterializedIsNull()
        {
            table.OuterJoin(table, MockValueJoiner.TOSTRING_JOINER, (Materialized)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnTransformValuesWithKeyWhenTransformerSupplierIsNull()
        {
            table.transformValues((ValueTransformerWithKeySupplier)null);
        }


        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnTransformValuesWithKeyWhenMaterializedIsNull()
        {
            IValueTransformerWithKeySupplier<string, string, string> valueTransformerSupplier =
                  Mock.Of<IValueTransformerWithKeySupplier<string, string, string>>();

            table.TransformValues(valueTransformerSupplier, (Materialized)null);
        }

        [Fact] // (typeof(expected = NullPointerException))
        public void shouldThrowNullPointerOnTransformValuesWithKeyWhenStoreNamesNull()
        {
            IValueTransformerWithKeySupplier<string, string, string> valueTransformerSupplier =
                  Mock.Of<IValueTransformerWithKeySupplier<string, string, string>>();
            table.TransformValues(valueTransformerSupplier, (string[])null);
        }
    }
}
