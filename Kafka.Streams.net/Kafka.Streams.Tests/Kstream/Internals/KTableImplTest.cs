using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Nodes;
using Kafka.Streams.State.KeyValues;
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

            IKTable<string, int> table2 = table1.MapValues(v => int.Parse(v));
            table2.ToStream().Process(supplier);

            IKTable<string, int> table3 = table2.Filter((key, value) => (value % 2) == 0);
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

            var expected = new List<KeyValueTimestamp<string, object>>
            {
                new KeyValueTimestamp<string, object>("A", "01", 5),
                new KeyValueTimestamp<string, object>("B", "02", 100),
                new KeyValueTimestamp<string, object>("C", "03", 0),
                new KeyValueTimestamp<string, object>("D", "04", 0),
                new KeyValueTimestamp<string, object>("A", "05", 10),
                new KeyValueTimestamp<string, object>("A", "06", 8),
            };

            Assert.Equal(expected, processors.ElementAt(0).processed);

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, object>("A", 1, 5),
                    new KeyValueTimestamp<string, object>("B", 2, 100),
                    new KeyValueTimestamp<string, object>("C", 3, 0),
                    new KeyValueTimestamp<string, object>("D", 4, 0),
                    new KeyValueTimestamp<string, object>("A", 5, 10),
                    new KeyValueTimestamp<string, object>("A", 6, 8)),
                processors.ElementAt(1).processed);

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, object>("A", null, 5),
                    new KeyValueTimestamp<string, object>("B", 2, 100),
                    new KeyValueTimestamp<string, object>("C", null, 0),
                    new KeyValueTimestamp<string, object>("D", 4, 0),
                    new KeyValueTimestamp<string, object>("A", null, 10),
                    new KeyValueTimestamp<string, object>("A", 6, 8)),
                processors.ElementAt(2).processed);

            Assert.Equal(
                Arrays.asList(
                    new KeyValueTimestamp<string, object>("A", "01", 5),
                    new KeyValueTimestamp<string, object>("B", "02", 100),
                    new KeyValueTimestamp<string, object>("C", "03", 0),
                    new KeyValueTimestamp<string, object>("D", "04", 0),
                    new KeyValueTimestamp<string, object>("A", "05", 10),
                    new KeyValueTimestamp<string, object>("A", "06", 8)),
                processors.ElementAt(3).processed);
        }

        [Fact]
        public void shouldPreserveSerdesForOperators()
        {
            StreamsBuilder builder = new StreamsBuilder();
            IKTable<string, string> table1 = builder.Table("topic-2", stringConsumed);
            ConsumedInternal<string, string> consumedInternal = new ConsumedInternal<string, string>(stringConsumed);

            KeyValueMapper<string, string, string> selector = (key, value) => key;
            ValueMapper<string, string> mapper = value => value;
            ValueJoiner<string, string, string> joiner = (value1, value2) => value1;
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
                 ((AbstractStream<string, string>)table1.Filter((key, value) => false)).KeySerde,
                consumedInternal.KeySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.Filter((key, value) => false)).ValueSerde,
                 consumedInternal.ValueSerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.Filter((key, value) => false, Materialized.With(mySerde, mySerde))).KeySerde,
                 mySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.Filter((key, value) => false, Materialized.With(mySerde, mySerde))).ValueSerde,
                 mySerde);

            Assert.Equal(
                 ((AbstractStream<string, string>)table1.FilterNot((key, value) => false)).KeySerde,
                 consumedInternal.KeySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.FilterNot((key, value) => false)).ValueSerde,
                 consumedInternal.ValueSerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.FilterNot((key, value) => false, Materialized.With(mySerde, mySerde))).KeySerde,
                 mySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.FilterNot((key, value) => false, Materialized.With(mySerde, mySerde))).ValueSerde,
                 mySerde);

            Assert.Equal(
                 ((AbstractStream<string, string>)table1.MapValues(mapper)).KeySerde,
                 consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)table1.MapValues(mapper)).ValueSerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.MapValues(mapper, Materialized.With(mySerde, mySerde))).KeySerde,
                 mySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.MapValues(mapper, Materialized.With(mySerde, mySerde))).ValueSerde,
                 mySerde);

            Assert.Equal(
                 ((AbstractStream<string, string>)table1.ToStream()).KeySerde,
                 consumedInternal.KeySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.ToStream()).ValueSerde,
                 consumedInternal.ValueSerde);
            Assert.Null(((AbstractStream<string, string>)table1.ToStream(selector)).KeySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.ToStream(selector)).ValueSerde,
                 consumedInternal.ValueSerde);

            Assert.Equal(
                 ((AbstractStream<string, string>)table1.TransformValues(valueTransformerWithKeySupplier)).KeySerde,
                 consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)table1.TransformValues(valueTransformerWithKeySupplier)).ValueSerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.TransformValues(valueTransformerWithKeySupplier, Materialized.With(mySerde, mySerde))).KeySerde,
                 mySerde);
            Assert.Equal(((AbstractStream<string, string>)table1.TransformValues(valueTransformerWithKeySupplier, Materialized.With(mySerde, mySerde))).ValueSerde,
                 mySerde);

            Assert.Null(((AbstractStream<string, string>)table1.GroupBy(KeyValuePair.Create)).KeySerde);
            Assert.Null(((AbstractStream<string, string>)table1.GroupBy(KeyValuePair.Create)).ValueSerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.GroupBy(KeyValuePair.Create, Grouped.With(mySerde, mySerde))).KeySerde,
                 mySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.GroupBy(KeyValuePair.Create, Grouped.With(mySerde, mySerde))).ValueSerde,
                 mySerde);

            Assert.Equal(
                 ((AbstractStream<string, string>)table1.Join(table1, joiner)).KeySerde,
                 consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)table1.Join(table1, joiner)).ValueSerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.Join(table1, joiner, Materialized.With(mySerde, mySerde))).KeySerde,
                 mySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.Join(table1, joiner, Materialized.With(mySerde, mySerde))).ValueSerde,
                 mySerde);

            Assert.Equal(
                 ((AbstractStream<string, string>)table1.LeftJoin(table1, joiner)).KeySerde,
                 consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)table1.LeftJoin(table1, joiner)).ValueSerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.LeftJoin(table1, joiner, Materialized.With(mySerde, mySerde))).KeySerde,
                 mySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.LeftJoin(table1, joiner, Materialized.With(mySerde, mySerde))).ValueSerde,
                 mySerde);

            Assert.Equal(
                 ((AbstractStream<string, string>)table1.OuterJoin(table1, joiner)).KeySerde,
                 consumedInternal.KeySerde);
            Assert.Null(((AbstractStream<string, string>)table1.OuterJoin(table1, joiner)).ValueSerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.OuterJoin(table1, joiner, Materialized.With(mySerde, mySerde))).KeySerde,
                 mySerde);
            Assert.Equal(
                 ((AbstractStream<string, string>)table1.OuterJoin(table1, joiner, Materialized.With(mySerde, mySerde))).ValueSerde,
                 mySerde);
        }

        [Fact]
        public void testStateStoreLazyEval()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string topic1 = "topic1";
            string topic2 = "topic2";

            IKTable<string, string> table1 = builder.Table(topic1, consumed);
            builder.Table(topic2, consumed);

            IKTable<string, int> table1Mapped = table1.MapValues(v => int.Parse(v));
            table1Mapped.Filter((key, value) => (value % 2) == 0);

            Assert.Equal(0, driver.getAllStateStores().Count);
        }

        [Fact]
        public void testStateStore()
        {
            StreamsBuilder builder = new StreamsBuilder();
            string topic1 = "topic1";
            string topic2 = "topic2";

            IKTable<string, string> table1 = builder.Table(topic1, consumed);
            IKTable<string, string> table2 = builder.Table(topic2, consumed);

            IKTable<string, int> table1Mapped = table1.MapValues(v => int.Parse(v));
            IKTable<string, int> table1MappedFiltered = table1Mapped.Filter((key, value) => (value % 2) == 0);
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

            IKTable<string, string> table1 = builder.Table(
                    topic1,
                    consumed,
                    Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>(storeName1)
                        .WithKeySerde(Serdes.String())
                        .WithValueSerde(Serdes.String()));

            table1.GroupBy(MockMapper.GetNoOpKeyValueMapper<string, string>())
                .Aggregate<string>(
                    MockInitializer.STRING_INIT,
                    MockAggregator<string, string>.TOSTRING_ADDER,
                    MockAggregator<string, string>.TOSTRING_REMOVER,
                    Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("mock-result1"));

            table1.GroupBy(MockMapper.GetNoOpKeyValueMapper<string, string>())
                .Reduce(
                    MockReducer.STRING_ADDER,
                    MockReducer.STRING_REMOVER,
                    Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("mock-result2"));

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

        [Fact]// (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullSelectorOnToStream()
        {
            table.ToStream((KeyValueMapper<string, string, string>)null);
        }

        [Fact]// (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullPredicateOnFilter()
        {
            table.Filter(null);
        }

        [Fact]// (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullPredicateOnFilterNot()
        {
            table.FilterNot(null);
        }

        [Fact]// (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullMapperOnMapValues()
        {
            table.MapValues((ValueMapper<string, string>)null);
        }

        [Fact]// (typeof(expected = NullReferenceException))
        public void ShouldNotAllowNullMapperOnMapValueWithKey()
        {
            table.MapValues((ValueMapperWithKey<string, string, string>)null);
        }

        [Fact]// (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullSelectorOnGroupBy()
        {
            table.GroupBy<string, string>(null);
        }

        [Fact]// (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullOtherTableOnJoin()
        {
            table.Join<string, string>(null, MockValueJoiner.TOSTRING_JOINER());
        }

        [Fact]
        public void shouldAllowNullStoreInJoin()
        {
            table.Join(table, MockValueJoiner.TOSTRING_JOINER());
        }

        [Fact] //(typeof(expected = NullReferenceException))
        public void shouldNotAllowNullJoinerJoin()
        {
            table.Join<string, string>(table, null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullOtherTableOnOuterJoin()
        {
            table.OuterJoin<string, string>(null, MockValueJoiner.TOSTRING_JOINER());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullJoinerOnOuterJoin()
        {
            table.OuterJoin<string, string>(table, null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullJoinerOnLeftJoin()
        {
            table.LeftJoin<string, string>(table, null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldNotAllowNullOtherTableOnLeftJoin()
        {
            table.LeftJoin(null, MockValueJoiner.TOSTRING_JOINER());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnFilterWhenMaterializedIsNull()
        {
            table.Filter((key, value) => false, (Materialized<string, string, IKeyValueStore<Bytes, byte[]>>)null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnFilterNotWhenMaterializedIsNull()
        {
            table.FilterNot((key, value) => false, (Materialized<string, string, IKeyValueStore<Bytes, byte[]>>)null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnJoinWhenMaterializedIsNull()
        {
            table.Join(table, MockValueJoiner.TOSTRING_JOINER(), (Materialized<string, string, IKeyValueStore<Bytes, byte[]>>)null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnLeftJoinWhenMaterializedIsNull()
        {
            table.LeftJoin(table, MockValueJoiner.TOSTRING_JOINER(), (Materialized<string, string, IKeyValueStore<Bytes, byte[]>>)null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnOuterJoinWhenMaterializedIsNull()
        {
            table.OuterJoin(table, MockValueJoiner.TOSTRING_JOINER(), (Materialized<string, string, IKeyValueStore<Bytes, byte[]>>)null);
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnTransformValuesWithKeyWhenTransformerSupplierIsNull()
        {
            table.TransformValues((IValueTransformerWithKeySupplier<string, string, string>)null);
        }


        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnTransformValuesWithKeyWhenMaterializedIsNull()
        {
            IValueTransformerWithKeySupplier<string, string, string> valueTransformerSupplier =
                  Mock.Of<IValueTransformerWithKeySupplier<string, string, string>>();

            table.TransformValues(valueTransformerSupplier, (Materialized<string, string, IKeyValueStore<Bytes, byte[]>>)null, Array.Empty<string>());
        }

        [Fact] // (typeof(expected = NullReferenceException))
        public void shouldThrowNullPointerOnTransformValuesWithKeyWhenStoreNamesNull()
        {
            IValueTransformerWithKeySupplier<string, string, string> valueTransformerSupplier =
                  Mock.Of<IValueTransformerWithKeySupplier<string, string, string>>();
            table.TransformValues(valueTransformerSupplier, null);
        }
    }
}
