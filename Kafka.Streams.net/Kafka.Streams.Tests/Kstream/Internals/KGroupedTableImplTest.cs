namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KGroupedTableImplTest
//    {
//        private StreamsBuilder builder = new StreamsBuilder();
//        private static string INVALID_STORE_NAME = "~foo bar~";
//        private IKGroupedTable<string, string> groupedTable;
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.Int());
//        private string topic = "input";


//        public void before()
//        {
//            groupedTable = builder
//                .Table("blah", Consumed.with(Serdes.String(), Serdes.String()))
//                .groupBy(MockMapper.selectValueKeyValueMapper());
//        }

//        [Fact]
//        public void shouldNotAllowInvalidStoreNameOnAggregate()
//        {
//            groupedTable.aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As(INVALID_STORE_NAME));
//        }

//        [Fact]
//        public void shouldNotAllowNullInitializerOnAggregate()
//        {
//            groupedTable.aggregate(
//                null,
//                MockAggregator.TOSTRING_ADDER,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotAllowNullAdderOnAggregate()
//        {
//            groupedTable.aggregate(
//                MockInitializer.STRING_INIT,
//                null,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotAllowNullSubtractorOnAggregate()
//        {
//            groupedTable.aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                null,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotAllowNullAdderOnReduce()
//        {
//            groupedTable.reduce(
//                null,
//                MockReducer.STRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]//(expected = NullPointerException))
//        public void shouldNotAllowNullSubtractorOnReduce()
//        {
//            groupedTable.reduce(
//                MockReducer.STRING_ADDER,
//                null,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotAllowInvalidStoreNameOnReduce()
//        {
//            groupedTable.reduce(
//                MockReducer.STRING_ADDER,
//                MockReducer.STRING_REMOVER,
//                Materialized.As(INVALID_STORE_NAME));
//        }

//        private MockProcessorSupplier<string, int> getReducedResults(IKTable<string, int> inputKTable)
//        {
//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<>();
//            inputKTable
//                .toStream()
//                .process(supplier);
//            return supplier;
//        }

//        private void assertReduced(Dictionary<string, ValueAndTimestamp<int>> reducedResults,
//                                   string topic,
//                                   var driver)
//        {
//            ConsumerRecordFactory<string, double> recordFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), new DoubleSerializer());
//            driver.pipeInput(recordFactory.create(topic, "A", 1.1, 10));
//            driver.pipeInput(recordFactory.create(topic, "B", 2.2, 11));

//            Assert.Equal(ValueAndTimestamp.make(1, 10L), reducedResults["A"]);
//            Assert.Equal(ValueAndTimestamp.make(2, 11L), reducedResults["B"]);

//            driver.pipeInput(recordFactory.create(topic, "A", 2.6, 30));
//            driver.pipeInput(recordFactory.create(topic, "B", 1.3, 30));
//            driver.pipeInput(recordFactory.create(topic, "A", 5.7, 50));
//            driver.pipeInput(recordFactory.create(topic, "B", 6.2, 20));

//            Assert.Equal(ValueAndTimestamp.make(5, 50L), reducedResults["A"]);
//            Assert.Equal(ValueAndTimestamp.make(6, 30L), reducedResults["B"]);
//        }

//        [Fact]
//        public void shouldReduce()
//        {
//            IKeyValueMapper<string, int, KeyValuePair<string, int>> intProjection =
//                (key, value) => KeyValuePair.Create(key, value.intValue());

//            IKTable<string, int> reduced = builder
//                .Table(
//                    topic,
//                    Consumed.with(Serdes.String(), Serdes.Double()),
//                    Materialize.As < string, double, IKeyValueStore<Bytes, byte[]>("store")
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(Serdes.Double()))
//                .groupBy(intProjection)
//                .reduce(
//                    MockReducer.INTEGER_ADDER,
//                    MockReducer.INTEGER_SUBTRACTOR,
//                    Materialized.As("reduced"));

//            MockProcessorSupplier<string, int> supplier = getReducedResults(reduced);
//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//           assertReduced(supplier.theCapturedProcessor().lastValueAndTimestampPerKey, topic, driver);
//                Assert.Equal(reduced.queryableStoreName, "reduced");
//            }
//    }

//        [Fact]
//        public void shouldReduceWithInternalStoreName()
//        {
//            IKeyValueMapper<string, int, KeyValuePair<string, int>> intProjection =
//                (key, value) => KeyValuePair.Create(key, value.intValue());

//            IKTable<string, int> reduced = builder
//                .Table(
//                    topic,
//                    Consumed.with(Serdes.String(), Serdes.Double()),
//                    Materialize.As < string, double, IKeyValueStore<Bytes, byte[]>("store")
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(Serdes.Double()))
//                .groupBy(intProjection)
//                .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR);

//            MockProcessorSupplier<string, int> supplier = getReducedResults(reduced);
//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//           assertReduced(supplier.theCapturedProcessor().lastValueAndTimestampPerKey, topic, driver);
//                Assert.Null(reduced.queryableStoreName);
//            }
//            catch (Exception e)
//            { }
//        }


//        [Fact]
//        public void shouldReduceAndMaterializeResults()
//        {
//            IKeyValueMapper<string, int, KeyValuePair<string, int>> intProjection =
//                (key, value) => KeyValuePair.Create(key, value.intValue());

//            IKTable<string, int> reduced = builder
//                .Table(
//                    topic,
//                    Consumed.with(Serdes.String(), Serdes.Double()))
//                .groupBy(intProjection)
//                .reduce(
//                    MockReducer.INTEGER_ADDER,
//                    MockReducer.INTEGER_SUBTRACTOR,
//                    Materialize.As<string, int, IKeyValueStore<Bytes, byte[]>>("reduce")
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(Serdes.Int()));

//            MockProcessorSupplier<string, int> supplier = getReducedResults(reduced);
//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//           assertReduced(supplier.theCapturedProcessor().lastValueAndTimestampPerKey, topic, driver);
//                {
//                    IKeyValueStore<string, int> reduce = driver.getKeyValueStore("reduce");
//                    Assert.Equal(reduce.get("A"), (5));
//                    Assert.Equal(reduce.get("B"), (6));
//                }
//                {
//                    IKeyValueStore<string, ValueAndTimestamp<int>> reduce = driver.getTimestampedKeyValueStore("reduce");
//                    Assert.Equal(reduce.get("A"), (ValueAndTimestamp.make(5, 50L)));
//                    Assert.Equal(reduce.get("B"), (ValueAndTimestamp.make(6, 30L)));
//                }
//            }
//    }


//        [Fact]
//        public void shouldCountAndMaterializeResults()
//        {
//            builder
//                .Table(
//                    topic,
//                    Consumed.with(Serdes.String(), Serdes.String()))
//                .groupBy(
//                    MockMapper.selectValueKeyValueMapper(),
//                    Grouped.with(Serdes.String(), Serdes.String()))
//                .count(
//                    Materialize.As < string, long, IKeyValueStore<Bytes, byte[]>("count")
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(Serdes.Long()));

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(topic, driver);
//                {
//                    IKeyValueStore<string, long> counts = driver.getKeyValueStore("count");
//                    Assert.Equal(counts.get("1"), (3L));
//                    Assert.Equal(counts.get("2"), (2L));
//                }
//                {
//                    IKeyValueStore<string, ValueAndTimestamp<long>> counts = driver.getTimestampedKeyValueStore("count");
//                    Assert.Equal(counts.get("1"), (ValueAndTimestamp.make(3L, 50L)));
//                    Assert.Equal(counts.get("2"), (ValueAndTimestamp.make(2L, 60L)));
//                }
//            }
//    }


//        [Fact]
//        public void shouldAggregateAndMaterializeResults()
//        {
//            builder
//                .Table(
//                    topic,
//                    Consumed.with(Serdes.String(), Serdes.String()))
//                .groupBy(
//                    MockMapper.selectValueKeyValueMapper(),
//                    Grouped.with(Serdes.String(), Serdes.String()))
//                .aggregate(
//                    MockInitializer.STRING_INIT,
//                    MockAggregator.TOSTRING_ADDER,
//                    MockAggregator.TOSTRING_REMOVER,
//                    Materialize.As < string, string, IKeyValueStore<Bytes, byte[]>("aggregate")
//                        .withValueSerde(Serdes.String())
//                        .withKeySerde(Serdes.String()));

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(topic, driver);
//                {
//                    {
//                        IKeyValueStore<string, string> aggregate = driver.getKeyValueStore("aggregate");
//                        Assert.Equal(aggregate.get("1"), ("0+1+1+1"));
//                        Assert.Equal(aggregate.get("2"), ("0+2+2"));
//                    }
//                    {
//                        IKeyValueStore<string, ValueAndTimestamp<string>> aggregate = driver.getTimestampedKeyValueStore("aggregate");
//                        Assert.Equal(aggregate.get("1"), (ValueAndTimestamp.make("0+1+1+1", 50L)));
//                        Assert.Equal(aggregate.get("2"), (ValueAndTimestamp.make("0+2+2", 60L)));
//                    }
//                }
//            }
//    }


//        [Fact]
//        public void shouldThrowNullPointOnCountWhenMaterializedIsNull()
//        {
//            groupedTable.count((Materialized)null);
//        }


//        [Fact]
//        public void shouldThrowNullPointerOnReduceWhenMaterializedIsNull()
//        {
//            groupedTable.reduce(
//                MockReducer.STRING_ADDER,
//                MockReducer.STRING_REMOVER,
//                (Materialized)null);
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnReduceWhenAdderIsNull()
//        {
//            groupedTable.reduce(
//                null,
//                MockReducer.STRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnReduceWhenSubtractorIsNull()
//        {
//            groupedTable.reduce(
//                MockReducer.STRING_ADDER,
//                null,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnAggregateWhenInitializerIsNull()
//        {
//            groupedTable.aggregate(
//                null,
//                MockAggregator.TOSTRING_ADDER,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnAggregateWhenAdderIsNull()
//        {
//            groupedTable.aggregate(
//                MockInitializer.STRING_INIT,
//                null,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnAggregateWhenSubtractorIsNull()
//        {
//            groupedTable.aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                null,
//                Materialized.As("store"));
//        }


//        [Fact]
//        public void shouldThrowNullPointerOnAggregateWhenMaterializedIsNull()
//        {
//            groupedTable.aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                MockAggregator.TOSTRING_REMOVER,
//                (Materialized)null);
//        }

//        private void processData(string topic,
//                                 var driver)
//        {
//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String());
//            driver.pipeInput(recordFactory.create(topic, "A", "1", 10L));
//            driver.pipeInput(recordFactory.create(topic, "B", "1", 50L));
//            driver.pipeInput(recordFactory.create(topic, "C", "1", 30L));
//            driver.pipeInput(recordFactory.create(topic, "D", "2", 40L));
//            driver.pipeInput(recordFactory.create(topic, "E", "2", 60L));
//        }
//    }
