//using Kafka.Streams.State;
//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State.KeyValues;
//using System;
//using System.Collections.Generic;
//using Xunit;
//using Kafka.Streams.Tests.Mocks;
//using Kafka.Streams.Tests.Helpers;

//namespace Kafka.Streams.Tests.Kstream.Internals
//{
//    public class KGroupedTableImplTest
//    {
//        private StreamsBuilder builder = new StreamsBuilder();
//        private static string INVALID_STORE_NAME = "~foo bar~";
//        private IKGroupedTable<string, string> groupedTable;
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.Int());
//        private string topic = "input";
//        public KGroupedTableImplTest()
//        {
//            groupedTable = builder
//                .Table("blah", Consumed.With(Serdes.String(), Serdes.String()))
//                .GroupBy(MockMapper.GetSelectValueKeyValueMapper());
//        }

//        [Fact]
//        public void shouldNotAllowInvalidStoreNameOnAggregate()
//        {
//            groupedTable.Aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As(INVALID_STORE_NAME));
//        }

//        [Fact]
//        public void shouldNotAllowNullInitializerOnAggregate()
//        {
//            groupedTable.Aggregate(
//                null,
//                MockAggregator.TOSTRING_ADDER,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotAllowNullAdderOnAggregate()
//        {
//            groupedTable.Aggregate(
//                MockInitializer.STRING_INIT,
//                null,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotAllowNullSubtractorOnAggregate()
//        {
//            groupedTable.Aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                null,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotAllowNullAdderOnReduce()
//        {
//            groupedTable.Reduce(
//                null,
//                MockReducer.STRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]//(expected = NullReferenceException))
//        public void shouldNotAllowNullSubtractorOnReduce()
//        {
//            groupedTable.Reduce(
//                MockReducer.STRING_ADDER,
//                null,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotAllowInvalidStoreNameOnReduce()
//        {
//            groupedTable.Reduce(
//                MockReducer.STRING_ADDER,
//                MockReducer.STRING_REMOVER,
//                Materialized.As(INVALID_STORE_NAME));
//        }

//        private MockProcessorSupplier<string, int> getReducedResults(IKTable<string, int> inputKTable)
//        {
//            MockProcessorSupplier<string, int> supplier = new MockProcessorSupplier<string, int>();
//            inputKTable
//                .ToStream()
//                .Process(supplier);
//            return supplier;
//        }

//        private void assertReduced(Dictionary<string, IValueAndTimestamp<int>> reducedResults,
//                                   string topic,
//                                   TopologyTestDriver driver)
//        {
//            ConsumerRecordFactory<string, double> recordFactory =
//                new ConsumerRecordFactory<string, double>(Serdes.String(), Serdes.Double());

//            driver.PipeInput(recordFactory.Create(topic, "A", 1.1, 10));
//            driver.PipeInput(recordFactory.Create(topic, "B", 2.2, 11));

//            Assert.Equal(ValueAndTimestamp.Make(1, 10L), reducedResults["A"]);
//            Assert.Equal(ValueAndTimestamp.Make(2, 11L), reducedResults["B"]);

//            driver.PipeInput(recordFactory.Create(topic, "A", 2.6, 30));
//            driver.PipeInput(recordFactory.Create(topic, "B", 1.3, 30));
//            driver.PipeInput(recordFactory.Create(topic, "A", 5.7, 50));
//            driver.PipeInput(recordFactory.Create(topic, "B", 6.2, 20));

//            Assert.Equal(ValueAndTimestamp.Make(5, 50L), reducedResults["A"]);
//            Assert.Equal(ValueAndTimestamp.Make(6, 30L), reducedResults["B"]);
//        }

//        [Fact]
//        public void shouldReduce()
//        {
//            KeyValueMapper<string, int, KeyValuePair<string, int>> intProjection =
//                (key, value) => KeyValuePair.Create(key, value);

//            IKTable<string, int> reduced = builder
//                .Table(
//                    topic,
//                    Consumed.With(Serdes.String(), Serdes.Double()),
//                    Materialized.As<string, double, IKeyValueStore<Bytes, byte[]>>("store")
//                        .WithKeySerde(Serdes.String())
//                        .WithValueSerde(Serdes.Double()))
//                .GroupBy(intProjection)
//                .Reduce(
//                    MockReducer.INTEGER_ADDER,
//                    MockReducer.INTEGER_SUBTRACTOR,
//                    Materialized.As("reduced"));

//            MockProcessorSupplier<string, int> supplier = getReducedResults(reduced);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            assertReduced(supplier.TheCapturedProcessor().LastValueAndTimestampPerKey, topic, driver);
//            Assert.Equal("reduced", reduced.QueryableStoreName);
//        }

//        [Fact]
//        public void shouldReduceWithInternalStoreName()
//        {
//            KeyValueMapper<string, int, KeyValuePair<string, int>> intProjection =
//                (key, value) => KeyValuePair.Create(key, value);

//            IKTable<string, int> reduced = builder
//                .Table(
//                    topic,
//                    Consumed.With(Serdes.String(), Serdes.Double()),
//                    Materialized.As<string, double, IKeyValueStore<Bytes, byte[]>>("store")
//                        .WithKeySerde(Serdes.String())
//                        .WithValueSerde(Serdes.Double()))
//                .GroupBy(intProjection)
//                .Reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR);

//            MockProcessorSupplier<string, int> supplier = getReducedResults(reduced);
//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                assertReduced(supplier.TheCapturedProcessor().LastValueAndTimestampPerKey, topic, driver);
//                Assert.Null(reduced.QueryableStoreName);
//            }
//            catch (Exception e)
//            { }
//        }


//        [Fact]
//        public void ShouldReduceAndMaterializeResults()
//        {
//            var intProjection = new KeyValueMapper<string, int, KeyValuePair<string, int>>((key, value) => KeyValuePair.Create(key, value));

//            IKTable<string, int> reduced = builder
//                .Table(
//                    topic,
//                    Consumed.With(Serdes.String(), Serdes.Int()))
//                .GroupBy(intProjection)
//                .Reduce(
//                    MockReducer.INTEGER_ADDER,
//                    MockReducer.INTEGER_SUBTRACTOR,
//                    Materialized.As<string, int, IKeyValueStore<Bytes, byte[]>>("reduce")
//                        .WithKeySerde(Serdes.String())
//                        .WithValueSerde(Serdes.Int()));

//            MockProcessorSupplier<string, int> supplier = getReducedResults(reduced);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            assertReduced(supplier.TheCapturedProcessor().LastValueAndTimestampPerKey, topic, driver);
//            {
//                IKeyValueStore<string, int> reduce = driver.GetKeyValueStore("reduce");
//                Assert.Equal(5, reduce.Get("A"));
//                Assert.Equal(6, reduce.Get("B"));
//            }
//            {
//                IKeyValueStore<string, IValueAndTimestamp<int>> reduce = driver.GetTimestampedKeyValueStore("reduce");
//                Assert.Equal(reduce.Get("A"), ValueAndTimestamp.Make(5, 50L));
//                Assert.Equal(reduce.Get("B"), ValueAndTimestamp.Make(6, 30L));
//            }
//        }


//        [Fact]
//        public void shouldCountAndMaterializeResults()
//        {
//            builder
//                .Table(
//                    topic,
//                    Consumed.With(Serdes.String(), Serdes.String()))
//                .GroupBy(
//                    MockMapper.GetSelectValueKeyValueMapper<string, string>(),
//                    Grouped.With(Serdes.String(), Serdes.String()))
//                .Count(
//                    Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("count")
//                        .WithKeySerde(Serdes.String())
//                        .WithValueSerde(Serdes.Long()));

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(topic, driver);
//            {
//                IKeyValueStore<string, long> counts = driver.GetKeyValueStore("count");
//                Assert.Equal(3L, counts.Get("1"));
//                Assert.Equal(2L, counts.Get("2"));
//            }
//            {
//                IKeyValueStore<string, IValueAndTimestamp<long>> counts = driver.GetTimestampedKeyValueStore("count");
//                Assert.Equal(counts.Get("1"), ValueAndTimestamp.Make(3L, 50L));
//                Assert.Equal(counts.Get("2"), ValueAndTimestamp.Make(2L, 60L));
//            }
//        }

//        [Fact]
//        public void shouldAggregateAndMaterializeResults()
//        {
//            builder
//                .Table(
//                    topic,
//                    Consumed.With(Serdes.String(), Serdes.String()))
//                .GroupBy(
//                    MockMapper.GetSelectValueKeyValueMapper<string, string>(),
//                    Grouped.With(Serdes.String(), Serdes.String()))
//                .Aggregate(
//                    MockInitializer.STRING_INIT,
//                    MockAggregator.TOSTRING_ADDER(),
//                    MockAggregator.TOSTRING_REMOVER(),
//                    Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("aggregate")
//                        .WithValueSerde(Serdes.String())
//                        .WithKeySerde(Serdes.String()));

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(topic, driver);
//            {
//                {
//                    IKeyValueStore<string, string> aggregate = driver.GetKeyValueStore("aggregate");
//                    Assert.Equal("0+1+1+1", aggregate.Get("1"));
//                    Assert.Equal("0+2+2", aggregate.Get("2"));
//                }
//                {
//                    IKeyValueStore<string, IValueAndTimestamp<string>> aggregate = driver.getTimestampedKeyValueStore("aggregate");
//                    Assert.Equal(aggregate.Get("1"), ValueAndTimestamp.Make("0+1+1+1", 50L));
//                    Assert.Equal(aggregate.Get("2"), ValueAndTimestamp.Make("0+2+2", 60L));
//                }
//            }
//        }


//        [Fact]
//        public void shouldThrowNullPointOnCountWhenMaterializedIsNull()
//        {
//            groupedTable.Count((Materialized)null);
//        }


//        [Fact]
//        public void shouldThrowNullPointerOnReduceWhenMaterializedIsNull()
//        {
//            groupedTable.Reduce(
//                MockReducer.STRING_ADDER,
//                MockReducer.STRING_REMOVER,
//                (Materialized)null);
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnReduceWhenAdderIsNull()
//        {
//            groupedTable.Reduce(
//                null,
//                MockReducer.STRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnReduceWhenSubtractorIsNull()
//        {
//            groupedTable.Reduce(
//                MockReducer.STRING_ADDER,
//                null,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnAggregateWhenInitializerIsNull()
//        {
//            groupedTable.Aggregate(
//                null,
//                MockAggregator.TOSTRING_ADDER(),
//                MockAggregator.TOSTRING_REMOVER(),
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnAggregateWhenAdderIsNull()
//        {
//            groupedTable.Aggregate(
//                MockInitializer.STRING_INIT,
//                null,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldThrowNullPointerOnAggregateWhenSubtractorIsNull()
//        {
//            groupedTable.Aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                null,
//                Materialized.As("store"));
//        }


//        [Fact]
//        public void shouldThrowNullPointerOnAggregateWhenMaterializedIsNull()
//        {
//            groupedTable.Aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                MockAggregator.TOSTRING_REMOVER,
//                (Materialized)null);
//        }

//        private void processData(string topic, TopologyTestDriver driver)
//        {
//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<string, string>(Serdes.String().Serializer, Serdes.String().Serializer);

//            driver.PipeInput(recordFactory.Create(topic, "A", "1", 10L));
//            driver.PipeInput(recordFactory.Create(topic, "B", "1", 50L));
//            driver.PipeInput(recordFactory.Create(topic, "C", "1", 30L));
//            driver.PipeInput(recordFactory.Create(topic, "D", "2", 40L));
//            driver.PipeInput(recordFactory.Create(topic, "E", "2", 60L));
//        }
//    }
//}
