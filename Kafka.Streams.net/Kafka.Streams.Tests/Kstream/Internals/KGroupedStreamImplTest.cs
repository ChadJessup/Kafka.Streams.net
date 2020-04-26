using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.Windowed;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Integration;
using Kafka.Streams.Tests.Mocks;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KGroupedStreamImplTest
    {
        private static string TOPIC = "topic";
        private static string INVALID_STORE_NAME = "~foo bar~";
        private StreamsBuilder builder = new StreamsBuilder();
        private IKGroupedStream<string, string> groupedStream;

        private ConsumerRecordFactory<string, string> recordFactory =
            new ConsumerRecordFactory<string, string>(Serdes.String().Serializer, Serdes.String().Serializer);
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

        public KGroupedStreamImplTest()
        {
            IKStream<string, string> stream = builder.Stream(TOPIC, Consumed.With(Serdes.String(), Serdes.String()));
            groupedStream = stream.GroupByKey(Grouped.With(Serdes.String(), Serdes.String()));
        }

        [Fact]
        public void shouldNotHaveNullReducerOnReduce()
        {
            groupedStream.Reduce((IReducer<string>)null);
        }

        [Fact]
        public void ShouldNotHaveInvalidStoreNameOnReduce()
        {
            groupedStream.Reduce(MockReducer.STRING_ADDER, Materialized.As(INVALID_STORE_NAME));
        }

        [Fact]
        public void shouldNotHaveNullReducerWithWindowedReduce()
        {
            groupedStream
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)))
                .Reduce(null, Materialized.As<string, string>("store"));
        }

        [Fact]
        public void shouldNotHaveNullWindowsWithWindowedReduce()
        {
            groupedStream.WindowedBy((Windows)null);
        }

        [Fact]
        public void shouldNotHaveInvalidStoreNameWithWindowedReduce()
        {
            groupedStream
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)))
                .Reduce(MockReducer.STRING_ADDER, Materialized.As(INVALID_STORE_NAME));
        }

        [Fact]
        public void shouldNotHaveNullInitializerOnAggregate()
        {
            groupedStream.Aggregate((Initializer<string>)null, MockAggregator<string, string>.TOSTRING_ADDER, Materialized.As<string, string>("store"));
        }

        [Fact]
        public void shouldNotHaveNullAdderOnAggregate()
        {
            groupedStream.Aggregate(MockInitializer.STRING_INIT, null, Materialized.As("store"));
        }

        [Fact]
        public void shouldNotHaveInvalidStoreNameOnAggregate()
        {
            groupedStream.Aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.As(INVALID_STORE_NAME));
        }

        [Fact]
        public void ShouldNotHaveNullInitializerOnWindowedAggregate()
        {
            groupedStream
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)))
                .Aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.As("store"));
        }

        [Fact]
        public void shouldNotHaveNullAdderOnWindowedAggregate()
        {
            groupedStream
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)))
                .Aggregate(MockInitializer.STRING_INIT, null, Materialized.As("store"));
        }

        [Fact]
        public void shouldNotHaveNullWindowsOnWindowedAggregate()
        {
            groupedStream.WindowedBy((Windows<Window>)null);
        }

        [Fact]
        public void shouldNotHaveInvalidStoreNameOnWindowedAggregate()
        {
            groupedStream
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)))
                .Aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.As(INVALID_STORE_NAME));
        }

        private void doAggregateSessionWindows(MockProcessorSupplier<IWindowed<string>, int> supplier)
        {
            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 10));
            driver.PipeInput(recordFactory.Create(TOPIC, "2", "2", 15));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 30));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 70));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 100));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 90));

            Dictionary<IWindowed<string>, IValueAndTimestamp<int>> result
                = supplier.TheCapturedProcessor().LastValueAndTimestampPerKey;
            Assert.Equal(
                 ValueAndTimestamp.Make(2, 30L),
                 result[new Windowed<string>("1", new SessionWindow(10L, 30L))]);
            Assert.Equal(
                 ValueAndTimestamp.Make(1, 15L),
                 result[new Windowed<string>("2", new SessionWindow(15L, 15L))]);
            Assert.Equal(
                 ValueAndTimestamp.Make(3, 100L),
                 result[new Windowed<string>("1", new SessionWindow(70L, 100L))]);
        }

        [Fact]
        public void ShouldAggregateSessionWindows()
        {
            MockProcessorSupplier<IWindowed<string>, int> supplier = new MockProcessorSupplier<IWindowed<string>, int>();
            IKTable<IWindowed<string>, int> table = groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Aggregate(
                    () => 0,
                    (aggKey, value, aggregate) => aggregate + 1,
                    (aggKey, aggOne, aggTwo) => aggOne + aggTwo,
                    Materialized.As<string, int, ISessionStore<Bytes, byte[]>>("session-store")
                    .WithValueSerde(Serdes.Int()));

            table.ToStream().Process(supplier);

            doAggregateSessionWindows(supplier);
            Assert.Equal("session-store", table.QueryableStoreName);
        }

        [Fact]
        public void ShouldAggregateSessionWindowsWithInternalStoreName()
        {
            MockProcessorSupplier<IWindowed<string>, int> supplier = new MockProcessorSupplier<IWindowed<string>, int>();
            IKTable<IWindowed<string>, int> table = groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Aggregate(
                    () => 0,
                    new Aggregator<string, string, int>((aggKey, value, aggregate) => aggregate + 1),
                    (aggKey, aggOne, aggTwo) => aggOne + aggTwo,
                    Materialized.With<string, int, ISessionStore<Bytes, byte[]>>(null, Serdes.Int()));

            table.ToStream().Process(supplier);

            doAggregateSessionWindows(supplier);
        }

        private void doCountSessionWindows(MockProcessorSupplier<IWindowed<string>, long> supplier)
        {
            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 10));
            driver.PipeInput(recordFactory.Create(TOPIC, "2", "2", 15));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 30));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 70));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 100));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 90));

            Dictionary<IWindowed<string>, IValueAndTimestamp<long>> result =
                supplier.TheCapturedProcessor().LastValueAndTimestampPerKey;
            Assert.Equal(
                 ValueAndTimestamp.Make(2L, 30L),
                 result[new Windowed<string>("1", new SessionWindow(10L, 30L))]);
            Assert.Equal(
                 ValueAndTimestamp.Make(1L, 15L),
                 result[new Windowed<string>("2", new SessionWindow(15L, 15L))]);
            Assert.Equal(
                 ValueAndTimestamp.Make(3L, 100L),
                 result[new Windowed<string>("1", new SessionWindow(70L, 100L))]);
        }

        [Fact]
        public void shouldCountSessionWindows()
        {
            MockProcessorSupplier<IWindowed<string>, long> supplier = new MockProcessorSupplier<IWindowed<string>, long>();
            IKTable<IWindowed<string>, long> table = groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Count(Materialized.As("session-store"));
            table.ToStream().Process(supplier);
            doCountSessionWindows(supplier);
            Assert.Equal("session-store", table.QueryableStoreName);
        }

        [Fact]
        public void shouldCountSessionWindowsWithInternalStoreName()
        {
            MockProcessorSupplier<IWindowed<string>, long> supplier = new MockProcessorSupplier<IWindowed<string>, long>();
            IKTable<IWindowed<string>, long> table = groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Count();
            table.ToStream().Process(supplier);
            doCountSessionWindows(supplier);
            Assert.Null(table.QueryableStoreName);
        }

        private void DoReduceSessionWindows(MockProcessorSupplier<IWindowed<string>, string> supplier)
        {
            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "A", 10));
            driver.PipeInput(recordFactory.Create(TOPIC, "2", "Z", 15));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "B", 30));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "A", 70));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "B", 100));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "C", 90));

            Dictionary<IWindowed<string>, IValueAndTimestamp<string>> result =
                supplier.TheCapturedProcessor().LastValueAndTimestampPerKey;
            Assert.Equal(
                 ValueAndTimestamp.Make("A:B", 30L),
                 result[new Windowed<string>("1", new SessionWindow(10L, 30L))]);
            Assert.Equal(
                 ValueAndTimestamp.Make("Z", 15L),
                 result[new Windowed<string>("2", new SessionWindow(15L, 15L))]);
            Assert.Equal(
                 ValueAndTimestamp.Make("A:B:C", 100L),
                 result[new Windowed<string>("1", new SessionWindow(70L, 100L))]);
        }

        [Fact]
        public void shouldReduceSessionWindows()
        {
            var supplier = new MockProcessorSupplier<IWindowed<string>, string>();
            IKTable<IWindowed<string>, string> table = groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Reduce(new Reducer<string>((value1, value2) => value1 + ":" + value2), Materialized.As<string, string, ISessionStore<Bytes, byte[]>>("session-store"));
            table.ToStream().Process(supplier);
            DoReduceSessionWindows(supplier);

            Assert.Equal("session-store", table.QueryableStoreName);
        }

        [Fact]
        public void shouldReduceSessionWindowsWithInternalStoreName()
        {
            var supplier = new MockProcessorSupplier<IWindowed<string>, string>();
            IKTable<IWindowed<string>, string> table = groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Reduce((value1, value2) => value1 + ":" + value2);
            table.ToStream().Process(supplier);
            DoReduceSessionWindows(supplier);
            Assert.Null(table.QueryableStoreName);
        }

        [Fact]
        public void shouldNotAcceptNullReducerWhenReducingSessionWindows()
        {
            groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Reduce(null, Materialized.As("store"));
        }

        [Fact]
        public void shouldNotAcceptNullSessionWindowsReducingSessionWindows()
        {
            groupedStream.WindowedBy(null);
        }

        [Fact]
        public void shouldNotAcceptInvalidStoreNameWhenReducingSessionWindows()
        {
            groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Reduce(MockReducer.STRING_ADDER, Materialized.As(INVALID_STORE_NAME));
        }

        [Fact]
        public void shouldNotAcceptNullStateStoreSupplierWhenReducingSessionWindows()
        {
            groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Reduce(
                    null,
                    Materialized.As<string, string, ISessionStore<Bytes, byte[]>>(null));
        }

        [Fact]
        public void shouldNotAcceptNullInitializerWhenAggregatingSessionWindows()
        {
            groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Aggregate(
                    null,
                    MockAggregator.TOSTRING_ADDER,
                    (aggKey, aggOne, aggTwo) => null,
                    Materialized.As("storeName"));
        }

        [Fact]
        public void shouldNotAcceptNullAggregatorWhenAggregatingSessionWindows()
        {
            groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Aggregate(
                    MockInitializer.STRING_INIT,
                    null,
                    (aggKey, aggOne, aggTwo) => null,
                    Materialized.As("storeName"));
        }

        [Fact]
        public void shouldNotAcceptNullSessionMergerWhenAggregatingSessionWindows()
        {
            groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(30)))
                .Aggregate(
                    MockInitializer.STRING_INIT,
                    MockAggregator.TOSTRING_ADDER,
                    null,
                    Materialized.As("storeName"));
        }

        [Fact]
        public void shouldNotAcceptNullSessionWindowsWhenAggregatingSessionWindows()
        {
            groupedStream.WindowedBy(null);
        }

        [Fact]
        public void shouldAcceptNullStoreNameWhenAggregatingSessionWindows()
        {
            groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(10)))
                .Aggregate(
                    MockInitializer.STRING_INIT,
                    MockAggregator.TOSTRING_ADDER,
                    (aggKey, aggOne, aggTwo) => null,
                    Materialized.With(Serdes.String(), Serdes.String()));
        }

        [Fact]
        public void shouldNotAcceptInvalidStoreNameWhenAggregatingSessionWindows()
        {
            groupedStream
                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(10)))
                .Aggregate(
                    MockInitializer.STRING_INIT,
                    MockAggregator.TOSTRING_ADDER,
                    (aggKey, aggOne, aggTwo) => null,
                    Materialized.As(INVALID_STORE_NAME));
        }


        [Fact]
        public void shouldThrowNullPointerOnReduceWhenMaterializedIsNull()
        {
            groupedStream.Reduce(MockReducer.STRING_ADDER, (Materialized)null);
        }


        [Fact]
        public void shouldThrowNullPointerOnAggregateWhenMaterializedIsNull()
        {
            groupedStream.Aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, (Materialized)null);
        }


        [Fact]
        public void shouldThrowNullPointerOnCountWhenMaterializedIsNull()
        {
            groupedStream.Count((Materialized)null);
        }

        [Fact]
        public void shouldCountAndMaterializeResults()
        {
            groupedStream.Count(Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("count").WithKeySerde(Serdes.String()));

            var driver = new TopologyTestDriver(builder.Build(), props);
            ProcessData(driver);

            IKeyValueStore<string, long> count = driver.GetKeyValueStore<string, long>("count");

            Assert.Equal(3L, count.Get("1"));
            Assert.Equal(1L, count.Get("2"));
            Assert.Equal(2L, count.Get("3"));

            IKeyValueStore<string, IValueAndTimestamp<long>> count = driver.GetTimestampedKeyValueStore<string, long>("count");

            Assert.Equal(count.Get("1"), ValueAndTimestamp.Make(3L, 10L).Value);
            Assert.Equal(count.Get("2"), ValueAndTimestamp.Make(1L, 1L).Value);
            Assert.Equal(count.Get("3"), ValueAndTimestamp.Make(2L, 9L).Value);
        }

        //    [Fact]
        //    public void shouldLogAndMeasureSkipsInAggregate()
        //    {
        //        groupedStream.Count(Materialized.As < string, long, IKeyValueStore<Bytes, byte[]>>("count").WithKeySerde(Serdes.String()));
        //        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        //
        //        var driver = new TopologyTestDriver(builder.Build(), props);
        //        ProcessData(driver);
        //        LogCaptureAppender.unregister(appender);
        //
        //        var metrics = driver.metrics();
        //        Assert.Equal(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
        //        Assert.NotEqual(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
        //        Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] offset=[6]"));
        //    }

        [Fact]
        public void shouldReduceAndMaterializeResults()
        {
            groupedStream.Reduce(
                MockReducer.STRING_ADDER,
                Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("reduce")
                    .WithKeySerde(Serdes.String())
                    .WithValueSerde(Serdes.String()));

            var driver = new TopologyTestDriver(builder.Build(), props);
            ProcessData(driver);

            IKeyValueStore<string, string> reduced = driver.GetKeyValueStore<string, string>("reduce");

            Assert.Equal("A+C+D", reduced.Get("1"));
            Assert.Equal("B", reduced.Get("2"));
            Assert.Equal("E+F", reduced.Get("3"));
            var reducedKVS = driver.GetTimestampedKeyValueStore<string, string>("reduce");

            Assert.Equal(reduced.Get("1"), ValueAndTimestamp.Make("A+C+D", 10L).Value);
            Assert.Equal(reduced.Get("2"), ValueAndTimestamp.Make("B", 1L).Value);
            Assert.Equal(reduced.Get("3"), ValueAndTimestamp.Make("E+F", 9L).Value);

        }

        //    [Fact]
        //    public void shouldLogAndMeasureSkipsInReduce()
        //    {
        //        groupedStream.Reduce(
        //            MockReducer.STRING_ADDER,
        //            Materialized.As < string, string, IKeyValueStore<Bytes, byte[]>>("reduce")
        //                .WithKeySerde(Serdes.String())
        //                .WithValueSerde(Serdes.String())
        //        );
        //
        //        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        //        var driver = new TopologyTestDriver(builder.Build(), props);
        //        ProcessData(driver);
        //        LogCaptureAppender.unregister(appender);
        //
        //        Dictionary metrics = driver.metrics< MetricName, ? : Metric >();
        //        Assert.Equal(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
        //        Assert.NotEqual(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
        //        Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] offset=[6]"));
        //    }

        [Fact]
        public void shouldAggregateAndMaterializeResults()
        {
            groupedStream.Aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator<string, string>.TOSTRING_ADDER,
                Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("aggregate")
                    .WithKeySerde(Serdes.String())
                    .WithValueSerde(Serdes.String()));

            var driver = new TopologyTestDriver(builder.Build(), props);
            ProcessData(driver);

            {
                IKeyValueStore<string, string> aggregate = driver.GetKeyValueStore<string, string>("aggregate");

                Assert.Equal("0+A+C+D", aggregate.Get("1"));
                Assert.Equal("0+B", aggregate.Get("2"));
                Assert.Equal("0+E+F", aggregate.Get("3"));
            }
            {
                IKeyValueStore<string, IValueAndTimestamp<string>> aggregate = driver.GetTimestampedKeyValueStore<string, string>("aggregate");

                Assert.Equal(aggregate.Get("1"), ValueAndTimestamp.Make("0+A+C+D", 10L));
                Assert.Equal(aggregate.Get("2"), ValueAndTimestamp.Make("0+B", 1L));
                Assert.Equal(aggregate.Get("3"), ValueAndTimestamp.Make("0+E+F", 9L));
            }
        }


        [Fact]
        public void ShouldAggregateWithDefaultSerdes()
        {
            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<string, string>();
            groupedStream
                .Aggregate(MockInitializer.STRING_INIT, MockAggregator<string, string>.TOSTRING_ADDER)
                .ToStream()
                .Process(supplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            ProcessData(driver);

            Assert.Equal(
                supplier.TheCapturedProcessor().LastValueAndTimestampPerKey["1"],
                ValueAndTimestamp.Make("0+A+C+D", 10L));
            Assert.Equal(
                supplier.TheCapturedProcessor().LastValueAndTimestampPerKey["2"],
                ValueAndTimestamp.Make("0+B", 1L));
            Assert.Equal(
                supplier.TheCapturedProcessor().LastValueAndTimestampPerKey["3"],
                ValueAndTimestamp.Make("0+E+F", 9L));
        }

        private void ProcessData(TopologyTestDriver driver)
        {
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "A", 5L));
            driver.PipeInput(recordFactory.Create(TOPIC, "2", "B", 1L));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "C", 3L));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "D", 10L));
            driver.PipeInput(recordFactory.Create(TOPIC, "3", "E", 8L));
            driver.PipeInput(recordFactory.Create(TOPIC, "3", "F", 9L));
            driver.PipeInput(recordFactory.Create(TOPIC, "3", (string)null));
        }

        private void DoCountWindowed(MockProcessorSupplier<IWindowed<string>, long> supplier)
        {
            var driver = new TopologyTestDriver(builder.Build(), props);
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "A", 0L));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "A", 499L));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "A", 100L));
            driver.PipeInput(recordFactory.Create(TOPIC, "2", "B", 0L));
            driver.PipeInput(recordFactory.Create(TOPIC, "2", "B", 100L));
            driver.PipeInput(recordFactory.Create(TOPIC, "2", "B", 200L));
            driver.PipeInput(recordFactory.Create(TOPIC, "3", "C", 1L));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "A", 500L));
            driver.PipeInput(recordFactory.Create(TOPIC, "1", "A", 500L));
            driver.PipeInput(recordFactory.Create(TOPIC, "2", "B", 500L));
            driver.PipeInput(recordFactory.Create(TOPIC, "2", "B", 500L));
            driver.PipeInput(recordFactory.Create(TOPIC, "3", "B", 100L));

            Assert.Equal(
                supplier.TheCapturedProcessor().processed,
                new List<KeyValueTimestamp<IWindowed<string>, long>>
                {
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("1", new TimeWindow(0L, 500L)), 1L, 0L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("1", new TimeWindow(0L, 500L)), 2L, 499L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("1", new TimeWindow(0L, 500L)), 3L, 499L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("2", new TimeWindow(0L, 500L)), 1L, 0L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("2", new TimeWindow(0L, 500L)), 2L, 100L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("2", new TimeWindow(0L, 500L)), 3L, 200L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("3", new TimeWindow(0L, 500L)), 1L, 1L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("1", new TimeWindow(500L, 1000L)), 1L, 500L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("1", new TimeWindow(500L, 1000L)), 2L, 500L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("2", new TimeWindow(500L, 1000L)), 1L, 500L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("2", new TimeWindow(500L, 1000L)), 2L, 500L),
                    new KeyValueTimestamp<IWindowed<string>, long>(new Windowed<string>("3", new TimeWindow(0L, 500L)), 2L, 100L)
            });
        }

        [Fact]
        public void ShouldCountWindowed()
        {
            MockProcessorSupplier<IWindowed<string>, long> supplier = new MockProcessorSupplier<IWindowed<string>, long>();
            groupedStream
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(500L)))
                .Count(Materialized.As<string, long, IWindowStore<Bytes, byte[]>>("aggregate-by-key-windowed"))
                .ToStream()
                .Process(supplier);

            DoCountWindowed(supplier);
        }

        [Fact]
        public void ShouldCountWindowedWithInternalStoreName()
        {
            MockProcessorSupplier<IWindowed<string>, long> supplier = new MockProcessorSupplier<IWindowed<string>, long>();
            List<KeyValuePair<IWindowed<string>, KeyValuePair<long, long>>> results = new List<KeyValuePair<IWindowed<string>, KeyValuePair<long, long>>>();
            groupedStream
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(500L)))
                .Count()
                .ToStream()
                .Process(supplier);

            DoCountWindowed(supplier);
        }
    }
}
