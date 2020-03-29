//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Sessions;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KGroupedStreamImplTest
//    {
//        private static string TOPIC = "topic";
//        private static string INVALID_STORE_NAME = "~foo bar~";
//        private StreamsBuilder builder = new StreamsBuilder();
//        private KGroupedStream<string, string> groupedStream;

//        private ConsumerRecordFactory<string, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.String(), Serdes.String());
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());


//        public void before()
//        {
//            IKStream<string, string> stream = builder.Stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
//            groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
//        }

//        [Fact]
//        public void shouldNotHaveNullReducerOnReduce()
//        {
//            groupedStream.reduce(null);
//        }

//        [Fact]
//        public void shouldNotHaveInvalidStoreNameOnReduce()
//        {
//            groupedStream.reduce(MockReducer.STRING_ADDER, Materialized.As(INVALID_STORE_NAME));
//        }

//        [Fact]
//        public void shouldNotHaveNullReducerWithWindowedReduce()
//        {
//            groupedStream
//                .windowedBy(TimeWindows.of(Duration.FromMilliseconds(10)))
//                .reduce(null, Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotHaveNullWindowsWithWindowedReduce()
//        {
//            groupedStream.windowedBy((Windows)null);
//        }

//        [Fact]
//        public void shouldNotHaveInvalidStoreNameWithWindowedReduce()
//        {
//            groupedStream
//                .windowedBy(TimeWindows.of(Duration.FromMilliseconds(10)))
//                .reduce(MockReducer.STRING_ADDER, Materialized.As(INVALID_STORE_NAME));
//        }

//        [Fact]
//        public void shouldNotHaveNullInitializerOnAggregate()
//        {
//            groupedStream.aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotHaveNullAdderOnAggregate()
//        {
//            groupedStream.aggregate(MockInitializer.STRING_INIT, null, Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotHaveInvalidStoreNameOnAggregate()
//        {
//            groupedStream.aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                Materialized.As(INVALID_STORE_NAME));
//        }

//        [Fact]
//        public void shouldNotHaveNullInitializerOnWindowedAggregate()
//        {
//            groupedStream
//                .windowedBy(TimeWindows.of(Duration.FromMilliseconds(10)))
//                .aggregate(null, MockAggregator.TOSTRING_ADDER, Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotHaveNullAdderOnWindowedAggregate()
//        {
//            groupedStream
//                .windowedBy(TimeWindows.of(Duration.FromMilliseconds(10)))
//                .aggregate(MockInitializer.STRING_INIT, null, Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotHaveNullWindowsOnWindowedAggregate()
//        {
//            groupedStream.windowedBy((Windows)null);
//        }

//        [Fact]
//        public void shouldNotHaveInvalidStoreNameOnWindowedAggregate()
//        {
//            groupedStream
//                .windowedBy(TimeWindows.of(Duration.FromMilliseconds(10)))
//                .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, Materialized.As(INVALID_STORE_NAME));
//        }

//        private void doAggregateSessionWindows(MockProcessorSupplier<Windowed<string>, int> supplier)
//        {
//            var driver = new TopologyTestDriver(builder.Build(), props);
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 10));
//            driver.pipeInput(recordFactory.create(TOPIC, "2", "2", 15));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 30));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 70));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 100));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 90));

//            Dictionary<Windowed<string>, ValueAndTimestamp<int>> result
//                = supplier.theCapturedProcessor().lastValueAndTimestampPerKey;
//            Assert.Equal(
//                 ValueAndTimestamp.make(2, 30L),
//                 result.get(new Windowed<>("1", new SessionWindow(10L, 30L))));
//            Assert.Equal(
//                 ValueAndTimestamp.make(1, 15L),
//                 result.get(new Windowed<>("2", new SessionWindow(15L, 15L))));
//            Assert.Equal(
//                 ValueAndTimestamp.make(3, 100L),
//                 result.get(new Windowed<>("1", new SessionWindow(70L, 100L))));
//        }

//        [Fact]
//        public void shouldAggregateSessionWindows()
//        {
//            MockProcessorSupplier<Windowed<string>, int> supplier = new MockProcessorSupplier<>();
//            IKTable<Windowed<string>, int> table = groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .aggregate(
//                    () => 0,
//                    (aggKey, value, aggregate) => aggregate + 1,
//                    (aggKey, aggOne, aggTwo) => aggOne + aggTwo,
//                    Materialized.As < string, int, ISessionStore<Bytes, byte[]>("session-store").
//                        withValueSerde(Serdes.Int()));
//            table.toStream().process(supplier);

//            doAggregateSessionWindows(supplier);
//            Assert.Equal(table.queryableStoreName, "session-store");
//        }

//        [Fact]
//        public void shouldAggregateSessionWindowsWithInternalStoreName()
//        {
//            MockProcessorSupplier<Windowed<string>, int> supplier = new MockProcessorSupplier<>();
//            IKTable<Windowed<string>, int> table = groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .aggregate(
//                    () => 0,
//                    (aggKey, value, aggregate) => aggregate + 1,
//                    (aggKey, aggOne, aggTwo) => aggOne + aggTwo,
//                    Materialized.with(null, Serdes.Int()));
//            table.toStream().process(supplier);

//            doAggregateSessionWindows(supplier);
//        }

//        private void doCountSessionWindows(MockProcessorSupplier<Windowed<string>, long> supplier)
//        {
//            var driver = new TopologyTestDriver(builder.Build(), props);
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 10));
//            driver.pipeInput(recordFactory.create(TOPIC, "2", "2", 15));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 30));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 70));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 100));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 90));

//            Dictionary<Windowed<string>, ValueAndTimestamp<long>> result =
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey;
//            Assert.Equal(
//                 ValueAndTimestamp.make(2L, 30L),
//                 result.get(new Windowed<>("1", new SessionWindow(10L, 30L))));
//            Assert.Equal(
//                 ValueAndTimestamp.make(1L, 15L),
//                 result.get(new Windowed<>("2", new SessionWindow(15L, 15L))));
//            Assert.Equal(
//                 ValueAndTimestamp.make(3L, 100L),
//                 result.get(new Windowed<>("1", new SessionWindow(70L, 100L))));
//        }

//        [Fact]
//        public void shouldCountSessionWindows()
//        {
//            MockProcessorSupplier<Windowed<string>, long> supplier = new MockProcessorSupplier<>();
//            IKTable<Windowed<string>, long> table = groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .count(Materialized.As("session-store"));
//            table.toStream().process(supplier);
//            doCountSessionWindows(supplier);
//            Assert.Equal(table.queryableStoreName, "session-store");
//        }

//        [Fact]
//        public void shouldCountSessionWindowsWithInternalStoreName()
//        {
//            MockProcessorSupplier<Windowed<string>, long> supplier = new MockProcessorSupplier<>();
//            IKTable<Windowed<string>, long> table = groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .count();
//            table.toStream().process(supplier);
//            doCountSessionWindows(supplier);
//            Assert.Null(table.queryableStoreName);
//        }

//        private void doReduceSessionWindows(MockProcessorSupplier<Windowed<string>, string> supplier)
//        {
//            var driver = new TopologyTestDriver(builder.Build(), props);
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 10));
//            driver.pipeInput(recordFactory.create(TOPIC, "2", "Z", 15));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "B", 30));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 70));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "B", 100));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "C", 90));

//            Dictionary<Windowed<string>, ValueAndTimestamp<string>> result =
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey;
//            Assert.Equal(
//                 ValueAndTimestamp.make("A:B", 30L),
//                 result.get(new Windowed<>("1", new SessionWindow(10L, 30L))));
//            Assert.Equal(
//                 ValueAndTimestamp.make("Z", 15L),
//                 result.get(new Windowed<>("2", new SessionWindow(15L, 15L))));
//            Assert.Equal(
//                 ValueAndTimestamp.make("A:B:C", 100L),
//                 result.get(new Windowed<>("1", new SessionWindow(70L, 100L))));
//        }

//        [Fact]
//        public void shouldReduceSessionWindows()
//        {
//            MockProcessorSupplier<Windowed<string>, string> supplier = new MockProcessorSupplier<>();
//            IKTable<Windowed<string>, string> table = groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .reduce((value1, value2) => value1 + ":" + value2, Materialized.As("session-store"));
//            table.toStream().process(supplier);
//            doReduceSessionWindows(supplier);
//            Assert.Equal(table.queryableStoreName, "session-store");
//        }

//        [Fact]
//        public void shouldReduceSessionWindowsWithInternalStoreName()
//        {
//            MockProcessorSupplier<Windowed<string>, string> supplier = new MockProcessorSupplier<>();
//            IKTable<Windowed<string>, string> table = groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .reduce((value1, value2) => value1 + ":" + value2);
//            table.toStream().process(supplier);
//            doReduceSessionWindows(supplier);
//            Assert.Null(table.queryableStoreName);
//        }

//        [Fact]
//        public void shouldNotAcceptNullReducerWhenReducingSessionWindows()
//        {
//            groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .reduce(null, Materialized.As("store"));
//        }

//        [Fact]
//        public void shouldNotAcceptNullSessionWindowsReducingSessionWindows()
//        {
//            groupedStream.windowedBy((SessionWindows)null);
//        }

//        [Fact]
//        public void shouldNotAcceptInvalidStoreNameWhenReducingSessionWindows()
//        {
//            groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .reduce(MockReducer.STRING_ADDER, Materialized.As(INVALID_STORE_NAME));
//        }

//        [Fact]
//        public void shouldNotAcceptNullStateStoreSupplierWhenReducingSessionWindows()
//        {
//            groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .reduce(
//                    null,
//                    Materialize.As < string, string, ISessionStore<Bytes, byte[]>(null));
//        }

//        [Fact]
//        public void shouldNotAcceptNullInitializerWhenAggregatingSessionWindows()
//        {
//            groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .aggregate(
//                    null,
//                    MockAggregator.TOSTRING_ADDER,
//                    (aggKey, aggOne, aggTwo) => null,
//                    Materialized.As("storeName"));
//        }

//        [Fact]
//        public void shouldNotAcceptNullAggregatorWhenAggregatingSessionWindows()
//        {
//            groupedStream.
//                windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .aggregate(
//                    MockInitializer.STRING_INIT,
//                    null,
//                    (aggKey, aggOne, aggTwo) => null,
//                    Materialized.As("storeName"));
//        }

//        [Fact]
//        public void shouldNotAcceptNullSessionMergerWhenAggregatingSessionWindows()
//        {
//            groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(30)))
//                .aggregate(
//                    MockInitializer.STRING_INIT,
//                    MockAggregator.TOSTRING_ADDER,
//                    null,
//                    Materialized.As("storeName"));
//        }

//        [Fact]
//        public void shouldNotAcceptNullSessionWindowsWhenAggregatingSessionWindows()
//        {
//            groupedStream.windowedBy((SessionWindows)null);
//        }

//        [Fact]
//        public void shouldAcceptNullStoreNameWhenAggregatingSessionWindows()
//        {
//            groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(10)))
//                .aggregate(
//                    MockInitializer.STRING_INIT,
//                    MockAggregator.TOSTRING_ADDER,
//                    (aggKey, aggOne, aggTwo) => null,
//                    Materialized.with(Serdes.String(), Serdes.String()));
//        }

//        [Fact]
//        public void shouldNotAcceptInvalidStoreNameWhenAggregatingSessionWindows()
//        {
//            groupedStream
//                .windowedBy(SessionWindows.with(Duration.FromMilliseconds(10)))
//                .aggregate(
//                    MockInitializer.STRING_INIT,
//                    MockAggregator.TOSTRING_ADDER,
//                    (aggKey, aggOne, aggTwo) => null,
//                    Materialized.As(INVALID_STORE_NAME));
//        }


//        [Fact]
//        public void shouldThrowNullPointerOnReduceWhenMaterializedIsNull()
//        {
//            groupedStream.reduce(MockReducer.STRING_ADDER, (Materialized)null);
//        }


//        [Fact]
//        public void shouldThrowNullPointerOnAggregateWhenMaterializedIsNull()
//        {
//            groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, (Materialized)null);
//        }


//        [Fact]
//        public void shouldThrowNullPointerOnCountWhenMaterializedIsNull()
//        {
//            groupedStream.count((Materialized)null);
//        }

//        [Fact]
//        public void shouldCountAndMaterializeResults()
//        {
//            groupedStream.count(Materialize.As < string, long, IKeyValueStore<Bytes, byte[]>("count").withKeySerde(Serdes.String()));

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);

//            IKeyValueStore<string, long> count = driver.getKeyValueStore("count");

//            Assert.Equal(count.get("1"), (3L));
//            Assert.Equal(count.get("2"), (1L));
//            Assert.Equal(count.get("3"), (2L));

//            IKeyValueStore<string, ValueAndTimestamp<long>> count = driver.getTimestampedKeyValueStore("count");

//            Assert.Equal(count.get("1"), (ValueAndTimestamp.make(3L, 10L)));
//            Assert.Equal(count.get("2"), (ValueAndTimestamp.make(1L, 1L)));
//            Assert.Equal(count.get("3"), (ValueAndTimestamp.make(2L, 9L)));
//        }
//    }

//    //    [Fact]
//    //    public void shouldLogAndMeasureSkipsInAggregate()
//    //    {
//    //        groupedStream.count(Materialize.As < string, long, IKeyValueStore<Bytes, byte[]>("count").withKeySerde(Serdes.String()));
//    //        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//    //
//    //        var driver = new TopologyTestDriver(builder.Build(), props);
//    //        processData(driver);
//    //        LogCaptureAppender.unregister(appender);
//    //
//    //        var metrics = driver.metrics();
//    //        Assert.Equal(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
//    //        Assert.NotEqual(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
//    //        Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] offset=[6]"));
//    //    }

//    [Fact]
//    public void shouldReduceAndMaterializeResults()
//    {
//        groupedStream.reduce(
//            MockReducer.STRING_ADDER,
//            Materialize.As < string, string, IKeyValueStore<Bytes, byte[]>("reduce")
//                .withKeySerde(Serdes.String())
//                .withValueSerde(Serdes.String()));

//        var driver = new TopologyTestDriver(builder.Build(), props);
//        processData(driver);

//        IKeyValueStore<string, string> reduced = driver.getKeyValueStore("reduce");

//        Assert.Equal(reduced.get("1"), ("A+C+D"));
//        Assert.Equal(reduced.get("2"), ("B"));
//        Assert.Equal(reduced.get("3"), ("E+F"));
//        IKeyValueStore<string, ValueAndTimestamp<string>> reduced = driver.getTimestampedKeyValueStore("reduce");

//        Assert.Equal(reduced.get("1"), (ValueAndTimestamp.make("A+C+D", 10L)));
//        Assert.Equal(reduced.get("2"), (ValueAndTimestamp.make("B", 1L)));
//        Assert.Equal(reduced.get("3"), (ValueAndTimestamp.make("E+F", 9L)));

//    }

//    //    [Fact]
//    //    public void shouldLogAndMeasureSkipsInReduce()
//    //    {
//    //        groupedStream.reduce(
//    //            MockReducer.STRING_ADDER,
//    //            Materialize.As < string, string, IKeyValueStore<Bytes, byte[]>("reduce")
//    //                .withKeySerde(Serdes.String())
//    //                .withValueSerde(Serdes.String())
//    //        );
//    //
//    //        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//    //        var driver = new TopologyTestDriver(builder.Build(), props);
//    //        processData(driver);
//    //        LogCaptureAppender.unregister(appender);
//    //
//    //        Dictionary metrics = driver.metrics< MetricName, ? : Metric >();
//    //        Assert.Equal(1.0, getMetricByName(metrics, "skipped-records-total", "stream-metrics").metricValue());
//    //        Assert.NotEqual(0.0, getMetricByName(metrics, "skipped-records-rate", "stream-metrics").metricValue());
//    //        Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key or value. key=[3] value=[null] topic=[topic] partition=[0] offset=[6]"));
//    //    }



//    [Fact]
//    public void shouldAggregateAndMaterializeResults()
//    {
//        groupedStream.aggregate(
//            MockInitializer.STRING_INIT,
//            MockAggregator.TOSTRING_ADDER,
//            Materialize.As < string, string, IKeyValueStore<Bytes, byte[]>("aggregate")
//                .withKeySerde(Serdes.String())
//                .withValueSerde(Serdes.String()));

//        var driver = new TopologyTestDriver(builder.Build(), props);
//        processData(driver);

//        {
//            IKeyValueStore<string, string> aggregate = driver.getKeyValueStore("aggregate");

//            Assert.Equal(aggregate.get("1"), ("0+A+C+D"));
//            Assert.Equal(aggregate.get("2"), ("0+B"));
//            Assert.Equal(aggregate.get("3"), ("0+E+F"));
//        }
//        {
//            IKeyValueStore<string, ValueAndTimestamp<string>> aggregate = driver.getTimestampedKeyValueStore("aggregate");

//            Assert.Equal(aggregate.get("1"), (ValueAndTimestamp.make("0+A+C+D", 10L)));
//            Assert.Equal(aggregate.get("2"), (ValueAndTimestamp.make("0+B", 1L)));
//            Assert.Equal(aggregate.get("3"), (ValueAndTimestamp.make("0+E+F", 9L)));
//        }
//    }


//    [Fact]
//    public void shouldAggregateWithDefaultSerdes()
//    {
//        MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<>();
//        groupedStream
//            .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER)
//            .toStream()
//            .process(supplier);

//        var driver = new TopologyTestDriver(builder.Build(), props);
//        processData(driver);

//        Assert.Equal(
//            supplier.theCapturedProcessor().lastValueAndTimestampPerKey.get("1"),
//            equalTo(ValueAndTimestamp.make("0+A+C+D", 10L)));
//        Assert.Equal(
//            supplier.theCapturedProcessor().lastValueAndTimestampPerKey.get("2"),
//            equalTo(ValueAndTimestamp.make("0+B", 1L)));
//        Assert.Equal(
//            supplier.theCapturedProcessor().lastValueAndTimestampPerKey.get("3"),
//            equalTo(ValueAndTimestamp.make("0+E+F", 9L)));
//    }

//    private void processData(Top driver)
//    {
//        driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 5L));
//        driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 1L));
//        driver.pipeInput(recordFactory.create(TOPIC, "1", "C", 3L));
//        driver.pipeInput(recordFactory.create(TOPIC, "1", "D", 10L));
//        driver.pipeInput(recordFactory.create(TOPIC, "3", "E", 8L));
//        driver.pipeInput(recordFactory.create(TOPIC, "3", "F", 9L));
//        driver.pipeInput(recordFactory.create(TOPIC, "3", (string)null));
//    }

//    private void doCountWindowed(MockProcessorSupplier<Windowed<string>, long> supplier)
//    {
//        try
//        {
//            var driver = new TopologyTestDriver(builder.Build(), props);
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 0L));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 499L));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 100L));
//            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 0L));
//            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 100L));
//            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 200L));
//            driver.pipeInput(recordFactory.create(TOPIC, "3", "C", 1L));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 500L));
//            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 500L));
//            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 500L));
//            driver.pipeInput(recordFactory.create(TOPIC, "2", "B", 500L));
//            driver.pipeInput(recordFactory.create(TOPIC, "3", "B", 100L));
//        }
//        Assert.Equal(supplier.theCapturedProcessor().processed, (Array.AsReadOnly(
//            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(0L, 500L)), 1L, 0L),
//            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(0L, 500L)), 2L, 499L),
//            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(0L, 500L)), 3L, 499L),
//            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(0L, 500L)), 1L, 0L),
//            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(0L, 500L)), 2L, 100L),
//            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(0L, 500L)), 3L, 200L),
//            new KeyValueTimestamp<>(new Windowed<>("3", new TimeWindow(0L, 500L)), 1L, 1L),
//            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(500L, 1000L)), 1L, 500L),
//            new KeyValueTimestamp<>(new Windowed<>("1", new TimeWindow(500L, 1000L)), 2L, 500L),
//            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(500L, 1000L)), 1L, 500L),
//            new KeyValueTimestamp<>(new Windowed<>("2", new TimeWindow(500L, 1000L)), 2L, 500L),
//            new KeyValueTimestamp<>(new Windowed<>("3", new TimeWindow(0L, 500L)), 2L, 100L)
//        )));
//    }

//    [Fact]
//    public void shouldCountWindowed()
//    {
//        MockProcessorSupplier<Windowed<string>, long> supplier = new MockProcessorSupplier<>();
//        groupedStream
//            .windowedBy(TimeWindows.of(Duration.FromMilliseconds(500L)))
//            .count(Materialized.As("aggregate-by-key-windowed"))
//            .toStream()
//            .process(supplier);

//        doCountWindowed(supplier);
//    }

//    [Fact]
//    public void shouldCountWindowedWithInternalStoreName()
//    {
//        MockProcessorSupplier<Windowed<string>, long> supplier = new MockProcessorSupplier<>();
//        List<KeyValuePair<Windowed<string>, KeyValuePair<long, long>>> results = new List<>();
//        groupedStream
//            .windowedBy(TimeWindows.of(Duration.FromMilliseconds(500L)))
//            .count()
//            .toStream()
//            .process(supplier);

//        doCountWindowed(supplier);
//    }
//}
