namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Window;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class TimeWindowedKStreamImplTest
//    {
//        private static string TOPIC = "input";
//        private StreamsBuilder builder = new StreamsBuilder();
//        private ConsumerRecordFactory<string, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.String(), Serdes.String());
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());
//        private TimeWindowedKStream<string, string> windowedStream;


//        public void before()
//        {
//            IKStream<string, string> stream = builder.Stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
//            windowedStream = stream.
//                groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
//                .windowedBy(TimeWindows.of(Duration.FromMilliseconds(500L)));
//        }

//        [Fact]
//        public void shouldCountWindowed()
//        {
//            MockProcessorSupplier<Windowed<string>, long> supplier = new MockProcessorSupplier<>();
//            windowedStream
//                .count()
//                .toStream()
//                .process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);

//            Assert.Equal(
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey
//                    .get(new Windowed<>("1", new TimeWindow(0L, 500L))),
//                equalTo(ValueAndTimestamp.make(2L, 15L)));
//            Assert.Equal(
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey
//                    .get(new Windowed<>("2", new TimeWindow(500L, 1000L))),
//                equalTo(ValueAndTimestamp.make(2L, 550L)));
//            Assert.Equal(
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey
//                    .get(new Windowed<>("1", new TimeWindow(500L, 1000L))),
//                equalTo(ValueAndTimestamp.make(1L, 500L)));
//        }

//        [Fact]
//        public void shouldReduceWindowed()
//        {
//            MockProcessorSupplier<Windowed<string>, string> supplier = new MockProcessorSupplier<>();
//            windowedStream
//                .reduce(MockReducer.STRING_ADDER)
//                .toStream()
//                .process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);

//            Assert.Equal(
//                supplier.theCapturedProcessor().CastValueAndTimestampPerKey
//                    .get(new Windowed<>("1", new TimeWindow(0L, 500L))),
//                equalTo(ValueAndTimestamp.make("1+2", 15L)));
//            Assert.Equal(
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey
//                    .get(new Windowed<>("2", new TimeWindow(500L, 1000L))),
//                equalTo(ValueAndTimestamp.make("10+20", 550L)));
//            Assert.Equal(
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey
//                    .get(new Windowed<>("1", new TimeWindow(500L, 1000L))),
//                equalTo(ValueAndTimestamp.make("3", 500L)));
//        }

//        [Fact]
//        public void shouldAggregateWindowed()
//        {
//            MockProcessorSupplier<Windowed<string>, string> supplier = new MockProcessorSupplier<>();
//            windowedStream
//                .aggregate(
//                    MockInitializer.STRING_INIT,
//                    MockAggregator.TOSTRING_ADDER,
//                    Materialized.with(Serdes.String(), Serdes.String()))
//                .toStream()
//                .process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);

//            Assert.Equal(
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey
//                    .get(new Windowed<>("1", new TimeWindow(0L, 500L))),
//                equalTo(ValueAndTimestamp.make("0+1+2", 15L)));
//            Assert.Equal(
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey
//                    .get(new Windowed<>("2", new TimeWindow(500L, 1000L))),
//                equalTo(ValueAndTimestamp.make("0+10+20", 550L)));
//            Assert.Equal(
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey
//                    .get(new Windowed<>("1", new TimeWindow(500L, 1000L))),
//                equalTo(ValueAndTimestamp.make("0+3", 500L)));
//        }

//        [Fact]
//        public void shouldMaterializeCount()
//        {
//            windowedStream.count(
//                Materialize.As<string, long, IWindowStore<Bytes, byte[]>("count-store")
//                    .withKeySerde(Serdes.String())
//                    .withValueSerde(Serdes.Long()));

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);

//            IWindowStore<string, long> windowStore = driver.getWindowStore("count-store");
//            List<KeyValuePair<Windowed<string>, long>> data =
//                StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

//            Assert.Equal(data, Array.AsReadOnly(
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(0, 500)), 2L),
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(500, 1000)), 1L),
//                KeyValuePair.Create(new Windowed<>("2", new TimeWindow(500, 1000)), 2L)));

//            IWindowStore<string, ValueAndTimestamp<long>> windowStore =
//                        driver.getTimestampedWindowStore("count-store");
//            List<KeyValuePair<Windowed<string>, ValueAndTimestamp<long>>> data =
//                StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

//            Assert.Equal(data, (Array.AsReadOnly(
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(0, 500)), ValueAndTimestamp.make(2L, 15L)),
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.make(1L, 500L)),
//                KeyValuePair.Create(new Windowed<>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.make(2L, 550L)))));
//        }

//        [Fact]
//        public void shouldMaterializeReduced()
//        {
//            windowedStream.reduce(
//                MockReducer.STRING_ADDER,
//                Materialize.As < string, string, IWindowStore<Bytes, byte[]>("reduced")
//                    .withKeySerde(Serdes.String())
//                    .withValueSerde(Serdes.String()));

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);
//            IWindowStore<string, string> windowStore = driver.getWindowStore("reduced");
//            List<KeyValuePair<Windowed<string>, string>> data =
//                StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

//            Assert.Equal(data, (Array.AsReadOnly(
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(0, 500)), "1+2"),
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(500, 1000)), "3"),
//                KeyValuePair.Create(new Windowed<>("2", new TimeWindow(500, 1000)), "10+20"))));

//            IWindowStore<string, ValueAndTimestamp<string>> windowStore = driver.getTimestampedWindowStore("reduced");
//            List<KeyValuePair<Windowed<string>, ValueAndTimestamp<string>>> data =
//                StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

//            Assert.Equal(data, (Array.AsReadOnly(
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(0, 500)), ValueAndTimestamp.make("1+2", 15L)),
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.make("3", 500L)),
//                KeyValuePair.Create(new Windowed<>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.make("10+20", 550L)))));
//        }

//        [Fact]
//        public void shouldMaterializeAggregated()
//        {
//            windowedStream.aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                Materialize.As < string, string, IWindowStore<Bytes, byte[]>("aggregated")
//                    .withKeySerde(Serdes.String())
//                    .withValueSerde(Serdes.String()));

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);

//            IWindowStore<string, string> windowStore = driver.getWindowStore("aggregated");
//            List<KeyValuePair<Windowed<string>, string>> data =
//                StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

//            Assert.Equal(data, (Array.AsReadOnly(
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(0, 500)), "0+1+2"),
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(500, 1000)), "0+3"),
//                KeyValuePair.Create(new Windowed<>("2", new TimeWindow(500, 1000)), "0+10+20"))));

//            IWindowStore<string, ValueAndTimestamp<string>> windowStore = driver.getTimestampedWindowStore("aggregated");
//            List<KeyValuePair<Windowed<string>, ValueAndTimestamp<string>>> data =
//                StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

//            Assert.Equal(data, (Array.AsReadOnly(
//                KeyValuePair(new Windowed<>("1", new TimeWindow(0, 500)), ValueAndTimestamp.make("0+1+2", 15L)),
//                KeyValuePair.Create(new Windowed<>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.make("0+3", 500L)),
//                KeyValuePair.Create(new Windowed<>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.make("0+10+20", 550L)))));
//        }
//    }

//    [Fact]
//    public void shouldThrowNullPointerOnAggregateIfInitializerIsNull()
//    {
//        windowedStream.aggregate(null, MockAggregator.TOSTRING_ADDER);
//    }

//    [Fact]
//    public void shouldThrowNullPointerOnAggregateIfAggregatorIsNull()
//    {
//        windowedStream.aggregate(MockInitializer.STRING_INIT, null);
//    }

//    [Fact]
//    public void shouldThrowNullPointerOnReduceIfReducerIsNull()
//    {
//        windowedStream.reduce(null);
//    }

//    [Fact]
//    public void shouldThrowNullPointerOnMaterializedAggregateIfInitializerIsNull()
//    {
//        windowedStream.aggregate(
//            null,
//            MockAggregator.TOSTRING_ADDER,
//            Materialized.As("store"));
//    }

//    [Fact]
//    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull()
//    {
//        windowedStream.aggregate(
//            MockInitializer.STRING_INIT,
//            null,
//            Materialized.As("store"));
//    }


//    [Fact]
//    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull()
//    {
//        windowedStream.aggregate(
//            MockInitializer.STRING_INIT,
//            MockAggregator.TOSTRING_ADDER,
//            (Materialized)null);
//    }

//    [Fact]
//    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull()
//    {
//        windowedStream.reduce(
//            null,
//            Materialized.As("store"));
//    }

//    [Fact]
//    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull()
//    {
//        windowedStream.reduce(
//            MockReducer.STRING_ADDER,
//            null);
//    }

//    [Fact]
//    public void shouldThrowNullPointerOnCountIfMaterializedIsNull()
//    {
//        windowedStream.count(null);
//    }

//    private void processData(var driver)
//    {
//        driver.pipeInput(recordFactory.create(TOPIC, "1", "1", 10L));
//        driver.pipeInput(recordFactory.create(TOPIC, "1", "2", 15L));
//        driver.pipeInput(recordFactory.create(TOPIC, "1", "3", 500L));
//        driver.pipeInput(recordFactory.create(TOPIC, "2", "10", 550L));
//        driver.pipeInput(recordFactory.create(TOPIC, "2", "20", 500L));
//    }

//}
