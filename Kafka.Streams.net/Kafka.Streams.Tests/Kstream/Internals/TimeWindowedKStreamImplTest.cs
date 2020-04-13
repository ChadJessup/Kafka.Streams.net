using Kafka.Streams.State;

using Kafka.Streams;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.Windowed;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Kstream.Internals
{

    public class TimeWindowedKStreamImplTest
    {
        private static string TOPIC = "input";
        private StreamsBuilder builder = new StreamsBuilder();
        private ConsumerRecordFactory<string, string> recordFactory =
            new ConsumerRecordFactory<>(Serdes.String(), Serdes.String());
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());
        private TimeWindowedIIKStream<K, V> windowedStream;


        public void before()
        {
            IKStream<K, V> stream = builder.Stream(TOPIC, Consumed.With(Serdes.String(), Serdes.String()));
            windowedStream = stream.
                groupByKey(Grouped.With(Serdes.String(), Serdes.String()))
                .WindowedBy(TimeWindows.of(TimeSpan.FromMilliseconds(500L)));
        }

        [Fact]
        public void shouldCountWindowed()
        {
            MockProcessorSupplier<IWindowed<string>, long> supplier = new MockProcessorSupplier<>();
            windowedStream
                .Count()
                .ToStream()
                .Process(supplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            processData(driver);

            Assert.Equal(
                supplier.TheCapturedProcessor().lastValueAndTimestampPerKey
                    .Get(new Windowed<string>("1", new TimeWindow(0L, 500L))),
                equalTo(ValueAndTimestamp.Make(2L, 15L)));
            Assert.Equal(
                supplier.TheCapturedProcessor().lastValueAndTimestampPerKey
                    .Get(new Windowed<string>("2", new TimeWindow(500L, 1000L))),
                equalTo(ValueAndTimestamp.Make(2L, 550L)));
            Assert.Equal(
                supplier.TheCapturedProcessor().lastValueAndTimestampPerKey
                    .Get(new Windowed<string>("1", new TimeWindow(500L, 1000L))),
                equalTo(ValueAndTimestamp.Make(1L, 500L)));
        }

        [Fact]
        public void shouldReduceWindowed()
        {
            MockProcessorSupplier<IWindowed<string>, string> supplier = new MockProcessorSupplier<>();
            windowedStream
                .Reduce(MockReducer.STRING_ADDER)
                .ToStream()
                .Process(supplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            processData(driver);

            Assert.Equal(
                supplier.TheCapturedProcessor().CastValueAndTimestampPerKey
                    .Get(new Windowed<string>("1", new TimeWindow(0L, 500L))),
                equalTo(ValueAndTimestamp.Make("1+2", 15L)));
            Assert.Equal(
                supplier.TheCapturedProcessor().lastValueAndTimestampPerKey
                    .Get(new Windowed<string>("2", new TimeWindow(500L, 1000L))),
                equalTo(ValueAndTimestamp.Make("10+20", 550L)));
            Assert.Equal(
                supplier.TheCapturedProcessor().lastValueAndTimestampPerKey
                    .Get(new Windowed<string>("1", new TimeWindow(500L, 1000L))),
                equalTo(ValueAndTimestamp.Make("3", 500L)));
        }

        [Fact]
        public void shouldAggregateWindowed()
        {
            MockProcessorSupplier<IWindowed<string>, string> supplier = new MockProcessorSupplier<>();
            windowedStream
                .Aggregate(
                    MockInitializer.STRING_INIT,
                    MockAggregator.TOSTRING_ADDER,
                    Materialized.With(Serdes.String(), Serdes.String()))
                .ToStream()
                .Process(supplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            processData(driver);

            Assert.Equal(
                supplier.TheCapturedProcessor().lastValueAndTimestampPerKey
                    .Get(new Windowed<string>("1", new TimeWindow(0L, 500L))),
                equalTo(ValueAndTimestamp.Make("0+1+2", 15L)));
            Assert.Equal(
                supplier.TheCapturedProcessor().lastValueAndTimestampPerKey
                    .Get(new Windowed<string>("2", new TimeWindow(500L, 1000L))),
                equalTo(ValueAndTimestamp.Make("0+10+20", 550L)));
            Assert.Equal(
                supplier.TheCapturedProcessor().lastValueAndTimestampPerKey
                    .Get(new Windowed<string>("1", new TimeWindow(500L, 1000L))),
                equalTo(ValueAndTimestamp.Make("0+3", 500L)));
        }

        [Fact]
        public void shouldMaterializeCount()
        {
            windowedStream.Count(
                Materialized.As < string, long, IWindowStore<Bytes, byte[]>("count-store")
                    .WithKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long()));

            var driver = new TopologyTestDriver(builder.Build(), props);
            processData(driver);

            IWindowStore<string, long> windowStore = driver.getWindowStore("count-store");
            List<KeyValuePair<IWindowed<string>, long>> data =
                StreamsTestUtils.toList(windowStore.Fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

            Assert.Equal(data, Array.AsReadOnly(
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(0, 500)), 2L),
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(500, 1000)), 1L),
                KeyValuePair.Create(new Windowed<string>("2", new TimeWindow(500, 1000)), 2L)));

            IWindowStore<string, IValueAndTimestamp<long>> windowStore =
                        driver.getTimestampedWindowStore("count-store");
            List<KeyValuePair<IWindowed<string>, IValueAndTimestamp<long>>> data =
                StreamsTestUtils.toList(windowStore.Fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

            Assert.Equal(data, Array.AsReadOnly(
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(0, 500)), ValueAndTimestamp.Make(2L, 15L)),
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.Make(1L, 500L)),
                KeyValuePair.Create(new Windowed<string>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.Make(2L, 550L))));
        }

        [Fact]
        public void shouldMaterializeReduced()
        {
            windowedStream.Reduce(
                MockReducer.STRING_ADDER,
                Materialized.As < string, string, IWindowStore<Bytes, byte[]>("reduced")
                    .WithKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String()));

            var driver = new TopologyTestDriver(builder.Build(), props);
            processData(driver);
            IWindowStore<string, string> windowStore = driver.getWindowStore("reduced");
            List<KeyValuePair<IWindowed<string>, string>> data =
                StreamsTestUtils.toList(windowStore.Fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

            Assert.Equal(data, Array.AsReadOnly(
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(0, 500)), "1+2"),
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(500, 1000)), "3"),
                KeyValuePair.Create(new Windowed<string>("2", new TimeWindow(500, 1000)), "10+20")));

            IWindowStore<string, IValueAndTimestamp<string>> windowStore = driver.getTimestampedWindowStore("reduced");
            List<KeyValuePair<IWindowed<string>, IValueAndTimestamp<string>>> data =
                StreamsTestUtils.toList(windowStore.Fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

            Assert.Equal(data, Array.AsReadOnly(
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(0, 500)), ValueAndTimestamp.Make("1+2", 15L)),
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.Make("3", 500L)),
                KeyValuePair.Create(new Windowed<string>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.Make("10+20", 550L))));
        }

        [Fact]
        public void shouldMaterializeAggregated()
        {
            windowedStream.Aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                Materialized.As < string, string, IWindowStore<Bytes, byte[]>("aggregated")
                    .WithKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String()));

            var driver = new TopologyTestDriver(builder.Build(), props);
            processData(driver);

            IWindowStore<string, string> windowStore = driver.getWindowStore("aggregated");
            List<KeyValuePair<IWindowed<string>, string>> data =
                StreamsTestUtils.toList(windowStore.Fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

            Assert.Equal(data, Array.AsReadOnly(
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(0, 500)), "0+1+2"),
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(500, 1000)), "0+3"),
                KeyValuePair.Create(new Windowed<string>("2", new TimeWindow(500, 1000)), "0+10+20")));

            IWindowStore<string, IValueAndTimestamp<string>> windowStore = driver.getTimestampedWindowStore("aggregated");
            List<KeyValuePair<IWindowed<string>, IValueAndTimestamp<string>>> data =
                StreamsTestUtils.toList(windowStore.Fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

            Assert.Equal(data, Array.AsReadOnly(
                KeyValuePair(new Windowed<string>("1", new TimeWindow(0, 500)), ValueAndTimestamp.Make("0+1+2", 15L)),
                KeyValuePair.Create(new Windowed<string>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.Make("0+3", 500L)),
                KeyValuePair.Create(new Windowed<string>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.Make("0+10+20", 550L))));
        }
    }

    [Fact]
    public void shouldThrowNullPointerOnAggregateIfInitializerIsNull()
    {
        windowedStream.Aggregate(null, MockAggregator.TOSTRING_ADDER);
    }

    [Fact]
    public void shouldThrowNullPointerOnAggregateIfAggregatorIsNull()
    {
        windowedStream.Aggregate(MockInitializer.STRING_INIT, null);
    }

    [Fact]
    public void shouldThrowNullPointerOnReduceIfReducerIsNull()
    {
        windowedStream.Reduce(null);
    }

    [Fact]
    public void shouldThrowNullPointerOnMaterializedAggregateIfInitializerIsNull()
    {
        windowedStream.Aggregate(
            null,
            MockAggregator.TOSTRING_ADDER,
            Materialized.As("store"));
    }

    [Fact]
    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull()
    {
        windowedStream.Aggregate(
            MockInitializer.STRING_INIT,
            null,
            Materialized.As("store"));
    }


    [Fact]
    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull()
    {
        windowedStream.Aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            (Materialized)null);
    }

    [Fact]
    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull()
    {
        windowedStream.Reduce(
            null,
            Materialized.As("store"));
    }

    [Fact]
    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull()
    {
        windowedStream.Reduce(
            MockReducer.STRING_ADDER,
            null);
    }

    [Fact]
    public void shouldThrowNullPointerOnCountIfMaterializedIsNull()
    {
        windowedStream.Count(null);
    }

    private void processData(var driver)
    {
        driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 10L));
        driver.PipeInput(recordFactory.Create(TOPIC, "1", "2", 15L));
        driver.PipeInput(recordFactory.Create(TOPIC, "1", "3", 500L));
        driver.PipeInput(recordFactory.Create(TOPIC, "2", "10", 550L));
        driver.PipeInput(recordFactory.Create(TOPIC, "2", "20", 500L));
    }

}
