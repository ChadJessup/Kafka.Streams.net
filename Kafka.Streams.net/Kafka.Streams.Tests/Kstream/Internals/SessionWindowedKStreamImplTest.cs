namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Sessions;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class SessionWindowedKStreamImplTest
//    {
//        private static string TOPIC = "input";
//        private StreamsBuilder builder = new StreamsBuilder();
//        private ConsumerRecordFactory<string, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.String(), Serdes.String());
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());
//        private Merger<string, string> sessionMerger = (aggKey, aggOne, aggTwo) => aggOne + "+" + aggTwo;
//        private SessionWindowedKStream<string, string> stream;


//        public void before()
//        {
//            IKStream<string, string> stream = builder.Stream(TOPIC, Consumed.With(Serdes.String(), Serdes.String()));
//            this.Stream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
//                    .windowedBy(SessionWindows.with(TimeSpan.FromMilliseconds(500)));
//        }

//        [Fact]
//        public void shouldCountSessionWindowedWithCachingDisabled()
//        {
//            props.Set(StreamsConfigPropertyNames.CacheMaxBytesBuffering, 0);
//            shouldCountSessionWindowed();
//        }

//        [Fact]
//        public void shouldCountSessionWindowedWithCachingEnabled()
//        {
//            shouldCountSessionWindowed();
//        }

//        private void shouldCountSessionWindowed()
//        {
//            MockProcessorSupplier<Windowed<string>, long> supplier = new MockProcessorSupplier<>();
//            stream.count()
//                .toStream()
//                .process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);

//            Dictionary<Windowed<string>, ValueAndTimestamp<long>> result =
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey;

//            Assert.Equal(result.Count, (3));
//            Assert.Equal(
//                result.Get(new Windowed<>("1", new SessionWindow(10L, 15L))),
//                equalTo(ValueAndTimestamp.Make(2L, 15L)));

//            Assert.Equal(
//                result.Get(new Windowed<>("2", new SessionWindow(599L, 600L))),
//                equalTo(ValueAndTimestamp.Make(2L, 600L)));

//            Assert.Equal(
//                result.Get(new Windowed<>("1", new SessionWindow(600L, 600L))),
//                equalTo(ValueAndTimestamp.Make(1L, 600L)));
//        }

//        [Fact]
//        public void shouldReduceWindowed()
//        {
//            MockProcessorSupplier<Windowed<string>, string> supplier = new MockProcessorSupplier<>();
//            stream.reduce(MockReducer.STRING_ADDER)
//                .toStream()
//                .process(supplier);

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);

//            Dictionary<Windowed<string>, ValueAndTimestamp<string>> result =
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey;

//            Assert.Equal(result.Count, (3));
//            Assert.Equal(
//                result.Get(new Windowed<>("1", new SessionWindow(10, 15))),
//                equalTo(ValueAndTimestamp.Make("1+2", 15L)));
//            Assert.Equal(
//                result.Get(new Windowed<>("2", new SessionWindow(599L, 600))),
//                equalTo(ValueAndTimestamp.Make("1+2", 600L)));
//            Assert.Equal(
//                result.Get(new Windowed<>("1", new SessionWindow(600, 600))),
//                equalTo(ValueAndTimestamp.Make("3", 600L)));
//        }

//        [Fact]
//        public void shouldAggregateSessionWindowed()
//        {
//            MockProcessorSupplier<Windowed<string>, string> supplier = new MockProcessorSupplier<>();
//            stream.aggregate(MockInitializer.STRING_INIT,
//                             MockAggregator.TOSTRING_ADDER,
//                             sessionMerger,
//                             Materialized.with(Serdes.String(), Serdes.String()))
//                .toStream()
//                .process(supplier);
//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);

//            Dictionary<Windowed<string>, ValueAndTimestamp<string>> result =
//                supplier.theCapturedProcessor().lastValueAndTimestampPerKey;

//            Assert.Equal(result.Count, (3));
//            Assert.Equal(
//                result.Get(new Windowed<>("1", new SessionWindow(10, 15))),
//                equalTo(ValueAndTimestamp.Make("0+0+1+2", 15L)));
//            Assert.Equal(
//                result.Get(new Windowed<>("2", new SessionWindow(599, 600))),
//                equalTo(ValueAndTimestamp.Make("0+0+1+2", 600L)));
//            Assert.Equal(
//                result.Get(new Windowed<>("1", new SessionWindow(600, 600))),
//                equalTo(ValueAndTimestamp.Make("0+3", 600L)));
//        }

//        [Fact]
//        public void shouldMaterializeCount()
//        {
//            stream.count(Materialized.As("count-store"));

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                processData(driver);
//                ISessionStore<string, long> store = driver.getSessionStore("count-store");
//                List<KeyValuePair<Windowed<string>, long>> data = StreamsTestUtils.toList(store.Fetch("1", "2"));
//                Assert.Equal(
//                    data,
//                    equalTo(Array.AsReadOnly(
//                        KeyValuePair.Create(new Windowed<>("1", new SessionWindow(10, 15)), 2L),
//                        KeyValuePair.Create(new Windowed<>("1", new SessionWindow(600, 600)), 1L),
//                        KeyValuePair.Create(new Windowed<>("2", new SessionWindow(599, 600)), 2L))));
//            }
//    }

//        [Fact]
//        public void shouldMaterializeReduced()
//        {
//            stream.reduce(MockReducer.STRING_ADDER, Materialized.As("reduced"));

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            processData(driver);
//            ISessionStore<string, string> sessionStore = driver.getSessionStore("reduced");
//            List<KeyValuePair<Windowed<string>, string>> data = StreamsTestUtils.toList(sessionStore.Fetch("1", "2"));

//            Assert.Equal(
//                data,
//                equalTo(Array.AsReadOnly(
//                    KeyValuePair.Create(new Windowed<>("1", new SessionWindow(10, 15)), "1+2"),
//                    KeyValuePair.Create(new Windowed<>("1", new SessionWindow(600, 600)), "3"),
//                    KeyValuePair.Create(new Windowed<>("2", new SessionWindow(599, 600)), "1+2"))));
//        }
//    }

//    [Fact]
//    public void shouldMaterializeAggregated()
//    {
//        stream.aggregate(
//            MockInitializer.STRING_INIT,
//            MockAggregator.TOSTRING_ADDER,
//            sessionMerger,
//            Materialize.As < string, string, ISessionStore<Bytes, byte[]>("aggregated").withValueSerde(Serdes.String()));

//        var driver = new TopologyTestDriver(builder.Build(), props);
//        processData(driver);
//        ISessionStore<string, string> sessionStore = driver.getSessionStore("aggregated");
//        List<KeyValuePair<Windowed<string>, string>> data = StreamsTestUtils.toList(sessionStore.Fetch("1", "2"));
//        Assert.Equal(
//            data,
//            Array.AsReadOnly(
//                KeyValuePair.Create(new Windowed<>("1", new SessionWindow(10, 15)), "0+0+1+2"),
//                KeyValuePair.Create(new Windowed<>("1", new SessionWindow(600, 600)), "0+3"),
//                KeyValuePair.Create(new Windowed<>("2", new SessionWindow(599, 600)), "0+0+1+2")));
//    }
//}

//[Fact]
//public void shouldThrowNullPointerOnAggregateIfInitializerIsNull()
//{
//    stream.aggregate(null, MockAggregator.TOSTRING_ADDER, sessionMerger);
//}

//[Fact]
//public void shouldThrowNullPointerOnAggregateIfAggregatorIsNull()
//{
//    stream.aggregate(MockInitializer.STRING_INIT, null, sessionMerger);
//}

//[Fact]
//public void shouldThrowNullPointerOnAggregateIfMergerIsNull()
//{
//    stream.aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, null);
//}

//[Fact]
//public void shouldThrowNullPointerOnReduceIfReducerIsNull()
//{
//    stream.reduce(null);
//}

//[Fact]
//public void shouldThrowNullPointerOnMaterializedAggregateIfInitializerIsNull()
//{
//    stream.aggregate(
//        null,
//        MockAggregator.TOSTRING_ADDER,
//        sessionMerger,
//        Materialized.As("store"));
//}

//[Fact]
//public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull()
//{
//    stream.aggregate(
//        MockInitializer.STRING_INIT,
//        null,
//        sessionMerger,
//        Materialized.As("store"));
//}

//[Fact]
//public void shouldThrowNullPointerOnMaterializedAggregateIfMergerIsNull()
//{
//    stream.aggregate(
//        MockInitializer.STRING_INIT,
//        MockAggregator.TOSTRING_ADDER,
//        null,
//        Materialized.As("store"));
//}


//[Fact]
//public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull()
//{
//    stream.aggregate(
//        MockInitializer.STRING_INIT,
//        MockAggregator.TOSTRING_ADDER,
//        sessionMerger,
//        (Materialized)null);
//}

//[Fact]
//public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull()
//{
//    stream.reduce(null, Materialized.As("store"));
//}

//[Fact]
//public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull()
//{
//    stream.reduce(MockReducer.STRING_ADDER,
//                  null);
//}

//[Fact]
//public void shouldThrowNullPointerOnCountIfMaterializedIsNull()
//{
//    stream.count(null);
//}

//private void processData(var driver)
//{
//    driver.PipeInput(recordFactory.Create(TOPIC, "1", "1", 10));
//    driver.PipeInput(recordFactory.Create(TOPIC, "1", "2", 15));
//    driver.PipeInput(recordFactory.Create(TOPIC, "1", "3", 600));
//    driver.PipeInput(recordFactory.Create(TOPIC, "2", "1", 600));
//    driver.PipeInput(recordFactory.Create(TOPIC, "2", "2", 599));
//}
//}
