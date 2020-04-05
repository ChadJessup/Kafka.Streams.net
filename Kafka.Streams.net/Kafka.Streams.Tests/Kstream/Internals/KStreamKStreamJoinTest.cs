namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamKStreamJoinTest
//    {
//        private static KeyValueTimestamp[] EMPTY = new KeyValueTimestamp[0];

//        private string topic1 = "topic1";
//        private string topic2 = "topic2";
//        private Consumed<int, string> consumed = Consumed.With(Serdes.Int(), Serdes.String());
//        private ConsumerRecordFactory<int, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

//        // [Fact]
//        // public void shouldLogAndMeterOnSkippedRecordsWithNullValue()
//        // {
//        //     var builder = new StreamsBuilder();
//        // 
//        //     IKStream<string, int> left = builder.Stream("left", Consumed.With(Serdes.String(), Serdes.Int()));
//        //     IKStream<string, int> right = builder.Stream("right", Consumed.With(Serdes.String(), Serdes.Int()));
//        //     ConsumerRecordFactory<string, int> recordFactory =
//        //         new ConsumerRecordFactory<>(Serdes.String(), Serdes.Int());
//        // 
//        //     left.join(
//        //         right,
//        //         (value1, value2) => value1 + value2,
//        //         JoinWindows.of(Duration.FromMilliseconds(100)),
//        //         Joined.with(Serdes.String(), Serdes.Int(), Serdes.Int())
//        //     );
//        // 
//        //     LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//        //     try
//        //     {
//        //         var driver = new TopologyTestDriver(builder.Build(), props);
//        //         driver.PipeInput(recordFactory.Create("left", "A", null));
//        //         LogCaptureAppender.unregister(appender);
//        // 
//        //         Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key or value. key=[A] value=[null] topic=[left] partition=[0] offset=[0]"));
//        // 
//        //         Assert.Equal(1.0, getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue());
//        //     }
//        // 

//        [Fact]
//        public void testJoin()
//        {
//            var builder = new StreamsBuilder();

//            var expectedKeys = new int[] { 0, 1, 2, 3 };

//            IKStream<int, string> stream1;
//            IKStream<int, string> stream2;
//            IKStream<int, string> joined;
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
//            stream1 = builder.Stream(topic1, consumed);
//            stream2 = builder.Stream(topic2, consumed);
//            joined = stream1.join(
//                stream2,
//                MockValueJoiner.TOSTRING_JOINER,
//                JoinWindows.of(Duration.FromMilliseconds(100)),
//                Joined.with(Serdes.Int(), Serdes.String(), Serdes.String()));
//            joined.process(supplier);

//            Collection<HashSet<string>> copartitionGroups =
//                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

//            Assert.Equal(1, copartitionGroups.Count);
//            Assert.Equal(new HashSet<>(new List<string> { topic1, topic2 }), copartitionGroups.iterator().MoveNext());

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                MockProcessor<int, string> processor = supplier.theCapturedProcessor();

//                // push two items to the primary stream; the other window is empty
//                // w1 = {}
//                // w2 = {}
//                // -=> w1 = { 0:A0, 1:A1 }
//                //     w2 = {}
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "A" + expectedKeys[i]));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push two items to the other stream; this should produce two items
//                // w1 = { 0:A0, 1:A1 }
//                // w2 = {}
//                // -=> w1 = { 0:A0, 1:A1 }
//                //     w2 = { 0:a0, 1:a1 }
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "a" + expectedKeys[i]));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+a0", 0),
//                    new KeyValueTimestamp<>(1, "A1+a1", 0));

//                // push all four items to the primary stream; this should produce two items
//                // w1 = { 0:A0, 1:A1 }
//                // w2 = { 0:a0, 1:a1 }
//                // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
//                //     w2 = { 0:a0, 1:a1 }
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "B" + expectedKey));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+a0", 0),
//                    new KeyValueTimestamp<>(1, "B1+a1", 0));

//                // push all items to the other stream; this should produce six items
//                // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
//                // w2 = { 0:a0, 1:a1 }
//                // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
//                //     w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "b" + expectedKey));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+b0", 0),
//                    new KeyValueTimestamp<>(0, "B0+b0", 0),
//                    new KeyValueTimestamp<>(1, "A1+b1", 0),
//                    new KeyValueTimestamp<>(1, "B1+b1", 0),
//                    new KeyValueTimestamp<>(2, "B2+b2", 0),
//                    new KeyValueTimestamp<>(3, "B3+b3", 0));

//                // push all four items to the primary stream; this should produce six items
//                // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
//                // w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
//                // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
//                //     w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "C" + expectedKey));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "C0+a0", 0),
//                    new KeyValueTimestamp<>(0, "C0+b0", 0),
//                    new KeyValueTimestamp<>(1, "C1+a1", 0),
//                    new KeyValueTimestamp<>(1, "C1+b1", 0),
//                    new KeyValueTimestamp<>(2, "C2+b2", 0),
//                    new KeyValueTimestamp<>(3, "C3+b3", 0));

//                // push two items to the other stream; this should produce six items
//                // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
//                // w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
//                // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
//                //     w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3, 0:c0, 1:c1 }
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "c" + expectedKeys[i]));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+c0", 0),
//                    new KeyValueTimestamp<>(0, "B0+c0", 0),
//                    new KeyValueTimestamp<>(0, "C0+c0", 0),
//                    new KeyValueTimestamp<>(1, "A1+c1", 0),
//                    new KeyValueTimestamp<>(1, "B1+c1", 0),
//                    new KeyValueTimestamp<>(1, "C1+c1", 0));
//            }
//    }

//        [Fact]
//        public void testOuterJoin()
//        {
//            var builder = new StreamsBuilder();

//            var expectedKeys = new int[] { 0, 1, 2, 3 };

//            IKStream<int, string> stream1;
//            IKStream<int, string> stream2;
//            IKStream<int, string> joined;
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();

//            stream1 = builder.Stream(topic1, consumed);
//            stream2 = builder.Stream(topic2, consumed);
//            joined = stream1.outerJoin(
//                stream2,
//                MockValueJoiner.TOSTRING_JOINER,
//                JoinWindows.of(Duration.FromMilliseconds(100)),
//                Joined.with(Serdes.Int(), Serdes.String(), Serdes.String()));
//            joined.process(supplier);
//            Collection<HashSet<string>> copartitionGroups =
//                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

//            Assert.Equal(1, copartitionGroups.Count);
//            Assert.Equal(new HashSet<>(new List<string> { topic1, topic2 }), copartitionGroups.iterator().MoveNext());

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                MockProcessor<int, string> processor = supplier.theCapturedProcessor();

//                // push two items to the primary stream; the other window is empty; this should produce two items
//                // w1 = {}
//                // w2 = {}
//                // -=> w1 = { 0:A0, 1:A1 }
//                //     w2 = {}
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "A" + expectedKeys[i]));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+null", 0),
//                    new KeyValueTimestamp<>(1, "A1+null", 0));

//                // push two items to the other stream; this should produce two items
//                // w1 = { 0:A0, 1:A1 }
//                // w2 = {}
//                // -=> w1 = { 0:A0, 1:A1 }
//                //     w2 = { 0:a0, 1:a1 }
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "a" + expectedKeys[i]));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+a0", 0),
//                    new KeyValueTimestamp<>(1, "A1+a1", 0));

//                // push all four items to the primary stream; this should produce four items
//                // w1 = { 0:A0, 1:A1 }
//                // w2 = { 0:a0, 1:a1 }
//                // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
//                //     w2 = { 0:a0, 1:a1 }
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "B" + expectedKey));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+a0", 0),
//                    new KeyValueTimestamp<>(1, "B1+a1", 0),
//                    new KeyValueTimestamp<>(2, "B2+null", 0),
//                    new KeyValueTimestamp<>(3, "B3+null", 0));

//                // push all items to the other stream; this should produce six items
//                // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
//                // w2 = { 0:a0, 1:a1 }
//                // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
//                //     w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3 }
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "b" + expectedKey));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+b0", 0),
//                    new KeyValueTimestamp<>(0, "B0+b0", 0),
//                    new KeyValueTimestamp<>(1, "A1+b1", 0),
//                    new KeyValueTimestamp<>(1, "B1+b1", 0),
//                    new KeyValueTimestamp<>(2, "B2+b2", 0),
//                    new KeyValueTimestamp<>(3, "B3+b3", 0));

//                // push all four items to the primary stream; this should produce six items
//                // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
//                // w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3 }
//                // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
//                //     w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3 }
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "C" + expectedKey));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "C0+a0", 0),
//                    new KeyValueTimestamp<>(0, "C0+b0", 0),
//                    new KeyValueTimestamp<>(1, "C1+a1", 0),
//                    new KeyValueTimestamp<>(1, "C1+b1", 0),
//                    new KeyValueTimestamp<>(2, "C2+b2", 0),
//                    new KeyValueTimestamp<>(3, "C3+b3", 0));

//                // push two items to the other stream; this should produce six items
//                // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
//                // w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3 }
//                // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
//                //     w2 = { 0:a0, 1:a1, 0:b0, 0:b0, 1:b1, 2:b2, 3:b3, 0:c0, 1:c1 }
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "c" + expectedKeys[i]));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+c0", 0),
//                    new KeyValueTimestamp<>(0, "B0+c0", 0),
//                    new KeyValueTimestamp<>(0, "C0+c0", 0),
//                    new KeyValueTimestamp<>(1, "A1+c1", 0),
//                    new KeyValueTimestamp<>(1, "B1+c1", 0),
//                    new KeyValueTimestamp<>(1, "C1+c1", 0));
//            }
//    }

//        [Fact]
//        public void testWindowing()
//        {
//            var builder = new StreamsBuilder();

//            var expectedKeys = new int[] { 0, 1, 2, 3 };

//            IKStream<int, string> stream1;
//            IKStream<int, string> stream2;
//            IKStream<int, string> joined;
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
//            stream1 = builder.Stream(topic1, consumed);
//            stream2 = builder.Stream(topic2, consumed);

//            joined = stream1.join(
//                stream2,
//                MockValueJoiner.TOSTRING_JOINER,
//                JoinWindows.of(Duration.FromMilliseconds(100)),
//                Joined.with(Serdes.Int(), Serdes.String(), Serdes.String()));
//            joined.process(supplier);

//            Collection<HashSet<string>> copartitionGroups =
//                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

//            Assert.Equal(1, copartitionGroups.Count);
//            Assert.Equal(new HashSet<>(new List<string> { topic1, topic2 }), copartitionGroups.iterator().MoveNext());

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                MockProcessor<int, string> processor = supplier.theCapturedProcessor();
//                var time = 0L;

//                // push two items to the primary stream; the other window is empty; this should produce no items
//                // w1 = {}
//                // w2 = {}
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
//                //     w2 = {}
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "A" + expectedKeys[i], time));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push two items to the other stream; this should produce two items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
//                // w2 = {}
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0) }
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "a" + expectedKeys[i], time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+a0", 0),
//                    new KeyValueTimestamp<>(1, "A1+a1", 0));

//                // push four items to the primary stream with larger and incr.Asing timestamp; this should produce no items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0) }
//                time = 1000L;
//                for (var i = 0; i < expectedKeys.Length; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "B" + expectedKeys[i], time + i));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items to the other stream with fixed larger timestamp; this should produce four items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100) }
//                time += 100L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "b" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+b0", 1100),
//                    new KeyValueTimestamp<>(1, "B1+b1", 1100),
//                    new KeyValueTimestamp<>(2, "B2+b2", 1100),
//                    new KeyValueTimestamp<>(3, "B3+b3", 1100));

//                // push four items to the other stream with incremented timestamp; this should produce three items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "c" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(1, "B1+c1", 1101),
//                    new KeyValueTimestamp<>(2, "B2+c2", 1101),
//                    new KeyValueTimestamp<>(3, "B3+c3", 1101));

//                // push four items to the other stream with incremented timestamp; this should produce two items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "d" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "B2+d2", 1102),
//                    new KeyValueTimestamp<>(3, "B3+d3", 1102));

//                // push four items to the other stream with incremented timestamp; this should produce one item
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "e" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(3, "B3+e3", 1103));

//                // push four items to the other stream with incremented timestamp; this should produce no items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "f" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items to the other stream with timestamp before the window bound; this should produce no items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
//                //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899) }
//                time = 1000L - 100L - 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "g" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items to the other stream with with incremented timestamp; this should produce one item
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
//                //        0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
//                //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
//                //            0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "h" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+h0", 1000));

//                // push four items to the other stream with with incremented timestamp; this should produce two items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
//                //        0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
//                //        0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
//                //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
//                //            0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
//                //            0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "i" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+i0", 1000),
//                    new KeyValueTimestamp<>(1, "B1+i1", 1001));

//                // push four items to the other stream with with incremented timestamp; this should produce three items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
//                //        0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
//                //        0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
//                //        0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
//                //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
//                //            0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
//                //            0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901),
//                //            0:j0 (ts: 902), 1:j1 (ts: 902), 2:j2 (ts: 902), 3:j3 (ts: 902) }
//                time += 1;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "j" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+j0", 1000),
//                    new KeyValueTimestamp<>(1, "B1+j1", 1001),
//                    new KeyValueTimestamp<>(2, "B2+j2", 1002));

//                // push four items to the other stream with with incremented timestamp; this should produce four items
//                // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //        0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003)  }
//                // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //        0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //        0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //        0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //        0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //        0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
//                //        0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
//                //        0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
//                //        0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901),
//                //        0:j0 (ts: 902), 1:j1 (ts: 902), 2:j2 (ts: 902), 3:j3 (ts: 902) }
//                // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
//                //            0:B0 (ts: 1000), 1:B1 (ts: 1001), 2:B2 (ts: 1002), 3:B3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0),
//                //            0:b0 (ts: 1100), 1:b1 (ts: 1100), 2:b2 (ts: 1100), 3:b3 (ts: 1100),
//                //            0:c0 (ts: 1101), 1:c1 (ts: 1101), 2:c2 (ts: 1101), 3:c3 (ts: 1101),
//                //            0:d0 (ts: 1102), 1:d1 (ts: 1102), 2:d2 (ts: 1102), 3:d3 (ts: 1102),
//                //            0:e0 (ts: 1103), 1:e1 (ts: 1103), 2:e2 (ts: 1103), 3:e3 (ts: 1103),
//                //            0:f0 (ts: 1104), 1:f1 (ts: 1104), 2:f2 (ts: 1104), 3:f3 (ts: 1104),
//                //            0:g0 (ts: 899), 1:g1 (ts: 899), 2:g2 (ts: 899), 3:g3 (ts: 899),
//                //            0:h0 (ts: 900), 1:h1 (ts: 900), 2:h2 (ts: 900), 3:h3 (ts: 900),
//                //            0:i0 (ts: 901), 1:i1 (ts: 901), 2:i2 (ts: 901), 3:i3 (ts: 901),
//                //            0:j0 (ts: 902), 1:j1 (ts: 902), 2:j2 (ts: 902), 3:j3 (ts: 902) }
//                //            0:k0 (ts: 903), 1:k1 (ts: 903), 2:k2 (ts: 903), 3:k3 (ts: 903) }
//                time += 1;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "k" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+k0", 1000),
//                    new KeyValueTimestamp<>(1, "B1+k1", 1001),
//                    new KeyValueTimestamp<>(2, "B2+k2", 1002),
//                    new KeyValueTimestamp<>(3, "B3+k3", 1003));

//                // advance time to not join with existing data
//                // we omit above exiting data, even if it's still in the window
//                //
//                // push four items with incr.Asing timestamps to the other stream. the primary window is empty; this should produce no items
//                // w1 = {}
//                // w2 = {}
//                // -=> w1 = {}
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time = 2000L;
//                for (var i = 0; i < expectedKeys.Length; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "l" + expectedKeys[i], time + i));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items with larger timestamps to the primary stream; this should produce four items
//                // w1 = {}
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time = 2000L + 100L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "C" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "C0+l0", 2100),
//                    new KeyValueTimestamp<>(1, "C1+l1", 2100),
//                    new KeyValueTimestamp<>(2, "C2+l2", 2100),
//                    new KeyValueTimestamp<>(3, "C3+l3", 2100));

//                // push four items with incrcase timestamps to the primary stream; this should produce three items
//                // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100) }
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "D" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(1, "D1+l1", 2101),
//                    new KeyValueTimestamp<>(2, "D2+l2", 2101),
//                    new KeyValueTimestamp<>(3, "D3+l3", 2101));

//                // push four items with incrcase timestamps to the primary stream; this should produce two items
//                // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101) }
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "E" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "E2+l2", 2102),
//                    new KeyValueTimestamp<>(3, "E3+l3", 2102));

//                // push four items with incrcase timestamps to the primary stream; this should produce one item
//                // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102) }
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "F" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(3, "F3+l3", 2103));

//                // push four items with incrcase timestamps (now out of window) to the primary stream; this should produce no items
//                // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103) }
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "G" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items with smaller timestamps (before window) to the primary stream; this should produce no items
//                // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104) }
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
//                //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time = 2000L - 100L - 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "H" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items with incr.Ased timestamps to the primary stream; this should produce one item
//                // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
//                //        0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899) }
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
//                //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
//                //            0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "I" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "I0+l0", 2000));

//                // push four items with incr.Ased timestamps to the primary stream; this should produce two items
//                // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
//                //        0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
//                //        0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900) }
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
//                //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
//                //            0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
//                //            0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "J" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "J0+l0", 2000),
//                    new KeyValueTimestamp<>(1, "J1+l1", 2001));

//                // push four items with incr.Ased timestamps to the primary stream; this should produce three items
//                // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
//                //        0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
//                //        0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
//                //        0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901) }
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
//                //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
//                //            0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
//                //            0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901),
//                //            0:K0 (ts: 1902), 1:K1 (ts: 1902), 2:K2 (ts: 1902), 3:K3 (ts: 1902) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "K" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "K0+l0", 2000),
//                    new KeyValueTimestamp<>(1, "K1+l1", 2001),
//                    new KeyValueTimestamp<>(2, "K2+l2", 2002));

//                // push four items with incr.Ased timestamps to the primary stream; this should produce four items
//                // w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //        0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //        0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //        0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //        0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
//                //        0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
//                //        0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
//                //        0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901) }
//                //        0:K0 (ts: 1902), 1:K1 (ts: 1902), 2:K2 (ts: 1902), 3:K3 (ts: 1902) }
//                // w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                // -=> w1 = { 0:C0 (ts: 2100), 1:C1 (ts: 2100), 2:C2 (ts: 2100), 3:C3 (ts: 2100),
//                //            0:D0 (ts: 2101), 1:D1 (ts: 2101), 2:D2 (ts: 2101), 3:D3 (ts: 2101),
//                //            0:E0 (ts: 2102), 1:E1 (ts: 2102), 2:E2 (ts: 2102), 3:E3 (ts: 2102),
//                //            0:F0 (ts: 2103), 1:F1 (ts: 2103), 2:F2 (ts: 2103), 3:F3 (ts: 2103),
//                //            0:G0 (ts: 2104), 1:G1 (ts: 2104), 2:G2 (ts: 2104), 3:G3 (ts: 2104),
//                //            0:H0 (ts: 1899), 1:H1 (ts: 1899), 2:H2 (ts: 1899), 3:H3 (ts: 1899),
//                //            0:I0 (ts: 1900), 1:I1 (ts: 1900), 2:I2 (ts: 1900), 3:I3 (ts: 1900),
//                //            0:J0 (ts: 1901), 1:J1 (ts: 1901), 2:J2 (ts: 1901), 3:J3 (ts: 1901),
//                //            0:K0 (ts: 1902), 1:K1 (ts: 1902), 2:K2 (ts: 1902), 3:K3 (ts: 1902),
//                //            0:L0 (ts: 1903), 1:L1 (ts: 1903), 2:L2 (ts: 1903), 3:L3 (ts: 1903) }
//                //     w2 = { 0:l0 (ts: 2000), 1:l1 (ts: 2001), 2:l2 (ts: 2002), 3:l3 (ts: 2003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKey, "L" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "L0+l0", 2000),
//                    new KeyValueTimestamp<>(1, "L1+l1", 2001),
//                    new KeyValueTimestamp<>(2, "L2+l2", 2002),
//                    new KeyValueTimestamp<>(3, "L3+l3", 2003));
//            }
//    }

//        [Fact]
//        public void testAsymmetricWindowingAfter()
//        {
//            var builder = new StreamsBuilder();

//            var expectedKeys = new int[] { 0, 1, 2, 3 };

//            IKStream<int, string> stream1;
//            IKStream<int, string> stream2;
//            IKStream<int, string> joined;
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
//            stream1 = builder.Stream(topic1, consumed);
//            stream2 = builder.Stream(topic2, consumed);

//            joined = stream1.join(
//                stream2,
//                MockValueJoiner.TOSTRING_JOINER,
//                JoinWindows.of(Duration.FromMilliseconds(0)).after(Duration.FromMilliseconds(100)),
//                Joined.with(Serdes.Int(),
//                    Serdes.String(),
//                    Serdes.String()));
//            joined.process(supplier);

//            Collection<HashSet<string>> copartitionGroups =
//                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

//            Assert.Equal(1, copartitionGroups.Count);
//            Assert.Equal(new HashSet<>(new List<string> { topic1, topic2 }), copartitionGroups.iterator().MoveNext());

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                MockProcessor<int, string> processor = supplier.theCapturedProcessor();
//                var time = 1000L;

//                // push four items with incr.Asing timestamps to the primary stream; the other window is empty; this should produce no items
//                // w1 = {}
//                // w2 = {}
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = {}
//                for (var i = 0; i < expectedKeys.Length; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "A" + expectedKeys[i], time + i));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items smaller timestamps (out of window) to the secondary stream; this should produce no items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = {}
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999) }
//                time = 1000L - 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "a" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items with incr.Ased timestamps to the secondary stream; this should produce one item
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "b" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+b0", 1000));

//                // push four items with incr.Ased timestamps to the secondary stream; this should produce two items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "c" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+c0", 1001),
//                    new KeyValueTimestamp<>(1, "A1+c1", 1001));

//                // push four items with incr.Ased timestamps to the secondary stream; this should produce three items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "d" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+d0", 1002),
//                    new KeyValueTimestamp<>(1, "A1+d1", 1002),
//                    new KeyValueTimestamp<>(2, "A2+d2", 1002));

//                // push four items with incr.Ased timestamps to the secondary stream; this should produce four items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "e" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+e0", 1003),
//                    new KeyValueTimestamp<>(1, "A1+e1", 1003),
//                    new KeyValueTimestamp<>(2, "A2+e2", 1003),
//                    new KeyValueTimestamp<>(3, "A3+e3", 1003));

//                // push four items with larger timestamps to the secondary stream; this should produce four items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
//                //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100) }
//                time = 1000 + 100L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "f" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+f0", 1100),
//                    new KeyValueTimestamp<>(1, "A1+f1", 1100),
//                    new KeyValueTimestamp<>(2, "A2+f2", 1100),
//                    new KeyValueTimestamp<>(3, "A3+f3", 1100));

//                // push four items with incr.Ased timestamps to the secondary stream; this should produce three items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
//                //        0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
//                //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
//                //            0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "g" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(1, "A1+g1", 1101),
//                    new KeyValueTimestamp<>(2, "A2+g2", 1101),
//                    new KeyValueTimestamp<>(3, "A3+g3", 1101));

//                // push four items with incr.Ased timestamps to the secondary stream; this should produce two items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
//                //        0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
//                //        0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
//                //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
//                //            0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
//                //            0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "h" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "A2+h2", 1102),
//                    new KeyValueTimestamp<>(3, "A3+h3", 1102));

//                // push four items with incr.Ased timestamps to the secondary stream; this should produce one item
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
//                //        0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
//                //        0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
//                //        0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
//                //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
//                //            0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
//                //            0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102),
//                //            0:i0 (ts: 1103), 1:i1 (ts: 1103), 2:i2 (ts: 1103), 3:i3 (ts: 1103) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "i" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(3, "A3+i3", 1103));

//                // push four items with incr.Ased timestamps (no out of window) to the secondary stream; this should produce no items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //        0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //        0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //        0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //        0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
//                //        0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
//                //        0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
//                //        0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102),
//                //        0:i0 (ts: 1103), 1:i1 (ts: 1103), 2:i2 (ts: 1103), 3:i3 (ts: 1103) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 999), 1:a1 (ts: 999), 2:a2 (ts: 999), 3:a3 (ts: 999),
//                //            0:b0 (ts: 1000), 1:b1 (ts: 1000), 2:b2 (ts: 1000), 3:b3 (ts: 1000),
//                //            0:c0 (ts: 1001), 1:c1 (ts: 1001), 2:c2 (ts: 1001), 3:c3 (ts: 1001),
//                //            0:d0 (ts: 1002), 1:d1 (ts: 1002), 2:d2 (ts: 1002), 3:d3 (ts: 1002),
//                //            0:e0 (ts: 1003), 1:e1 (ts: 1003), 2:e2 (ts: 1003), 3:e3 (ts: 1003),
//                //            0:f0 (ts: 1100), 1:f1 (ts: 1100), 2:f2 (ts: 1100), 3:f3 (ts: 1100),
//                //            0:g0 (ts: 1101), 1:g1 (ts: 1101), 2:g2 (ts: 1101), 3:g3 (ts: 1101),
//                //            0:h0 (ts: 1102), 1:h1 (ts: 1102), 2:h2 (ts: 1102), 3:h3 (ts: 1102),
//                //            0:i0 (ts: 1103), 1:i1 (ts: 1103), 2:i2 (ts: 1103), 3:i3 (ts: 1103),
//                //            0:j0 (ts: 1104), 1:j1 (ts: 1104), 2:j2 (ts: 1104), 3:j3 (ts: 1104) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "j" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(EMPTY);
//            }
//    }

//        [Fact]
//        public void testAsymmetricWindowingBefore()
//        {
//            var builder = new StreamsBuilder();

//            var expectedKeys = new int[] { 0, 1, 2, 3 };

//            IKStream<int, string> stream1;
//            IKStream<int, string> stream2;
//            IKStream<int, string> joined;
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();

//            stream1 = builder.Stream(topic1, consumed);
//            stream2 = builder.Stream(topic2, consumed);

//            joined = stream1.join(
//                stream2,
//                MockValueJoiner.TOSTRING_JOINER,
//                JoinWindows.of(Duration.FromMilliseconds(0)).before(Duration.FromMilliseconds(100)),
//                Joined.with(Serdes.Int(), Serdes.String(), Serdes.String()));
//            joined.process(supplier);

//            Collection<HashSet<string>> copartitionGroups =
//                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

//            Assert.Equal(1, copartitionGroups.Count);
//            Assert.Equal(new HashSet<>(new List<string> { topic1, topic2 }), copartitionGroups.iterator().MoveNext());

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                MockProcessor<int, string> processor = supplier.theCapturedProcessor();
//                var time = 1000L;

//                // push four items with incr.Asing timestamps to the primary stream; the other window is empty; this should produce no items
//                // w1 = {}
//                // w2 = {}
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = {}
//                for (var i = 0; i < expectedKeys.Length; i++)
//                {
//                    driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "A" + expectedKeys[i], time + i));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items with smaller timestamps (before the window) to the other stream; this should produce no items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = {}
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899) }
//                time = 1000L - 100L - 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "a" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(EMPTY);

//                // push four items with incr.Ased timestamp to the other stream; this should produce one item
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "b" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+b0", 1000));

//                // push four items with incr.Ased timestamp to the other stream; this should produce two items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "c" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+c0", 1000),
//                    new KeyValueTimestamp<>(1, "A1+c1", 1001));

//                // push four items with incr.Ased timestamp to the other stream; this should produce three items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "d" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+d0", 1000),
//                    new KeyValueTimestamp<>(1, "A1+d1", 1001),
//                    new KeyValueTimestamp<>(2, "A2+d2", 1002));

//                // push four items with incr.Ased timestamp to the other stream; this should produce four items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "e" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+e0", 1000),
//                    new KeyValueTimestamp<>(1, "A1+e1", 1001),
//                    new KeyValueTimestamp<>(2, "A2+e2", 1002),
//                    new KeyValueTimestamp<>(3, "A3+e3", 1003));

//                // push four items with larger timestamp to the other stream; this should produce four items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
//                //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000) }
//                time = 1000L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "f" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+f0", 1000),
//                    new KeyValueTimestamp<>(1, "A1+f1", 1001),
//                    new KeyValueTimestamp<>(2, "A2+f2", 1002),
//                    new KeyValueTimestamp<>(3, "A3+f3", 1003));

//                // push four items with incrcase timestamp to the other stream; this should produce three items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
//                //        0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
//                //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
//                //            0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "g" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(1, "A1+g1", 1001),
//                    new KeyValueTimestamp<>(2, "A2+g2", 1002),
//                    new KeyValueTimestamp<>(3, "A3+g3", 1003));

//                // push four items with incrcase timestamp to the other stream; this should produce two items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
//                //        0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
//                //        0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
//                //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
//                //            0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
//                //            0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "h" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(2, "A2+h2", 1002),
//                    new KeyValueTimestamp<>(3, "A3+h3", 1003));

//                // push four items with incrcase timestamp to the other stream; this should produce one item
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
//                //        0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
//                //        0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
//                //        0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
//                //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
//                //            0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
//                //            0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002),
//                //            0:i0 (ts: 1003), 1:i1 (ts: 1003), 2:i2 (ts: 1003), 3:i3 (ts: 1003) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "i" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(new KeyValueTimestamp<>(3, "A3+i3", 1003));

//                // push four items with incrcase timestamp (no out of window) to the other stream; this should produce no items
//                // w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                // w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //        0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //        0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //        0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //        0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
//                //        0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
//                //        0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
//                //        0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002),
//                //        0:i0 (ts: 1003), 1:i1 (ts: 1003), 2:i2 (ts: 1003), 3:i3 (ts: 1003) }
//                // -=> w1 = { 0:A0 (ts: 1000), 1:A1 (ts: 1001), 2:A2 (ts: 1002), 3:A3 (ts: 1003) }
//                //     w2 = { 0:a0 (ts: 899), 1:a1 (ts: 899), 2:a2 (ts: 899), 3:a3 (ts: 899),
//                //            0:b0 (ts: 900), 1:b1 (ts: 900), 2:b2 (ts: 900), 3:b3 (ts: 900),
//                //            0:c0 (ts: 901), 1:c1 (ts: 901), 2:c2 (ts: 901), 3:c3 (ts: 901),
//                //            0:d0 (ts: 902), 1:d1 (ts: 902), 2:d2 (ts: 902), 3:d3 (ts: 902),
//                //            0:e0 (ts: 903), 1:e1 (ts: 903), 2:e2 (ts: 903), 3:e3 (ts: 903),
//                //            0:f0 (ts: 1000), 1:f1 (ts: 1000), 2:f2 (ts: 1000), 3:f3 (ts: 1000),
//                //            0:g0 (ts: 1001), 1:g1 (ts: 1001), 2:g2 (ts: 1001), 3:g3 (ts: 1001),
//                //            0:h0 (ts: 1002), 1:h1 (ts: 1002), 2:h2 (ts: 1002), 3:h3 (ts: 1002),
//                //            0:i0 (ts: 1003), 1:i1 (ts: 1003), 2:i2 (ts: 1003), 3:i3 (ts: 1003),
//                //            0:j0 (ts: 1004), 1:j1 (ts: 1004), 2:j2 (ts: 1004), 3:j3 (ts: 1004) }
//                time += 1L;
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(topic2, expectedKey, "j" + expectedKey, time));
//                }
//                processor.checkAndClearProcessResult(EMPTY);
//            }
//    }
//    }