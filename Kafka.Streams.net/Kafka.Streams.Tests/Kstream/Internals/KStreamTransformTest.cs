//using Kafka.Streams.Configs;
//using Kafka.Streams.KStream;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamTransformTest
//    {
//        private static string TOPIC_NAME = "topic";
//        private ConsumerRecordFactory<int, int> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.Int(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.Int());

//        [Fact]
//        public void testTransform()
//        {
//            var builder = new StreamsBuilder();

//            ITransformerSupplier<int, int, KeyValue<int, int>> transformerSupplier = null;
//            //            () => new Transformer<int, int, KeyValue<int, int>>()
//            //            {
//            //                private int total = 0;


//            //    public void init(IProcessorContext context)
//            //    {
//            //        context.schedule(
//            //            Duration.FromMilliseconds(1),
//            //            PunctuationType.WALL_CLOCK_TIME,
//            //            timestamp => context.forward(-1, (int)timestamp)
//            //        );
//            //    }


//            //    public KeyValue<int, int> transform(int key, int value)
//            //    {
//            //        total += value.intValue();
//            //        return KeyValue.Pair(key.intValue() * 2, total);
//            //    }


//            //    public void close() { }
//            //};

//            int[] expectedKeys = { 1, 10, 100, 1000 };

//            MockProcessorSupplier<int, int> processor = new MockProcessorSupplier<>();
//            IKStream<int, int> stream = builder.Stream(TOPIC_NAME, Consumed.with(Serdes.Int(), Serdes.Int()));
//            stream.transform(transformerSupplier).process(processor);


//            var driver = new TopologyTestDriver(
//                    builder.Build(),
//                    mkProperties(mkMap(
//                        mkEntry(StreamsConfigPropertyNames.BootstrapServers, "dummy"),
//                        mkEntry(StreamsConfigPropertyNames.ApplicationId, "test")
//                    )),
//                    0L)) {
//                ConsumerRecordFactory<int, int> recordFactory =
//                    new ConsumerRecordFactory<>(TOPIC_NAME, Serdes.Int(), Serdes.Int());

//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.pipeInput(recordFactory.create(expectedKey, expectedKey * 10, expectedKey / 2L));
//                }

//                driver.advanceWallClockTime(2);
//                driver.advanceWallClockTime(1);

//                KeyValueTimestamp[] expected = {
//                new KeyValueTimestamp<>(2, 10, 0),
//                new KeyValueTimestamp<>(20, 110, 5),
//                new KeyValueTimestamp<>(200, 1110, 50),
//                new KeyValueTimestamp<>(2000, 11110, 500),
//                new KeyValueTimestamp<>(-1, 2, 2),
//                new KeyValueTimestamp<>(-1, 3, 3)
//            };

//                Assert.Equal(expected.Length, processor.theCapturedProcessor().processed.size());
//                for (var i = 0; i < expected.Length; i++)
//                {
//                    Assert.Equal(expected[i], processor.theCapturedProcessor().processed.get(i));
//                }
//            }
//        }

//        [Fact]
//        public void testTransformWithNewDriverAndPunctuator()
//        {
//            var builder = new StreamsBuilder();

//            TransformerSupplier<int, int, KeyValue<int, int>> transformerSupplier =
//                //() => new Transformer<int, int, KeyValue<int, int>>()
//                //{
//                //private int total = 0;


//                //public void init(IProcessorContext context)
//                //{
//                //context.schedule(
//                //Duration.FromMilliseconds(1),
//                //PunctuationType.WALL_CLOCK_TIME,
//                //timestamp => context.forward(-1, (int)timestamp));
//                //}


//                //public KeyValue<int, int> transform(int key, int value)
//                //{
//                //total += value.intValue();
//                //return KeyValue.Pair(key.intValue() * 2, total);
//                //}


//                //public void close() { }
//                //};

//                int[] expectedKeys = { 1, 10, 100, 1000 };

//            MockProcessorSupplier<int, int> processor = new MockProcessorSupplier<>();
//            IKStream<int, int> stream = builder.Stream(TOPIC_NAME, Consumed.with(Serdes.Int(), Serdes.Int()));
//            stream.transform(transformerSupplier).process(processor);

//            var driver = new TopologyTestDriver(builder.Build(), props, 0L);
//            foreach (int expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(TOPIC_NAME, expectedKey, expectedKey * 10, 0L));

//                // This tick yields the "-1:2" result
//                driver.advanceWallClockTime(2);
//                // This tick further advances the clock to 3, which leads to the "-1:3" result
//                driver.advanceWallClockTime(1);
//            }

//            Assert.Equal(6, processor.theCapturedProcessor().processed.size());

//            KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(2, 10, 0),
//            new KeyValueTimestamp<>(20, 110, 0),
//            new KeyValueTimestamp<>(200, 1110, 0),
//            new KeyValueTimestamp<>(2000, 11110, 0),
//            new KeyValueTimestamp<>(-1, 2, 2),
//            new KeyValueTimestamp<>(-1, 3, 3)};

//            for (var i = 0; i < expected.Length; i++)
//            {
//                Assert.Equal(expected[i], processor.theCapturedProcessor().processed.get(i));
//            }
//        }
//    }
//}
