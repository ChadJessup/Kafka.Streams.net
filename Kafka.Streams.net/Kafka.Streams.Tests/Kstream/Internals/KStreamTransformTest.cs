//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream.Internals
//{
//    public class KStreamTransformTest
//    {
//        private static string TOPIC_NAME = "topic";
//        private ConsumerRecordFactory<int, int> recordFactory =
//            new ConsumerRecordFactory<int, int>(Serdes.Int(), Serdes.Int(), 0L);

//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.Int());

//        [Fact]
//        public void testTransform()
//        {
//            var builder = new StreamsBuilder();

//            ITransformerSupplier<int, int, KeyValuePair<int, int>> transformerSupplier = null;
//            //            () => new Transformer<int, int, KeyValuePair<int, int>>()
//            //            {
//            //                private int total = 0;


//            //    public void Init(IProcessorContext context)
//            //    {
//            //        context.schedule(
//            //            TimeSpan.FromMilliseconds(1),
//            //            PunctuationType.WALL_CLOCK_TIME,
//            //            timestamp => context.Forward(-1, (int)timestamp)
//            //        );
//            //    }


//            //    public KeyValuePair<int, int> transform(int key, int value)
//            //    {
//            //        total += value.intValue();
//            //        return KeyValuePair.Create(key.intValue() * 2, total);
//            //    }


//            //    public void Close() { }
//            //};

//            int[] expectedKeys = { 1, 10, 100, 1000 };

//            MockProcessorSupplier<int, int> processor = new MockProcessorSupplier<int, int>();
//            IKStream<int, int> stream = builder.Stream(TOPIC_NAME, Consumed.With(Serdes.Int(), Serdes.Int()));
//            stream.Transform(transformerSupplier).Process(processor);

//            var driver = new TopologyTestDriver(
//                    builder.Build(),
//                    mkProperties(mkMap(
//                        mkEntry(StreamsConfig.BootstrapServers, "dummy"),
//                        mkEntry(StreamsConfig.ApplicationId, "test")
//                    )),
//                    0L)) {
//                ConsumerRecordFactory<int, int> recordFactory =
//                    new ConsumerRecordFactory<>(TOPIC_NAME, Serdes.Int(), Serdes.Int());

//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.PipeInput(recordFactory.Create(expectedKey, expectedKey * 10, expectedKey / 2L));
//                }

//                driver.AdvanceWallClockTime(2);
//                driver.AdvanceWallClockTime(1);

//                var expected = new KeyValueTimestamp<int, int>[]
//                {
//                    new KeyValueTimestamp<int, int>(2, 10, 0),
//                    new KeyValueTimestamp<int, int>(20, 110, 5),
//                    new KeyValueTimestamp<int, int>(200, 1110, 50),
//                    new KeyValueTimestamp<int, int>(2000, 11110, 500),
//                    new KeyValueTimestamp<int, int>(-1, 2, 2),
//                    new KeyValueTimestamp<int, int>(-1, 3, 3)
//                };

//                Assert.Equal(expected.Length, processor.TheCapturedProcessor().processed.Count);
//                for (var i = 0; i < expected.Length; i++)
//                {
//                    Assert.Equal(expected[i], processor.TheCapturedProcessor().processed.Get(i));
//                }
//            }
//        }

//        [Fact]
//        public void testTransformWithNewDriverAndPunctuator()
//        {
//            var builder = new StreamsBuilder();

//            //TransformerSupplier<int, int, IKeyValuePair<int, int>> transformerSupplier =
//            //() => new Transformer<int, int, KeyValuePair<int, int>>()
//            //{
//            //private int total = 0;


//            //public void Init(IProcessorContext context)
//            //{
//            //context.schedule(
//            //TimeSpan.FromMilliseconds(1),
//            //PunctuationType.WALL_CLOCK_TIME,
//            //timestamp => context.Forward(-1, (int)timestamp));
//            //}


//            //public KeyValuePair<int, int> transform(int key, int value)
//            //{
//            //total += value.intValue();
//            //return KeyValuePair.Create(key.intValue() * 2, total);
//            //}


//            //public void Close() { }
//            //};

//            int[] expectedKeys = new[] { 1, 10, 100, 1000 };

//            MockProcessorSupplier<int, int> processor = new MockProcessorSupplier<int, int>();
//            IKStream<int, int> stream = builder.Stream(TOPIC_NAME, Consumed.With(Serdes.Int(), Serdes.Int()));
//            stream.transform(transformerSupplier).Process(processor);

//            var driver = new TopologyTestDriver(builder.Build(), props, 0L);
//            foreach (int expectedKey in expectedKeys)
//            {
//                driver.PipeInput(recordFactory.Create(TOPIC_NAME, expectedKey, expectedKey * 10, 0L));

//                // This tick yields the "-1:2" result
//                driver.AdvanceWallClockTime(2);
//                // This tick further advances the clock to 3, which leads to the "-1:3" result
//                driver.AdvanceWallClockTime(1);
//            }

//            Assert.Equal(6, processor.TheCapturedProcessor().processed.Count);

//            var expected = new KeyValueTimestamp<int, int>[]
//            {
//                new KeyValueTimestamp<int, int>(2, 10, 0),
//                new KeyValueTimestamp<int, int>(20, 110, 0),
//                new KeyValueTimestamp<int, int>(200, 1110, 0),
//                new KeyValueTimestamp<int, int>(2000, 11110, 0),
//                new KeyValueTimestamp<int, int>(-1, 2, 2),
//                new KeyValueTimestamp<int, int>(-1, 3, 3),
//            };

//            for (var i = 0; i < expected.Length; i++)
//            {
//                Assert.Equal(expected[i], processor.TheCapturedProcessor().processed.Get(i));
//            }
//        }
//    }
//}
