using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamKStreamLeftJoinTest
    {
        private static KeyValueTimestamp[] EMPTY = System.Array.Empty<Streams.KeyValueTimestamp>();

        private string topic1 = "topic1";
        private string topic2 = "topic2";
        private Consumed<int, string> consumed = Consumed.With(Serdes.Int(), Serdes.String());
        private ConsumerRecordFactory<int, string> recordFactory =
            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String(), 0L);
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

        [Fact]
        public void testLeftJoin()
        {
            var builder = new StreamsBuilder();

            var expectedKeys = new int[] { 0, 1, 2, 3 };

            IKStream<K, V> stream1;
            IKStream<K, V> stream2;
            IKStream<K, V> joined;
            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
            stream1 = builder.Stream(topic1, consumed);
            stream2 = builder.Stream(topic2, consumed);

            joined = stream1.LeftJoin(
                stream2,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.Of(TimeSpan.FromMilliseconds(100)),
                Joined.With(Serdes.Int(), Serdes.String(), Serdes.String()));
            joined.Process(supplier);

            Collection<HashSet<string>> copartitionGroups =
                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

            Assert.Equal(1, copartitionGroups.Count);
            Assert.Equal(new HashSet<>(new List<string> { topic1, topic2 }), copartitionGroups.iterator().MoveNext());

            var driver = new TopologyTestDriver(builder.Build(), props);
            MockProcessor<int, string> processor = supplier.TheCapturedProcessor();

            // push two items to the primary stream; the other window is empty
            // w1 {}
            // w2 {}
            // -=> w1 = { 0:A0, 1:A1 }
            // -=> w2 = {}
            for (var i = 0; i < 2; i++)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "A" + expectedKeys[i]));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+null", 0),
                new KeyValueTimestamp<>(1, "A1+null", 0));
            // push two items to the other stream; this should produce two items
            // w1 = { 0:A0, 1:A1 }
            // w2 {}
            // -=> w1 = { 0:A0, 1:A1 }
            // -=> w2 = { 0:a0, 1:a1 }
            for (var i = 0; i < 2; i++)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "a" + expectedKeys[i]));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+a0", 0),
                new KeyValueTimestamp<>(1, "A1+a1", 0));
            // push three items to the primary stream; this should produce four items
            // w1 = { 0:A0, 1:A1 }
            // w2 = { 0:a0, 1:a1 }
            // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // -=> w2 = { 0:a0, 1:a1 }
            for (var i = 0; i < 3; i++)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "B" + expectedKeys[i]));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+a0", 0),
                new KeyValueTimestamp<>(1, "B1+a1", 0),
                new KeyValueTimestamp<>(2, "B2+null", 0));
            // push All items to the other stream; this should produce five items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // w2 = { 0:a0, 1:a1 }
            // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // -=> w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKey, "b" + expectedKey));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+b0", 0),
                new KeyValueTimestamp<>(0, "B0+b0", 0),
                new KeyValueTimestamp<>(1, "A1+b1", 0),
                new KeyValueTimestamp<>(1, "B1+b1", 0),
                new KeyValueTimestamp<>(2, "B2+b2", 0));
            // push All four items to the primary stream; this should produce six items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            // -=> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 0:C0, 1:C1, 2:C2, 3:C3 }
            // -=> w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "C" + expectedKey));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "C0+a0", 0),
                new KeyValueTimestamp<>(0, "C0+b0", 0),
                new KeyValueTimestamp<>(1, "C1+a1", 0),
                new KeyValueTimestamp<>(1, "C1+b1", 0),
                new KeyValueTimestamp<>(2, "C2+b2", 0),
                new KeyValueTimestamp<>(3, "C3+b3", 0));
        }

        [Fact]
        public void testWindowing()
        {
            var builder = new StreamsBuilder();
            var expectedKeys = new int[] { 0, 1, 2, 3 };

            IKStream<K, V> stream1;
            IKStream<K, V> stream2;
            IKStream<K, V> joined;
            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
            stream1 = builder.Stream(topic1, consumed);
            stream2 = builder.Stream(topic2, consumed);

            joined = stream1.LeftJoin(
                stream2,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.Of(TimeSpan.FromMilliseconds(100)),
                Joined.With(Serdes.Int(), Serdes.String(), Serdes.String()));
            joined.Process(supplier);

            Collection<HashSet<string>> copartitionGroups =
                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

            Assert.Equal(1, copartitionGroups.Count);
            Assert.Equal(new HashSet<>(new List<string> { topic1, topic2 }), copartitionGroups.iterator().MoveNext());

            var driver = new TopologyTestDriver(builder.Build(), props);
            MockProcessor<int, string> processor = supplier.TheCapturedProcessor();
            var time = 0L;

            // push two items to the primary stream; the other window is empty; this should produce two left-join items
            // w1 = {}
            // w2 = {}
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // -=> w2 = {}
            for (var i = 0; i < 2; i++)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "A" + expectedKeys[i], time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+null", 0),
                new KeyValueTimestamp<>(1, "A1+null", 0));
            // push four items to the other stream; this should produce two full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = {}
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0) }
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKey, "a" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "A0+a0", 0),
                new KeyValueTimestamp<>(1, "A1+a1", 0));
            testUpperWindowBound(expectedKeys, driver, processor);
            testLowerWindowBound(expectedKeys, driver, processor);
        }

        private void testUpperWindowBound(int[] expectedKeys,
                                          TopologyTestDriver driver,
                                          MockProcessor<int, string> processor)
        {
            long time;

            // push four items with larger and incr.Asing timestamp (out of window) to the other stream; this should produce no items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time = 1000L;
            for (var i = 0; i < expectedKeys.Length; i++)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "b" + expectedKeys[i], time + i));
            }
            processor.CheckAndClearProcessResult(EMPTY);

            // push four items with larger timestamp to the primary stream; this should produce four full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time = 1000L + 100L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "B" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "B0+b0", 1100),
                new KeyValueTimestamp<>(1, "B1+b1", 1100),
                new KeyValueTimestamp<>(2, "B2+b2", 1100),
                new KeyValueTimestamp<>(3, "B3+b3", 1100));
            // push four items with incr.Ased timestamp to the primary stream; this should produce one left-join and three full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time += 1L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "C" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "C0+null", 1101),
                new KeyValueTimestamp<>(1, "C1+b1", 1101),
                new KeyValueTimestamp<>(2, "C2+b2", 1101),
                new KeyValueTimestamp<>(3, "C3+b3", 1101));
            // push four items with incr.Ased timestamp to the primary stream; this should produce two left-join and two full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time += 1L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "D" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "D0+null", 1102),
                new KeyValueTimestamp<>(1, "D1+null", 1102),
                new KeyValueTimestamp<>(2, "D2+b2", 1102),
                new KeyValueTimestamp<>(3, "D3+b3", 1102));
            // push four items with incr.Ased timestamp to the primary stream; this should produce three left-join and one full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time += 1L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "E" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "E0+null", 1103),
                new KeyValueTimestamp<>(1, "E1+null", 1103),
                new KeyValueTimestamp<>(2, "E2+null", 1103),
                new KeyValueTimestamp<>(3, "E3+b3", 1103));
            // push four items with incr.Ased timestamp to the primary stream; this should produce four left-join and no full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time += 1L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "F" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "F0+null", 1104),
                new KeyValueTimestamp<>(1, "F1+null", 1104),
                new KeyValueTimestamp<>(2, "F2+null", 1104),
                new KeyValueTimestamp<>(3, "F3+null", 1104));
        }

        private void testLowerWindowBound(int[] expectedKeys,
                                          var driver,
                                          MockProcessor<int, string> processor)
        {
            long time;
            // push four items with smaller timestamp (before the window) to the primary stream; this should produce four left-join and no full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
            //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time = 1000L - 100L - 1L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "G" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "G0+null", 899),
                new KeyValueTimestamp<>(1, "G1+null", 899), new KeyValueTimestamp<>(2, "G2+null", 899),
                new KeyValueTimestamp<>(3, "G3+null", 899));
            // push four items with incrcase timestamp to the primary stream; this should produce three left-join and one full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
            //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
            //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
            //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time += 1L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "H" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "H0+b0", 1000),
                new KeyValueTimestamp<>(1, "H1+null", 900), new KeyValueTimestamp<>(2, "H2+null", 900),
                new KeyValueTimestamp<>(3, "H3+null", 900));
            // push four items with incrcase timestamp to the primary stream; this should produce two left-join and two full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
            //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
            //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
            //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
            //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
            //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time += 1L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "I" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "I0+b0", 1000),
                new KeyValueTimestamp<>(1, "I1+b1", 1001), new KeyValueTimestamp<>(2, "I2+null", 901),
                new KeyValueTimestamp<>(3, "I3+null", 901));
            // push four items with incrcase timestamp to the primary stream; this should produce one left-join and three full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
            //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
            //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
            //        0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
            //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
            //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
            //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
            //            0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time += 1L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "J" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "J0+b0", 1000),
                new KeyValueTimestamp<>(1, "J1+b1", 1001), new KeyValueTimestamp<>(2, "J2+b2", 1002),
                new KeyValueTimestamp<>(3, "J3+null", 902));
            // push four items with incrcase timestamp to the primary stream; this should produce one left-join and three full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
            //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
            //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
            //        0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
            //        0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902) }
            // w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //        0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            // -=> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
            //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
            //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
            //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
            //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
            //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
            //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
            //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
            //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
            //            0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902),
            //            0:K0 (ts: 903), 1:K1 (ts: 903), 2:K2 (ts: 903), 3:K3 (ts: 903) }
            // -=> w2 = { 0:a0 (ts: 0), 1:a1 (ts: 0), 2:a2 (ts: 0), 3:a3 (ts: 0),
            //            0:b0 (ts: 1000), 1:b1 (ts: 1001), 2:b2 (ts: 1002), 3:b3 (ts: 1003) }
            time += 1L;
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "K" + expectedKey, time));
            }
            processor.CheckAndClearProcessResult(new KeyValueTimestamp<>(0, "K0+b0", 1000),
                new KeyValueTimestamp<>(1, "K1+b1", 1001), new KeyValueTimestamp<>(2, "K2+b2", 1002),
                new KeyValueTimestamp<>(3, "K3+b3", 1003));
        }
    }
