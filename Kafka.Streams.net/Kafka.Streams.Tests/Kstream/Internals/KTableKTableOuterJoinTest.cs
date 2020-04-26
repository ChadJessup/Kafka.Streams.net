using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Integration;
using Kafka.Streams.Tests.Mocks;
using Kafka.Streams.Topologies;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{

    public class KTableKTableOuterJoinTest
    {
        private string topic1 = "topic1";
        private string topic2 = "topic2";
        private string output = "output";
        private Consumed<int, string> consumed = Consumed.With(Serdes.Int(), Serdes.String());
        private ConsumerRecordFactory<int, string> recordFactory =
                new ConsumerRecordFactory<int, string>(Serdes.Int().Serializer, Serdes.String().Serializer, 0L);
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void testJoin()
        {
            var builder = new StreamsBuilder();

            var expectedKeys = new int[] { 0, 1, 2, 3 };

            IKTable<int, string> table1;
            IKTable<int, string> table2;
            IKTable<int, string> joined;

            table1 = builder.Table(topic1, consumed);
            table2 = builder.Table(topic2, consumed);
            joined = table1.OuterJoin(table2, MockValueJoiner.TOSTRING_JOINER);
            joined.ToStream().To(output);

            List<HashSet<string>> copartitionGroups =
                    TopologyWrapper.getInternalTopologyBuilder(builder.Build()).CopartitionGroups();

            Assert.Single(copartitionGroups);
            Assert.Equal(new HashSet<>(new List<string> { topic1, topic2 }), copartitionGroups.iterator().MoveNext());

            var driver = new TopologyTestDriver(builder.Build(), props);
            // push two items to the primary stream. the other table is empty
            for (var i = 0; i < 2; i++)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "X" + expectedKeys[i], 5L + i));
            }
            // .Ass tuple with null key, it will be discarded in join process
            driver.PipeInput(recordFactory.Create(topic1, null, "SomeVal", 42L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right:
            assertOutputKeyValueTimestamp(driver, 0, "X0+null", 5L);
            assertOutputKeyValueTimestamp(driver, 1, "X1+null", 6L);
            Assert.Null(driver.readOutput(output));

            // push two items to the other stream. this should produce two items.
            for (var i = 0; i < 2; i++)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "Y" + expectedKeys[i], 10L * i));
            }
            // .Ass tuple with null key, it will be discarded in join process
            driver.PipeInput(recordFactory.Create(topic2, null, "AnotherVal", 73L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            assertOutputKeyValueTimestamp(driver, 0, "X0+Y0", 5L);
            assertOutputKeyValueTimestamp(driver, 1, "X1+Y1", 10L);
            Assert.Null(driver.readOutput(output));

            // push All four items to the primary stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "XX" + expectedKey, 7L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            assertOutputKeyValueTimestamp(driver, 0, "XX0+Y0", 7L);
            assertOutputKeyValueTimestamp(driver, 1, "XX1+Y1", 10L);
            assertOutputKeyValueTimestamp(driver, 2, "XX2+null", 7L);
            assertOutputKeyValueTimestamp(driver, 3, "XX3+null", 7L);
            Assert.Null(driver.readOutput(output));

            // push All items to the other stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKey, "YY" + expectedKey, expectedKey * 5L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, "XX0+YY0", 7L);
            assertOutputKeyValueTimestamp(driver, 1, "XX1+YY1", 7L);
            assertOutputKeyValueTimestamp(driver, 2, "XX2+YY2", 10L);
            assertOutputKeyValueTimestamp(driver, 3, "XX3+YY3", 15L);
            Assert.Null(driver.readOutput(output));

            // push All four items to the primary stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "XXX" + expectedKey, 6L));
            }
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, "XXX0+YY0", 6L);
            assertOutputKeyValueTimestamp(driver, 1, "XXX1+YY1", 6L);
            assertOutputKeyValueTimestamp(driver, 2, "XXX2+YY2", 10L);
            assertOutputKeyValueTimestamp(driver, 3, "XXX3+YY3", 15L);
            Assert.Null(driver.readOutput(output));

            // push two items with null to the other stream.As deletes. this should produce two item.
            driver.PipeInput(recordFactory.Create(topic2, expectedKeys[0], null, 5L));
            driver.PipeInput(recordFactory.Create(topic2, expectedKeys[1], null, 7L));
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, "XXX0+null", 6L);
            assertOutputKeyValueTimestamp(driver, 1, "XXX1+null", 7L);
            Assert.Null(driver.readOutput(output));

            // push All four items to the primary stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "XXXX" + expectedKey, 13L));
            }
            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, "XXXX0+null", 13L);
            assertOutputKeyValueTimestamp(driver, 1, "XXXX1+null", 13L);
            assertOutputKeyValueTimestamp(driver, 2, "XXXX2+YY2", 13L);
            assertOutputKeyValueTimestamp(driver, 3, "XXXX3+YY3", 15L);
            Assert.Null(driver.readOutput(output));

            // push four items to the primary stream with null. this should produce four items.
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[0], null, 0L));
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[1], null, 42L));
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[2], null, 5L));
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[3], null, 20L));
            // left:
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, null, 0L);
            assertOutputKeyValueTimestamp(driver, 1, null, 42L);
            assertOutputKeyValueTimestamp(driver, 2, "null+YY2", 10L);
            assertOutputKeyValueTimestamp(driver, 3, "null+YY3", 20L);
            Assert.Null(driver.readOutput(output));
        }

        [Fact]
        public void testNotSendingOldValue()
        {
            var builder = new StreamsBuilder();

            var expectedKeys = new int[] { 0, 1, 2, 3 };

            IKTable<int, string> table1;
            IKTable<int, string> table2;
            IKTable<int, string> joined;
            MockProcessorSupplier<int, string> supplier;

            table1 = builder.Table(topic1, consumed);
            table2 = builder.Table(topic2, consumed);
            joined = table1.OuterJoin(table2, MockValueJoiner.TOSTRING_JOINER);

            supplier = new MockProcessorSupplier<int, string>();
            Topology topology = builder.Build().AddProcessor("proc", supplier, ((IKTable<object, object, object>)joined).Name);

            var driver = new TopologyTestDriver(topology, props);
            MockProcessor<int, string> proc = supplier.TheCapturedProcessor();

            Assert.True(((IKTable<object, object>)table1).SendingOldValueEnabled());
            Assert.True(((IKTable<object, object>)table2).SendingOldValueEnabled());
            Assert.False(((IKTable<object, object>)joined).SendingOldValueEnabled());

            // push two items to the primary stream. the other table is empty
            for (var i = 0; i < 2; i++)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "X" + expectedKeys[i], 5L + i));
            }
            // .Ass tuple with null key, it will be discarded in join process
            driver.PipeInput(recordFactory.Create(topic1, null, "SomeVal", 42L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right:
            proc.CheckAndClearProcessResult(new KeyValueTimestamp<int, string>(0, new Change<string>("X0+null", null), 5),
                    new KeyValueTimestamp<int, string>(1, new Change<string>("X1+null", null), 6));
            // push two items to the other stream. this should produce two items.
            for (var i = 0; i < 2; i++)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "Y" + expectedKeys[i], 10L * i));
            }
            // .Ass tuple with null key, it will be discarded in join process
            driver.PipeInput(recordFactory.Create(topic2, null, "AnotherVal", 73L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("X0+Y0", null), 5),
                new KeyValueTimestamp<int, string>(1, new Change<string>("X1+Y1", null), 10));

            // push All four items to the primary stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "XX" + expectedKey, 7L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XX0+Y0", null), 7),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XX1+Y1", null), 10),
                new KeyValueTimestamp<int, string>(2, new Change<string>("XX2+null", null), 7),
                new KeyValueTimestamp<int, string>(3, new Change<string>("XX3+null", null), 7));
            // push All items to the other stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKey, "YY" + expectedKey, expectedKey * 5L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XX0+YY0", null), 7),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XX1+YY1", null), 7),
                new KeyValueTimestamp<int, string>(2, new Change<string>("XX2+YY2", null), 10),
                new KeyValueTimestamp<int, string>(3, new Change<string>("XX3+YY3", null), 15));
            // push All four items to the primary stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "XXX" + expectedKey, 6L));
            }
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XXX0+YY0", null), 6),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XXX1+YY1", null), 6),
                new KeyValueTimestamp<int, string>(2, new Change<string>("XXX2+YY2", null), 10),
                new KeyValueTimestamp<int, string>(3, new Change<string>("XXX3+YY3", null), 15));
            // push two items with null to the other stream.As deletes. this should produce two item.
            driver.PipeInput(recordFactory.Create(topic2, expectedKeys[0], null, 5L));
            driver.PipeInput(recordFactory.Create(topic2, expectedKeys[1], null, 7L));
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XXX0+null", null), 6),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XXX1+null", null), 7));

            // push All four items to the primary stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "XXXX" + expectedKey, 13L));
            }
            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XXXX0+null", null), 13),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XXXX1+null", null), 13),
                new KeyValueTimestamp<int, string>(2, new Change<string>("XXXX2+YY2", null), 13),
                new KeyValueTimestamp<int, string>(3, new Change<string>("XXXX3+YY3", null), 15));
            // push four items to the primary stream with null. this should produce four items.
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[0], null, 0L));
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[1], null, 42L));
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[2], null, 5L));
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[3], null, 20L));
            // left:
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>(null, null), 0),
                new KeyValueTimestamp<int, string>(1, new Change<string>(null, null), 42),
                new KeyValueTimestamp<int, string>(2, new Change<string>("null+YY2", null), 10),
                new KeyValueTimestamp<int, string>(3, new Change<string>("null+YY3", null), 20));
        }

        [Fact]
        public void testSendingOldValue()
        {
            var builder = new StreamsBuilder();

            var expectedKeys = new int[] { 0, 1, 2, 3 };

            IKTable<int, string> table1;
            IKTable<int, string> table2;
            IKTable<int, string> joined;
            MockProcessorSupplier<int, string> supplier;

            table1 = builder.Table(topic1, consumed);
            table2 = builder.Table(topic2, consumed);
            joined = table1.OuterJoin(table2, MockValueJoiner.TOSTRING_JOINER);

            ((IKTable<object, object>)joined).EnableSendingOldValues();

            supplier = new MockProcessorSupplier<>();
            Topology topology = builder.Build().AddProcessor("proc", supplier, ((IKTable<object, object>)joined).Name);

            var driver = new TopologyTestDriver(topology, props);
            MockProcessor<int, string> proc = supplier.TheCapturedProcessor();

            Assert.True(((IKTable<object, object>)table1).SendingOldValueEnabled());
            Assert.True(((IKTable<object, object>)table2).SendingOldValueEnabled());
            Assert.True(((IKTable<object, object>)joined).SendingOldValueEnabled());

            // push two items to the primary stream. the other table is empty
            for (var i = 0; i < 2; i++)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKeys[i], "X" + expectedKeys[i], 5L + i));
            }
            // .Ass tuple with null key, it will be discarded in join process
            driver.PipeInput(recordFactory.Create(topic1, null, "SomeVal", 42L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right:
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("X0+null", null), 5),
                new KeyValueTimestamp<int, string>(1, new Change<string>("X1+null", null), 6));

            // push two items to the other stream. this should produce two items.
            for (var i = 0; i < 2; i++)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKeys[i], "Y" + expectedKeys[i], 10L * i));
            }
            // .Ass tuple with null key, it will be discarded in join process
            driver.PipeInput(recordFactory.Create(topic2, null, "AnotherVal", 73L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.CheckAndClearProcessResult(new KeyValueTimestamp<string, string>(0, new Change<string>("X0+Y0", "X0+null"), 5),
                    new KeyValueTimestamp<string, string>(1, new Change<string>("X1+Y1", "X1+null"), 10));
            // push All four items to the primary stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "XX" + expectedKey, 7L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XX0+Y0", "X0+Y0"), 7),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XX1+Y1", "X1+Y1"), 10),
                new KeyValueTimestamp<int, string>(2, new Change<string>("XX2+null", null), 7),
                new KeyValueTimestamp<int, string>(3, new Change<string>("XX3+null", null), 7));

            // push All items to the other stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic2, expectedKey, "YY" + expectedKey, expectedKey * 5L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XX0+YY0", "XX0+Y0"), 7),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XX1+YY1", "XX1+Y1"), 7),
                new KeyValueTimestamp<int, string>(2, new Change<string>("XX2+YY2", "XX2+null"), 10),
                new KeyValueTimestamp<int, string>(3, new Change<string>("XX3+YY3", "XX3+null"), 15));

            // push All four items to the primary stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "XXX" + expectedKey, 6L));
            }
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XXX0+YY0", "XX0+YY0"), 6),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XXX1+YY1", "XX1+YY1"), 6),
                new KeyValueTimestamp<int, string>(2, new Change<string>("XXX2+YY2", "XX2+YY2"), 10),
                new KeyValueTimestamp<int, string>(3, new Change<string>("XXX3+YY3", "XX3+YY3"), 15));

            // push two items with null to the other stream.As deletes. this should produce two item.
            driver.PipeInput(recordFactory.Create(topic2, expectedKeys[0], null, 5L));
            driver.PipeInput(recordFactory.Create(topic2, expectedKeys[1], null, 7L));
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XXX0+null", "XXX0+YY0"), 6),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XXX1+null", "XXX1+YY1"), 7));
            // push All four items to the primary stream. this should produce four items.
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topic1, expectedKey, "XXXX" + expectedKey, 13L));
            }
            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>("XXXX0+null", "XXX0+null"), 13),
                new KeyValueTimestamp<int, string>(1, new Change<string>("XXXX1+null", "XXX1+null"), 13),
                new KeyValueTimestamp<int, string>(2, new Change<string>("XXXX2+YY2", "XXX2+YY2"), 13),
                new KeyValueTimestamp<int, string>(3, new Change<string>("XXXX3+YY3", "XXX3+YY3"), 15));
            // push four items to the primary stream with null. this should produce four items.
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[0], null, 0L));
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[1], null, 42L));
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[2], null, 5L));
            driver.PipeInput(recordFactory.Create(topic1, expectedKeys[3], null, 20L));
            // left:
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, new Change<string>(null, "XXXX0+null"), 0),
                new KeyValueTimestamp<int, string>(1, new Change<string>(null, "XXXX1+null"), 42),
                new KeyValueTimestamp<int, string>(2, new Change<string>("null+YY2", "XXXX2+YY2"), 10),
                new KeyValueTimestamp<int, string>(3, new Change<string>("null+YY3", "XXXX3+YY3"), 20));
        }

        [Fact]
        public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey()
        {
            var builder = new StreamsBuilder();

            var join = new KTableKTableOuterJoin<string, Change<string>>(
                    (IKTable<string, string>)builder.Table("left", Consumed.With(Serdes.String(), Serdes.String())),
                    (IKTable<string, string>)builder.Table("right", Consumed.With(Serdes.String(), Serdes.String())),
                    null
            ).Get();

            var context = new MockProcessorContext();
            context.SetRecordMetadata("left", -1, -2, null, -3);
            join.Init(context);
            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
            join.Process(null, new Change<string>("new", "old"));
            LogCaptureAppender.unregister(appender);

            // Assert.Equal(1.0, getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
            // Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key. change=[(new<-old)] topic=[left] partition=[-1] offset=[-2]"));
        }

        private void assertOutputKeyValueTimestamp(
            TopologyTestDriver driver,
            int expectedKey,
            string expectedValue,
            long expectedTimestamp)
        {
            OutputVerifier.compareKeyValueTimestamp(
                    driver.readOutput(output, Serdes.Int().Deserializer, Serdes.String().Deserializer),
                    expectedKey,
                    expectedValue,
                    expectedTimestamp);
        }
    }
}
