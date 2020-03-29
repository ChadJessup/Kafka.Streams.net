//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.Tests.Mocks;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableLeftJoinTest
//    {
//        private string topic1 = "topic1";
//        private string topic2 = "topic2";
//        private string output = "output";
//        private Consumed<int, string> consumed = Consumed.with(Serdes.Int(), Serdes.String());
//        private ConsumerRecordFactory<int, string> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int().Serializer, Serdes.String().Serializer, 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

//        [Fact]
//        public void testJoin()
//        {
//            var builder = new StreamsBuilder();

//            var expectedKeys = new int[] { 0, 1, 2, 3 };

//            IKTable<int, string> table1 = builder.Table(topic1, consumed);
//            IKTable<int, string> table2 = builder.Table(topic2, consumed);
//            IKTable<int, string> joined = table1.leftJoin(table2, MockValueJoiner.TOSTRING_JOINER);
//            joined.toStream().to(output);

//            Collection<HashSet<string>> copartitionGroups =
//                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).copartitionGroups();

//            Assert.Equal(1, copartitionGroups.Count);
//            Assert.Equal(new HashSet<>(new List<string> { topic1, topic2 }), copartitionGroups.iterator().next());

//            var driver = new TopologyTestDriver(builder.Build(), props);
//            // push two items to the primary stream. the other table is empty
//            for (var i = 0; i < 2; i++)
//            {
//                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], 5L + i));
//            }
//            // .Ass tuple with null key, it will be discarded in join process
//            driver.pipeInput(recordFactory.create(topic1, null, "SomeVal", 42L));
//            // left: X0:0 (ts: 5), X1:1 (ts: 6)
//            // right:
//            assertOutputKeyValueTimestamp(driver, 0, "X0+null", 5L);
//            assertOutputKeyValueTimestamp(driver, 1, "X1+null", 6L);
//            Assert.Null(driver.readOutput(output));

//            // push two items to the other stream. this should produce two items.
//            for (var i = 0; i < 2; i++)
//            {
//                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i], 10L * i));
//            }
//            // .Ass tuple with null key, it will be discarded in join process
//            driver.pipeInput(recordFactory.create(topic2, null, "AnotherVal", 73L));
//            // left: X0:0 (ts: 5), X1:1 (ts: 6)
//            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
//            assertOutputKeyValueTimestamp(driver, 0, "X0+Y0", 5L);
//            assertOutputKeyValueTimestamp(driver, 1, "X1+Y1", 10L);
//            Assert.Null(driver.readOutput(output));

//            // push all four items to the primary stream. this should produce four items.
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, 7L));
//            }
//            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
//            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
//            assertOutputKeyValueTimestamp(driver, 0, "XX0+Y0", 7L);
//            assertOutputKeyValueTimestamp(driver, 1, "XX1+Y1", 10L);
//            assertOutputKeyValueTimestamp(driver, 2, "XX2+null", 7L);
//            assertOutputKeyValueTimestamp(driver, 3, "XX3+null", 7L);
//            Assert.Null(driver.readOutput(output));

//            // push all items to the other stream. this should produce four items.
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, expectedKey * 5L));
//            }
//            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
//            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
//            assertOutputKeyValueTimestamp(driver, 0, "XX0+YY0", 7L);
//            assertOutputKeyValueTimestamp(driver, 1, "XX1+YY1", 7L);
//            assertOutputKeyValueTimestamp(driver, 2, "XX2+YY2", 10L);
//            assertOutputKeyValueTimestamp(driver, 3, "XX3+YY3", 15L);
//            Assert.Null(driver.readOutput(output));

//            // push all four items to the primary stream. this should produce four items.
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXX" + expectedKey, 6L));
//            }
//            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
//            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
//            assertOutputKeyValueTimestamp(driver, 0, "XXX0+YY0", 6L);
//            assertOutputKeyValueTimestamp(driver, 1, "XXX1+YY1", 6L);
//            assertOutputKeyValueTimestamp(driver, 2, "XXX2+YY2", 10L);
//            assertOutputKeyValueTimestamp(driver, 3, "XXX3+YY3", 15L);
//            Assert.Null(driver.readOutput(output));

//            // push two items with null to the other stream.As deletes. this should produce two item.
//            driver.pipeInput(recordFactory.create(topic2, expectedKeys[0], null, 5L));
//            driver.pipeInput(recordFactory.create(topic2, expectedKeys[1], null, 7L));
//            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
//            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
//            assertOutputKeyValueTimestamp(driver, 0, "XXX0+null", 6L);
//            assertOutputKeyValueTimestamp(driver, 1, "XXX1+null", 7L);
//            Assert.Null(driver.readOutput(output));

//            // push all four items to the primary stream. this should produce four items.
//            foreach (var expectedKey in expectedKeys)
//            {
//                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXXX" + expectedKey, 13L));
//            }
//            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
//            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
//            assertOutputKeyValueTimestamp(driver, 0, "XXXX0+null", 13L);
//            assertOutputKeyValueTimestamp(driver, 1, "XXXX1+null", 13L);
//            assertOutputKeyValueTimestamp(driver, 2, "XXXX2+YY2", 13L);
//            assertOutputKeyValueTimestamp(driver, 3, "XXXX3+YY3", 15L);
//            Assert.Null(driver.readOutput(output));

//            // push three items to the primary stream with null. this should produce four items.
//            driver.pipeInput(recordFactory.create(topic1, expectedKeys[0], null, 0L));
//            driver.pipeInput(recordFactory.create(topic1, expectedKeys[1], null, 42L));
//            driver.pipeInput(recordFactory.create(topic1, expectedKeys[2], null, 5L));
//            driver.pipeInput(recordFactory.create(topic1, expectedKeys[3], null, 20L));
//            // left:
//            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
//            assertOutputKeyValueTimestamp(driver, 0, null, 0L);
//            assertOutputKeyValueTimestamp(driver, 1, null, 42L);
//            assertOutputKeyValueTimestamp(driver, 2, null, 10L);
//            assertOutputKeyValueTimestamp(driver, 3, null, 20L);
//            Assert.Null(driver.readOutput(output));
//        }

//        [Fact]
//        public void testNotSendingOldValue()
//        {
//            var builder = new StreamsBuilder();

//            var expectedKeys = new int[] { 0, 1, 2, 3 };

//            IKTable<int, string> table1;
//            IKTable<int, string> table2;
//            IKTable<int, string> joined;
//            MockProcessorSupplier<int, string> supplier;

//            table1 = builder.Table(topic1, consumed);
//            table2 = builder.Table(topic2, consumed);
//            joined = table1.leftJoin(table2, MockValueJoiner.TOSTRING_JOINER);

//            supplier = new MockProcessorSupplier<>();
//            Topology topology = builder.Build().addProcessor("proc", supplier, ((IKTable<object, object, object>)joined).name);

//            try
//            {
//                var driver = new TopologyTestDriver(topology, props);
//                MockProcessor<int, string> proc = supplier.theCapturedProcessor();

//                Assert.True(((IKTable<object, object, object>)table1).sendingOldValueEnabled());
//                Assert.False(((IKTable<object, object, object>)table2).sendingOldValueEnabled());
//                Assert.False(((IKTable<object, object, object>)joined).sendingOldValueEnabled());

//                // push two items to the primary stream. the other table is empty
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], 5L + i));
//                }
//                // .Ass tuple with null key, it will be discarded in join process
//                driver.pipeInput(recordFactory.create(topic1, null, "SomeVal", 42L));
//                // left: X0:0 (ts: 5), X1:1 (ts: 6)
//                // right:
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+null", null), 5),
//                    new KeyValueTimestamp<>(1, new Change<>("X1+null", null), 6));

//                // push two items to the other stream. this should produce two items.
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i], 10L * i));
//                }
//                // .Ass tuple with null key, it will be discarded in join process
//                driver.pipeInput(recordFactory.create(topic2, null, "AnotherVal", 73L));
//                // left: X0:0 (ts: 5), X1:1 (ts: 6)
//                // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+Y0", null), 5),
//                    new KeyValueTimestamp<>(1, new Change<>("X1+Y1", null), 10));

//                // push all four items to the primary stream. this should produce four items.
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, 7L));
//                }
//                // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
//                // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+Y0", null), 7),
//                    new KeyValueTimestamp<>(1, new Change<>("XX1+Y1", null), 10),
//                    new KeyValueTimestamp<>(2, new Change<>("XX2+null", null), 7),
//                    new KeyValueTimestamp<>(3, new Change<>("XX3+null", null), 7));

//                // push all items to the other stream. this should produce four items.
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, expectedKey * 5L));
//                }
//                // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
//                // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+YY0", null), 7),
//                    new KeyValueTimestamp<>(1, new Change<>("XX1+YY1", null), 7),
//                    new KeyValueTimestamp<>(2, new Change<>("XX2+YY2", null), 10),
//                    new KeyValueTimestamp<>(3, new Change<>("XX3+YY3", null), 15));

//                // push all four items to the primary stream. this should produce four items.
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXX" + expectedKey, 6L));
//                }
//                // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
//                // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+YY0", null), 6),
//                    new KeyValueTimestamp<>(1, new Change<>("XXX1+YY1", null), 6),
//                    new KeyValueTimestamp<>(2, new Change<>("XXX2+YY2", null), 10),
//                    new KeyValueTimestamp<>(3, new Change<>("XXX3+YY3", null), 15));

//                // push two items with null to the other stream.As deletes. this should produce two item.
//                driver.pipeInput(recordFactory.create(topic2, expectedKeys[0], null, 5L));
//                driver.pipeInput(recordFactory.create(topic2, expectedKeys[1], null, 7L));
//                // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
//                // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+null", null), 6),
//                    new KeyValueTimestamp<>(1, new Change<>("XXX1+null", null), 7));

//                // push all four items to the primary stream. this should produce four items.
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXXX" + expectedKey, 13L));
//                }
//                // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
//                // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXXX0+null", null), 13),
//                    new KeyValueTimestamp<>(1, new Change<>("XXXX1+null", null), 13),
//                    new KeyValueTimestamp<>(2, new Change<>("XXXX2+YY2", null), 13),
//                    new KeyValueTimestamp<>(3, new Change<>("XXXX3+YY3", null), 15));

//                // push four items to the primary stream with null. this should produce four items.
//                driver.pipeInput(recordFactory.create(topic1, expectedKeys[0], null, 0L));
//                driver.pipeInput(recordFactory.create(topic1, expectedKeys[1], null, 42L));
//                driver.pipeInput(recordFactory.create(topic1, expectedKeys[2], null, 5L));
//                driver.pipeInput(recordFactory.create(topic1, expectedKeys[3], null, 20L));
//                // left:
//                // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>(null, null), 0),
//                    new KeyValueTimestamp<>(1, new Change<>(null, null), 42),
//                    new KeyValueTimestamp<>(2, new Change<>(null, null), 10),
//                    new KeyValueTimestamp<>(3, new Change<>(null, null), 20));
//            }
//    }

//        [Fact]
//        public void testSendingOldValue()
//        {
//            var builder = new StreamsBuilder();

//            var expectedKeys = new int[] { 0, 1, 2, 3 };

//            IKTable<int, string> table1;
//            IKTable<int, string> table2;
//            IKTable<int, string> joined;
//            MockProcessorSupplier<int, string> supplier;

//            table1 = builder.Table(topic1, consumed);
//            table2 = builder.Table(topic2, consumed);
//            joined = table1.leftJoin(table2, MockValueJoiner.TOSTRING_JOINER);

//            ((IKTable<object, object, object>)joined).enableSendingOldValues();

//            supplier = new MockProcessorSupplier<>();
//            Topology topology = builder.Build().addProcessor("proc", supplier, ((IKTable<object, object, object>)joined).name);

//            try
//            {
//                var driver = new TopologyTestDriverWrapper(topology, props);
//                MockProcessor<int, string> proc = supplier.theCapturedProcessor();

//                Assert.True(((IKTable<object, object, object>)table1).sendingOldValueEnabled());
//                Assert.True(((IKTable<object, object, object>)table2).sendingOldValueEnabled());
//                Assert.True(((IKTable<object, object, object>)joined).sendingOldValueEnabled());

//                // push two items to the primary stream. the other table is empty
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], 5L + i));
//                }
//                // .Ass tuple with null key, it will be discarded in join process
//                driver.pipeInput(recordFactory.create(topic1, null, "SomeVal", 42L));
//                // left: X0:0 (ts: 5), X1:1 (ts: 6)
//                // right:
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+null", null), 5),
//                    new KeyValueTimestamp<>(1, new Change<>("X1+null", null), 6));

//                // push two items to the other stream. this should produce two items.
//                for (var i = 0; i < 2; i++)
//                {
//                    driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i], 10L * i));
//                }
//                // .Ass tuple with null key, it will be discarded in join process
//                driver.pipeInput(recordFactory.create(topic2, null, "AnotherVal", 73L));
//                // left: X0:0 (ts: 5), X1:1 (ts: 6)
//                // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+Y0", "X0+null"), 5),
//                    new KeyValueTimestamp<>(1, new Change<>("X1+Y1", "X1+null"), 10));

//                // push all four items to the primary stream. this should produce four items.
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, 7L));
//                }
//                // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
//                // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+Y0", "X0+Y0"), 7),
//                    new KeyValueTimestamp<>(1, new Change<>("XX1+Y1", "X1+Y1"), 10),
//                    new KeyValueTimestamp<>(2, new Change<>("XX2+null", null), 7),
//                    new KeyValueTimestamp<>(3, new Change<>("XX3+null", null), 7));

//                // push all items to the other stream. this should produce four items.
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, expectedKey * 5L));
//                }
//                // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
//                // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+YY0", "XX0+Y0"), 7),
//                    new KeyValueTimestamp<>(1, new Change<>("XX1+YY1", "XX1+Y1"), 7),
//                    new KeyValueTimestamp<>(2, new Change<>("XX2+YY2", "XX2+null"), 10),
//                    new KeyValueTimestamp<>(3, new Change<>("XX3+YY3", "XX3+null"), 15));
//                // push all four items to the primary stream. this should produce four items.
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXX" + expectedKey, 6L));
//                }
//                // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
//                // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+YY0", "XX0+YY0"), 6),
//                    new KeyValueTimestamp<>(1, new Change<>("XXX1+YY1", "XX1+YY1"), 6),
//                    new KeyValueTimestamp<>(2, new Change<>("XXX2+YY2", "XX2+YY2"), 10),
//                    new KeyValueTimestamp<>(3, new Change<>("XXX3+YY3", "XX3+YY3"), 15));

//                // push two items with null to the other stream.As deletes. this should produce two item.
//                driver.pipeInput(recordFactory.create(topic2, expectedKeys[0], null, 5L));
//                driver.pipeInput(recordFactory.create(topic2, expectedKeys[1], null, 7L));
//                // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
//                // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+null", "XXX0+YY0"), 6),
//                    new KeyValueTimestamp<>(1, new Change<>("XXX1+null", "XXX1+YY1"), 7));

//                // push all four items to the primary stream. this should produce four items.
//                foreach (var expectedKey in expectedKeys)
//                {
//                    driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXXX" + expectedKey, 13L));
//                }
//                // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
//                // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXXX0+null", "XXX0+null"), 13),
//                    new KeyValueTimestamp<>(1, new Change<>("XXXX1+null", "XXX1+null"), 13),
//                    new KeyValueTimestamp<>(2, new Change<>("XXXX2+YY2", "XXX2+YY2"), 13),
//                    new KeyValueTimestamp<>(3, new Change<>("XXXX3+YY3", "XXX3+YY3"), 15));
//                // push four items to the primary stream with null. this should produce four items.
//                driver.pipeInput(recordFactory.create(topic1, expectedKeys[0], null, 0L));
//                driver.pipeInput(recordFactory.create(topic1, expectedKeys[1], null, 42L));
//                driver.pipeInput(recordFactory.create(topic1, expectedKeys[2], null, 5L));
//                driver.pipeInput(recordFactory.create(topic1, expectedKeys[3], null, 20L));
//                // left:
//                // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
//                proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>(null, "XXXX0+null"), 0),
//                    new KeyValueTimestamp<>(1, new Change<>(null, "XXXX1+null"), 42),
//                    new KeyValueTimestamp<>(2, new Change<>(null, "XXXX2+YY2"), 10),
//                    new KeyValueTimestamp<>(3, new Change<>(null, "XXXX3+YY3"), 20));
//            }
//    }

//        /**
//         * This test .As written to reproduce https://issues.apache.org/jira/browse/KAFKA-4492
//         * It is .Ased on a fairly complicated join used by the developer that reported the bug.
//         * Before the fix this would trigger an IllegalStateException.
//         */
//        [Fact]
//        public void shouldNotThrowIllegalStateExceptionWhenMultiCacheEvictions()
//        {
//            var agg = "agg";
//            var tableOne = "tableOne";
//            var tableTwo = "tableTwo";
//            var tableThree = "tableThree";
//            var tableFour = "tableFour";
//            var tableFive = "tableFive";
//            var tableSix = "tableSix";
//            string[] inputs = { agg, tableOne, tableTwo, tableThree, tableFour, tableFive, tableSix };

//            var builder = new StreamsBuilder();
//            Consumed<long, string> consumed = Consumed.with(Serdes.Long(), Serdes.String());
//            IKTable<long, string> aggTable = builder
//                .Table(agg, consumed, Materialized.As(Stores.inMemoryKeyValueStore("agg-case-store")))
//                .groupBy(KeyValuePair, Grouped.with(Serdes.Long(), Serdes.String()))
//                .reduce(
//                    MockReducer.STRING_ADDER,
//                    MockReducer.STRING_ADDER,
//                    Materialized.As(Stores.inMemoryKeyValueStore("agg-store")));

//            IKTable<long, string> one = builder.Table(
//                tableOne,
//                consumed,
//                Materialized.As(Stores.inMemoryKeyValueStore("tableOne-case-store")));
//            IKTable<long, string> two = builder.Table(
//                tableTwo,
//                consumed,
//                Materialized.As(Stores.inMemoryKeyValueStore("tableTwo-case-store")));
//            IKTable<long, string> three = builder.Table(
//                tableThree,
//                consumed,
//                Materialized.As(Stores.inMemoryKeyValueStore("tableThree-case-store")));
//            IKTable<long, string> four = builder.Table(
//                tableFour,
//                consumed,
//                Materialized.As(Stores.inMemoryKeyValueStore("tableFour-case-store")));
//            IKTable<long, string> five = builder.Table(
//                tableFive,
//                consumed,
//                Materialized.As(Stores.inMemoryKeyValueStore("tableFive-case-store")));
//            IKTable<long, string> six = builder.Table(
//                tableSix,
//                consumed,
//                Materialized.As(Stores.inMemoryKeyValueStore("tableSix-case-store")));

//            IValueMapper<string, string> mapper = value => value.toUppercase(Locale.ROOT);

//            IKTable<long, string> seven = one.mapValues(mapper);

//            IKTable<long, string> eight = six.leftJoin(seven, MockValueJoiner.TOSTRING_JOINER);

//            aggTable
//                .leftJoin(one, MockValueJoiner.TOSTRING_JOINER)
//                .leftJoin(two, MockValueJoiner.TOSTRING_JOINER)
//                .leftJoin(three, MockValueJoiner.TOSTRING_JOINER)
//                .leftJoin(four, MockValueJoiner.TOSTRING_JOINER)
//                .leftJoin(five, MockValueJoiner.TOSTRING_JOINER)
//                .leftJoin(eight, MockValueJoiner.TOSTRING_JOINER)
//                .mapValues(mapper);

//            ConsumerRecordFactory<long, string> factory = new ConsumerRecordFactory<>(Serdes.Long().Serializer, Serdes.String().Serializer);
//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);

//                string[] values = {
//                "a", "AA", "BBB", "CCCC", "DD", "EEEEEEEE", "F", "GGGGGGGGGGGGGGG", "HHH", "IIIIIIIIII",
//                "J", "KK", "LLLL", "MMMMMMMMMMMMMMMMMMMMMM", "NNNNN", "O", "P", "QQQQQ", "R", "SSSS",
//                "T", "UU", "VVVVVVVVVVVVVVVVVVV"
//            };

//                var random = new Random();
//                for (var i = 0; i < 1000; i++)
//                {
//                    foreach (var input in inputs)
//                    {
//                        long key = (long)random.nextInt(1000);
//                        string value = values[random.nextInt(values.Length)];
//                        driver.pipeInput(factory.create(input, key, value));
//                    }
//                }
//            }
//    }

//        [Fact]
//        public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey()
//        {
//            var builder = new StreamsBuilder();


//            Processor<string, Change<string>> join = new KTableKTableLeftJoin<>(
//                (IKTable<string, string, string>)builder.Table("left", Consumed.with(Serdes.String(), Serdes.String())),
//                (IKTable<string, string, string>)builder.Table("right", Consumed.with(Serdes.String(), Serdes.String())),
//                null
//            ).get();

//            var context = new MockProcessorContext();
//            context.setRecordMetadata("left", -1, -2, null, -3);
//            join.init(context);
//            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//            join.process(null, new Change<>("new", "old"));
//            LogCaptureAppender.unregister(appender);

//            Assert.Equal(1.0, getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
//            Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key. change=[(new<-old)] topic=[left] partition=[-1] offset=[-2]"));
//        }

//        private void assertOutputKeyValueTimestamp(var driver,
//                                                   int expectedKey,
//                                                   string expectedValue,
//                                                   long expectedTimestamp)
//        {
//            OutputVerifier.compareKeyValueTimestamp(
//                driver.readOutput(output, Serdes.Int().deserializer(), Serdes.String().deserializer()),
//                expectedKey,
//                expectedValue,
//                expectedTimestamp);
//        }
//    }
