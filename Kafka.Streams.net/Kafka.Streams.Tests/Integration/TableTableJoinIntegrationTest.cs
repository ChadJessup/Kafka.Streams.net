//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State.KeyValues;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.Integration
//{
//    /**
//     * Tests all available joins of Kafka Streams DSL.
//     */
//    public class TableTableJoinIntegrationTest : AbstractJoinIntegrationTest
//    {
//        private IKTable<long, string> leftTable;
//        private IKTable<long, string> rightTable;

//        public TableTableJoinIntegrationTest(bool cacheEnabled)
//            : base(cacheEnabled)
//        {
//        }


//        public void PrepareTopology()
//        {// throws InterruptedException
//            base.prepareEnvironment();

//            appID = "table-table-join-integration-test";

//            builder = new StreamsBuilder();
//            leftTable = builder.table(INPUT_TOPIC_LEFT, Materialized.As<long, string, IKeyValueStore<Bytes, byte[]>>("left").WithLoggingDisabled());
//            rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.As<long, string, IKeyValueStore<Bytes, byte[]>>("right").WithLoggingDisabled());
//        }

//        private KeyValueTimestamp<long, string> expectedFinalJoinResult = new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d", 15L);
//        private KeyValueTimestamp<long, string> expectedFinalMultiJoinResult = new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L);
//        private readonly string storeName = appID + "-store";

//        private Materialized<long, string, IKeyValueStore<Bytes, byte[]>> materialized = Materialized.As<long, string, IKeyValueStore<Bytes, byte[]>>(storeName)
//                .WithKeySerde(Serdes.Long())
//                .WithValueSerde(Serdes.String())
//                .WithCachingDisabled()
//                .WithLoggingDisabled();

//        private class CountingPeek : IForeachAction<long, string>
//        {
//            private KeyValueTimestamp<long, string> expected;

//            CountingPeek(bool multiJoin)
//            {
//                this.expected = multiJoin ? expectedFinalMultiJoinResult : expectedFinalJoinResult;
//            }


//            public void Apply(long key, string value)
//            {
//                numRecordsExpected++;
//                if (expected.Value.equals(value))
//                {
//                    bool ret = finalResultReached.compareAndSet(false, true);

//                    if (!ret)
//                    {
//                        // do nothing; it is possible that we will see multiple duplicates of results due to KAFKA-4309
//                        // TODO: should be removed when KAFKA-4309 is fixed
//                    }
//                }
//            }
//        }

//        [Fact]
//        public void TestInner()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

//            if (cacheEnabled)
//            {
//                leftTable.Join(rightTable, valueJoiner, materialized).ToStream().Peek(new CountingPeek(false)).To(OUTPUT_TOPIC);
//                runTest(expectedFinalJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 7L)),
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c", 10L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 11L)),
//                    null,
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d", 15L))
//                );

//                leftTable.Join(rightTable, valueJoiner, materialized).ToStream().To(OUTPUT_TOPIC);
//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestLeft()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

//            if (cacheEnabled)
//            {
//                leftTable.LeftJoin(rightTable, valueJoiner, materialized).ToStream().Peek(new CountingPeek(false)).To(OUTPUT_TOPIC);
//                runTest(expectedFinalJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-null", 3L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 7L)),
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null", 9L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c", 10L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null", 11L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 12L)),
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d", 15L))
//                );

//                leftTable.LeftJoin(rightTable, valueJoiner, materialized).ToStream().To(OUTPUT_TOPIC);
//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestOuter()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

//            if (cacheEnabled)
//            {
//                leftTable.OuterJoin(rightTable, valueJoiner, materialized).ToStream().Peek(new CountingPeek(false)).To(OUTPUT_TOPIC);
//                runTest(expectedFinalJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-null", 3L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-b", 7L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 8L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null", 9L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c", 10L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null", 11L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 12L)),
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-d", 14L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d", 15L))
//                );

//                leftTable.OuterJoin(rightTable, valueJoiner, materialized).ToStream().To(OUTPUT_TOPIC);
//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestInnerInner()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-inner");

//            if (cacheEnabled)
//            {
//                leftTable.Join(rightTable, valueJoiner)
//                        .Join(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .Peek(new CountingPeek(true))
//                        .To(OUTPUT_TOPIC);
//                runTest(expectedFinalMultiJoinResult, storeName);
//            }
//            else
//            {
//                // FIXME: the duplicate below for all the multi-joins
//                //        are due to KAFKA-6443, should be updated once it is fixed.
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 7L)),
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                    null, // correct would be => new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 11L)
//                          // we don't get correct value, because of self-join of `rightTable`
//                    null,
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//                );

//                leftTable.Join(rightTable, valueJoiner)
//                        .Join(rightTable, valueJoiner, materialized)
//                        .ToStream().To(OUTPUT_TOPIC);

//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestInnerLeft()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-left");

//            if (cacheEnabled)
//            {
//                leftTable.Join(rightTable, valueJoiner)
//                        .LeftJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .Peek(new CountingPeek(true))
//                        .To(OUTPUT_TOPIC);
//                runTest(expectedFinalMultiJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 7L)),
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 11L)),
//                    null,
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//                );

//                leftTable.Join(rightTable, valueJoiner)
//                        .LeftJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .To(OUTPUT_TOPIC);

//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestInnerOuter()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-outer");

//            if (cacheEnabled)
//            {
//                leftTable.Join(rightTable, valueJoiner)
//                        .OuterJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .Peek(new CountingPeek(true))
//                        .To(OUTPUT_TOPIC);
//                runTest(expectedFinalMultiJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-b", 7L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 8L)),
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 11L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 11L)),
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        // incorrect result `null-d` is caused by self-join of `rightTable`
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-d", 14L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//                );

//                leftTable.Join(rightTable, valueJoiner)
//                        .OuterJoin(rightTable, valueJoiner, materialized)
//                        .ToStream().To(OUTPUT_TOPIC);

//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestLeftInner()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-inner");

//            if (cacheEnabled)
//            {
//                leftTable.LeftJoin(rightTable, valueJoiner)
//                        .Join(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .Peek(new CountingPeek(true))
//                        .To(OUTPUT_TOPIC);
//                runTest(expectedFinalMultiJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 7L)),
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 11L)),
//                    null,
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//                );

//                leftTable.LeftJoin(rightTable, valueJoiner)
//                        .Join(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .To(OUTPUT_TOPIC);

//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestLeftLeft()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-left");

//            if (cacheEnabled)
//            {
//                leftTable.LeftJoin(rightTable, valueJoiner)
//                        .LeftJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .Peek(new CountingPeek(true))
//                        .To(OUTPUT_TOPIC);
//                runTest(expectedFinalMultiJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-null-null", 3L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 7L)),
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 9L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 11L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 11L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 12L)),
//                    null,
//                    null,
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//                );

//                leftTable.LeftJoin(rightTable, valueJoiner)
//                        .LeftJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .To(OUTPUT_TOPIC);

//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestLeftOuter()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-outer");

//            if (cacheEnabled)
//            {
//                leftTable.LeftJoin(rightTable, valueJoiner)
//                        .OuterJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .Peek(new CountingPeek(true))
//                        .To(OUTPUT_TOPIC);
//                runTest(expectedFinalMultiJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-null-null", 3L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-b", 7L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 8L)),
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 9L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 11L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 11L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 12L)),
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-d", 14L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//                );

//                leftTable.LeftJoin(rightTable, valueJoiner)
//                        .OuterJoin(rightTable, valueJoiner, materialized)
//                        .ToStream().To(OUTPUT_TOPIC);

//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestOuterInner()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-inner");

//            if (cacheEnabled)
//            {
//                leftTable.OuterJoin(rightTable, valueJoiner)
//                        .Join(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .Peek(new CountingPeek(true))
//                        .To(OUTPUT_TOPIC);
//                runTest(expectedFinalMultiJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-b-b", 7L)),
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 11L)),
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-d-d", 14L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-d-d", 14L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//                );

//                leftTable.OuterJoin(rightTable, valueJoiner)
//                        .Join(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .To(OUTPUT_TOPIC);

//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestOuterLeft()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-left");

//            if (cacheEnabled)
//            {
//                leftTable.OuterJoin(rightTable, valueJoiner)
//                        .LeftJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .Peek(new CountingPeek(true))
//                        .To(OUTPUT_TOPIC);
//                runTest(expectedFinalMultiJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-null-null", 3L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-b-b", 7L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 8L)),
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 9L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 11L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 11L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 12L)),
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-d-d", 14L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-d-d", 14L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//                );

//                leftTable.OuterJoin(rightTable, valueJoiner)
//                        .LeftJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .To(OUTPUT_TOPIC);

//                runTest(expectedResult, storeName);
//            }
//        }

//        [Fact]
//        public void TestOuterOuter()
//        {// throws Exception
//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-outer");

//            if (cacheEnabled)
//            {
//                leftTable.OuterJoin(rightTable, valueJoiner)
//                        .OuterJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .Peek(new CountingPeek(true))
//                        .To(OUTPUT_TOPIC);
//                runTest(expectedFinalMultiJoinResult, storeName);
//            }
//            else
//            {
//                List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                    null,
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-null-null", 3L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-b-b", 7L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 8L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 8L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 9L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 11L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null-null", 11L)),
//                    Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, null, 12L)),
//                    null,
//                    null,
//                    Array.asList(
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-d-d", 14L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "null-d-d", 14L),
//                        new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//                );

//                leftTable.OuterJoin(rightTable, valueJoiner)
//                        .OuterJoin(rightTable, valueJoiner, materialized)
//                        .ToStream()
//                        .To(OUTPUT_TOPIC);

//                runTest(expectedResult, storeName);
//            }
//        }
//    }
//}
