//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.Integration
//{
//    /**
//     * Tests All available joins of Kafka Streams DSL.
//     */


//    public class StreamStreamJoinIntegrationTest : AbstractJoinIntegrationTest
//    {
//        private IKStream<long, string> leftStream;
//        private IKStream<long, string> rightStream;

//        public StreamStreamJoinIntegrationTest(bool cacheEnabled)
//        {
//            super(cacheEnabled);
//        }


//        public void PrepareTopology()
//        {// throws InterruptedException
//            base.prepareEnvironment();

//            appID = "stream-stream-join-integration-test";

//            builder = new StreamsBuilder();
//            leftStream = builder.Stream(INPUT_TOPIC_LEFT);
//            rightStream = builder.Stream(INPUT_TOPIC_RIGHT);
//        }

//        [Fact]
//        public void TestInner()
//        {// throws Exception
//            STREAMS_CONFIG.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

//            List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                null,
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
//                null,
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
//            );

//            leftStream.join(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).To(OUTPUT_TOPIC);

//            runTest(expectedResult);
//        }

//        [Fact]
//        public void TestInnerRepartitioned()
//        {// throws Exception
//            STREAMS_CONFIG.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-repartitioned");

//            List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                null,
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
//                null,
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
//            );

//            leftStream.map(MockMapper.noOpKeyValueMapper())
//                    .join(rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
//                                     .selectKey(MockMapper.selectKeyKeyValueMapper()),
//                           valueJoiner, JoinWindows.of(ofSeconds(10))).To(OUTPUT_TOPIC);

//            runTest(expectedResult);
//        }

//        [Fact]
//        public void TestLeft()
//        {// throws Exception
//            STREAMS_CONFIG.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

//            List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
//                null,
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
//            );

//            leftStream.leftJoin(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).To(OUTPUT_TOPIC);

//            runTest(expectedResult);
//        }

//        [Fact]
//        public void TestLeftRepartitioned()
//        {// throws Exception
//            STREAMS_CONFIG.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left-repartitioned");

//            List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
//                null,
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
//            );

//            leftStream.map(MockMapper.noOpKeyValueMapper())
//                    .leftJoin(rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
//                                         .selectKey(MockMapper.selectKeyKeyValueMapper()),
//                            valueJoiner, JoinWindows.of(ofSeconds(10))).To(OUTPUT_TOPIC);

//            runTest(expectedResult);
//        }

//        [Fact]
//        public void TestOuter()
//        {// throws Exception
//            STREAMS_CONFIG.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

//            List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
//                null,
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
//            );

//            leftStream.outerJoin(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).To(OUTPUT_TOPIC);

//            runTest(expectedResult);
//        }

//        [Fact]
//        public void TestOuterRepartitioned()
//        {// throws Exception
//            STREAMS_CONFIG.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

//            List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-null", 3L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a", 4L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b", 6L)),
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a", 9L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b", 9L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c", 10L)),
//                null,
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d", 14L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d", 15L))
//            );

//            leftStream.map(MockMapper.noOpKeyValueMapper())
//                    .outerJoin(rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
//                                    .selectKey(MockMapper.selectKeyKeyValueMapper()),
//                            valueJoiner, JoinWindows.of(ofSeconds(10))).To(OUTPUT_TOPIC);

//            runTest(expectedResult);
//        }

//        [Fact]
//        public void TestMultiInner()
//        {// throws Exception
//            STREAMS_CONFIG.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-multi-inner");

//            List<List<KeyValueTimestamp<long, string>>> expectedResult = Array.asList(
//                null,
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-a", 4L)),
//                Collections.singletonList(new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-a", 5L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b-a", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-a", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-b", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-b", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b-b", 6L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-b", 6L)),
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a-a", 9L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a-b", 9L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b-a", 9L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b-b", 9L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c-a", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c-b", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c-a", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c-b", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-a", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-b", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c-c", 10L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-c", 10L)),
//                null,
//                null,
//                null,
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d-a", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d-b", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d-c", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d-a", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d-b", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d-c", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d-a", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d-b", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d-c", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-a-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-a-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-b-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-b-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-a-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-b-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-c-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-c-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-c-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "A-d-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "B-d-d", 14L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "C-d-d", 14L)),
//                Array.asList(
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-a-d", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-b-d", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-c-d", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-a", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-b", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-c", 15L),
//                    new KeyValueTimestamp<>(ANY_UNIQUE_KEY, "D-d-d", 15L))
//            );

//            leftStream.join(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10)))
//                    .join(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).To(OUTPUT_TOPIC);

//            runTest(expectedResult);
//        }
//    }
//}
