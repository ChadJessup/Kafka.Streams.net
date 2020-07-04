//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Tests.Helpers;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    /**
//     * Tests All available joins of Kafka Streams DSL.
//     */
//    public class StreamTableJoinIntegrationTest : AbstractJoinIntegrationTest
//    {
//        private IKStream<long, string> leftStream;
//        private IKTable<long, string> rightTable;

//        public StreamTableJoinIntegrationTest(bool cacheEnabled)
//            : base(cacheEnabled)
//        {
//        }


//        public void PrepareTopology()
//        {// throws InterruptedException
//            base.prepareEnvironment();

//            appID = "stream-table-join-integration-test";

//            builder = new StreamsBuilder();
//            rightTable = builder.table(INPUT_TOPIC_RIGHT);
//            leftStream = builder.Stream(INPUT_TOPIC_LEFT);
//        }

//        [Fact]
//        public void TestShouldAutoShutdownOnIncompleteMetadata()
//        {// throws InterruptedException
//            STREAMS_CONFIG.Put(StreamsConfig.ApplicationId, appID + "-incomplete");

//            IKStream<long, string> notExistStream = builder.Stream(INPUT_TOPIC_LEFT + "-not-existed");

//            IKTable<long, string> aggregatedTable = notExistStream.LeftJoin(rightTable, valueJoiner)
//                    .GroupBy((key, value) => key)
//                    .Reduce((value1, value2) => value1 + value2);

//            // Write the (continuously updating) results to the output topic.
//            aggregatedTable.ToStream().To(OUTPUT_TOPIC);

//            KafkaStreamsWrapper streams = new KafkaStreamsWrapper(builder.Build(), STREAMS_CONFIG);
//            IntegrationTestUtils.StateListenerStub listener = new IntegrationTestUtils.StateListenerStub();
//            streams.setStreamThreadStateListener(listener);
//            streams.Start();

//            TestUtils.WaitForCondition(listener.revokedToPendingShutdownSeen, "Did not seen thread state transited to PENDING_SHUTDOWN");

//            streams.Close();
//            Assert.True(listener.createdToRevokedSeen());
//            Assert.True(listener.revokedToPendingShutdownSeen());
//        }

//        [Fact]
//        public void TestInner()
//        {// throws Exception
//            STREAMS_CONFIG.Put(StreamsConfig.ApplicationId, appID + "-inner");

//            List<List<KeyValueTimestamp<long, string>>> expectedResult = Arrays.asList(
//                null,
//                null,
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<string, string>(ANY_UNIQUE_KEY, "D-d", 15L))
//            );

//            leftStream.Join(rightTable, valueJoiner).To(OUTPUT_TOPIC);

//            runTest(expectedResult);
//        }

//        [Fact]
//        public void TestLeft()
//        {// throws Exception
//            STREAMS_CONFIG.Put(StreamsConfig.ApplicationId, appID + "-left");

//            List<List<KeyValueTimestamp<long, string>>> expectedResult = Arrays.asList(
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "A-null", 3L)),
//                null,
//                Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "B-a", 5L)),
//                null,
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "C-null", 9L)),
//                null,
//                null,
//                null,
//                null,
//                null,
//                Collections.singletonList(new KeyValueTimestamp<long, string>(ANY_UNIQUE_KEY, "D-d", 15L))
//            );

//            leftStream.LeftJoin(rightTable, valueJoiner).To(OUTPUT_TOPIC);

//            runTest(expectedResult);
//        }
//    }
//}
