//using Kafka.Streams.Configs;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.Integration
//{
//    /*






//    *

//    *





//    */
















//    /**
//     * Tests local state store and global application cleanup.
//     */

//    public class ResetIntegrationTest : AbstractResetIntegrationTest
//    {
//        public static EmbeddedKafkaCluster CLUSTER;

//        private const string TEST_ID = "reset-integration-test";

//        StreamsConfig brokerProps = new StreamsConfig();
//        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
//        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
//        // very long sleep times
//        //brokerProps.Put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);
//        //CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);

//        Dictionary<string, object> GetClientSslConfig()
//        {
//            return null;
//        }


//        public void Before()
//        {// throws Exception
//            testId = TEST_ID;
//            cluster = CLUSTER;
//            prepareTest();
//        }


//        public void After()
//        {// throws Exception
//            cleanupTest();
//        }

//        [Fact]
//        public void TestReprocessingFromScratchAfterResetWithoutIntermediateUserTopic()
//        {// throws Exception
//            base.TestReprocessingFromScratchAfterResetWithoutIntermediateUserTopic();
//        }

//        [Fact]
//        public void TestReprocessingFromScratchAfterResetWithIntermediateUserTopic()
//        {// throws Exception
//            base.TestReprocessingFromScratchAfterResetWithIntermediateUserTopic();
//        }

//        [Fact]
//        public void TestReprocessingFromFileAfterResetWithoutIntermediateUserTopic()
//        {// throws Exception
//            base.TestReprocessingFromFileAfterResetWithoutIntermediateUserTopic();
//        }

//        [Fact]
//        public void TestReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic()
//        {// throws Exception
//            base.testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic();
//        }

//        [Fact]
//        public void TestReprocessingByDurationAfterResetWithoutIntermediateUserTopic()
//        {// throws Exception
//            base.testReprocessingByDurationAfterResetWithoutIntermediateUserTopic();
//        }

//        [Fact]
//        public void ShouldNotAllowToResetWhileStreamsRunning()
//        {// throws Exception
//            base.shouldNotAllowToResetWhileStreamsIsRunning();
//        }

//        [Fact]
//        public void ShouldNotAllowToResetWhenInputTopicAbsent()
//        {// throws Exception
//            base.shouldNotAllowToResetWhenInputTopicAbsent();
//        }

//        [Fact]
//        public void ShouldNotAllowToResetWhenIntermediateTopicAbsent()
//        {// throws Exception
//            //base.shouldNotAllowToResetWhenIntermediateTopicAbsent();
//        }
//    }
//}
