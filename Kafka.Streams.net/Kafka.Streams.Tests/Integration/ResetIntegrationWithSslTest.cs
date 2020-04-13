//using Kafka.Streams.Configs;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.Integration
//{
//    /**
//     * Tests command line SSL setup for reset tool.
//     */
//    public class ResetIntegrationWithSslTest : AbstractResetIntegrationTest
//    {
//        public static EmbeddedKafkaCluster CLUSTER;

//        private const string TEST_ID = "reset-with-ssl-integration-test";

//        private static Dictionary<string, object> sslConfig;

//        //static {
//        StreamsConfig brokerProps = new StreamsConfig();
//        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
//        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
//        // very long sleep times
//        //brokerProps.Put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);

//        //sslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, TestUtils.tempFile(), "testCert");

//        //brokerProps.Put(KafkaConfig$.MODULE$.ListenersProp(), "SSL://localhost:0");
//        //brokerProps.Put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(), "SSL");
//        //brokerProps.PutAll(sslConfig);
//        //} catch (Exception e) {
//        //    throw new RuntimeException(e);
//        //}
//        //
//        //CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
//    }


//    Dictionary<string, object> GetClientSslConfig()
//    {
//        return sslConfig;
//    }


//    public void Before()
//    {// throws Exception
//        testId = TEST_ID;
//        cluster = CLUSTER;
//        prepareTest();
//    }

//    public void After()
//    {// throws Exception
//        cleanupTest();
//    }

//    [Fact]
//    public void TestReprocessingFromScratchAfterResetWithoutIntermediateUserTopic()
//    {// throws Exception
//        base.testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic();
//    }

//    [Fact]
//    public void TestReprocessingFromScratchAfterResetWithIntermediateUserTopic()
//    {// throws Exception
//        base.testReprocessingFromScratchAfterResetWithIntermediateUserTopic();
//    }
//}
