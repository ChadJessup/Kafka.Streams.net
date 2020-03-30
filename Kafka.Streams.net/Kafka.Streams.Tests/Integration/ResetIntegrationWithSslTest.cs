namespace Kafka.Streams.Tests.Integration
{
    /*






    *

    *





    */

















    /**
     * Tests command line SSL setup for reset tool.
     */

    public class ResetIntegrationWithSslTest : AbstractResetIntegrationTest
    {


        public static EmbeddedKafkaCluster CLUSTER;

        private static readonly string TEST_ID = "reset-with-ssl-integration-test";

        private static Dictionary<string, object> sslConfig;

        static {
        Properties brokerProps = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        brokerProps.put(KafkaConfig$.MODULE$.ConnectionsMaxIdleMsProp(), -1L);

        try {
            sslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, TestUtils.tempFile(), "testCert");

            brokerProps.put(KafkaConfig$.MODULE$.ListenersProp(), "SSL://localhost:0");
            brokerProps.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(), "SSL");
            brokerProps.putAll(sslConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
    }

    
    Dictionary<string, object> GetClientSslConfig()
    {
        return sslConfig;
    }


    public void Before()
    {// throws Exception
        testId = TEST_ID;
        cluster = CLUSTER;
        prepareTest();
    }


    public void After()
    {// throws Exception
        cleanupTest();
    }

    [Xunit.Fact]
    public void TestReprocessingFromScratchAfterResetWithoutIntermediateUserTopic()
    {// throws Exception
        base.testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic();
    }

    [Xunit.Fact]
    public void TestReprocessingFromScratchAfterResetWithIntermediateUserTopic()
    {// throws Exception
        base.testReprocessingFromScratchAfterResetWithIntermediateUserTopic();
    }
}
}
/*






*

*





*/

















/**
 * Tests command line SSL setup for reset tool.
 */










